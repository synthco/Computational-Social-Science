import argparse
import csv
import os
import tempfile
import time
from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Iterable
import requests


@dataclass
class ResourceInfo:
    id: str
    url: Optional[str]
    datastore_active: bool
    format: Optional[str] = None


@dataclass
class DatasetInfo:
    id: str
    title: str
    resources: List[ResourceInfo]


@dataclass
class DatasetMetrics:
    dataset_id: str
    dataset_title: str
    n_resources: int
    rows_total: int


class CKANClient:
    def __init__(self, base_url: str, page_size: int = 1000, delay: float = 0.1, timeout: int = 30,
                 retries: int = 3):
        self.base_url = base_url.rstrip("/")
        self.page_size = page_size
        self.delay = delay
        self.timeout = timeout
        self.retries = retries

    def _get_json(self, path: str, params: Dict):
        url = f"{self.base_url}{path}"
        backoff = 0.5
        for _ in range(self.retries):
            try:
                r = requests.get(
                    url,
                    params=params,
                    timeout=self.timeout,
                    headers={"User-Agent": "ckan-audit/1.0"}
                )
                if r.status_code == 200:
                    return r.json()
            except Exception:
                time.sleep(backoff)
                backoff *= 2
        return None

    def _package_search(self, start: int, rows: int) -> Optional[Dict]:
        return self._get_json("/package_search", {"q": "*:*", "rows": rows, "start": start})

    def iter_datasets(self, limit: Optional[int] = None) -> Iterable[DatasetInfo]:
        first = self._package_search(start=0, rows=1)
        if not first or not first.get("success"):
            print("[ckan] package_search initial request failed or no success; falling back to package_list")
            yield from self.iter_datasets_via_list(limit=limit)
            return
        total = int(first["result"].get("count") or 0)
        print(f"[ckan] package_search count={total}")
        if total == 0:
            print("[ckan] package_search returned 0 results; falling back to package_list")
            yield from self.iter_datasets_via_list(limit=limit)
            return
        start = 0
        emitted = 0
        while start < total:
            payload = self._package_search(start=start, rows=self.page_size)
            if not payload or not payload.get("success"):
                print("[ckan] package_search page fetch failed; falling back to package_list for remaining")
                yield from self.iter_datasets_via_list(limit=limit)
                return
            results = payload["result"]["results"]
            if not results:
                break
            for pkg in results:
                if limit is not None and emitted >= limit:
                    return
                yield self._to_dataset(pkg)
                emitted += 1
            start += self.page_size
            if self.delay:
                time.sleep(self.delay)

    def iter_datasets_via_list(self, limit: Optional[int] = None) -> Iterable[DatasetInfo]:
        listing = self._get_json("/package_list", {})
        if not listing or not listing.get("success"):
            print("[ckan] package_list failed")
            return
        ids = listing.get("result") or []
        emitted = 0
        for pkg_id in ids:
            if limit is not None and emitted >= limit:
                return
            data = self._get_json("/package_show", {"id": pkg_id})
            if data and data.get("success"):
                yield self._to_dataset(data["result"])
                emitted += 1
            else:
                print(f"[ckan] package_show failed for id={pkg_id}")
            if self.delay:
                time.sleep(self.delay)

    def _to_dataset(self, pkg: Dict[str, Any]) -> DatasetInfo:
        resources = []
        for res in pkg.get("resources") or []:
            fmt = res.get("format")
            fmt_norm = fmt.lower() if isinstance(fmt, str) and fmt.strip() else None
            resources.append(
                ResourceInfo(
                    id=res.get("id"),
                    url=res.get("url"),
                    datastore_active=res.get("datastore_active"),
                    format = fmt_norm
                )
            )
        return DatasetInfo(
            id=pkg.get("id"),
            title=pkg.get("title") or pkg.get("name") or pkg.get("id"),
            resources=resources,
        )


class DataStoreClient:
    def __init__(self, base_url: str, timeout: int = 30, retries: int = 3, delay: float = 0.1):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self.delay = delay

    def get_rows_cols(self, resource_id: str) -> Optional[Dict[str, int]]:
        url = f"{self.base_url}/datastore_search"
        params = {"resource_id": resource_id, "limit": 0}
        backoff = 0.5
        for _ in range(self.retries):
            try:
                r = requests.get(url, params=params, timeout=self.timeout)
                if r.status_code == 200:
                    data = r.json()
                    if data.get("success") and "result" in data:
                        res = data["result"]
                        total = res.get("total")
                        fields = res.get("fields") or []
                        if isinstance(total, int):
                            if self.delay:
                                time.sleep(self.delay)
                            return {"rows": int(total), "cols": len(fields)}
            except Exception:
                time.sleep(backoff)
                backoff *= 2
        return None


class HttpUtil:
    def __init__(self, timeout: int = 30):
        self.timeout = timeout

    def head_content_length(self, url: str) -> Optional[int]:
        try:
            r = requests.head(url, allow_redirects=True, timeout=self.timeout)
            if r.status_code // 100 == 2:
                cl = r.headers.get("Content-Length")
                if cl and cl.isdigit():
                    return int(cl)
        except Exception:
            return None
        return None

    def stream_bytes(self, url: str, chunk_size: int = 65536):
        r = requests.get(url, stream=True, timeout=self.timeout)
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=chunk_size):
            if chunk:
                yield chunk

    def download_to_tempfile(self, url: str, max_bytes: int, chunk_size: int = 65536) -> Optional[str]:
        try:
            r = requests.get(url, stream=True, timeout=self.timeout)
            r.raise_for_status()
            total = 0
            suffix = ".xlsx" if url.lower().endswith(".xlsx") else ""
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if not chunk:
                        continue
                    total += len(chunk)
                    if total > max_bytes:
                        tmp.close()
                        os.unlink(tmp.name)
                        return None
                    tmp.write(chunk)
                return tmp.name
        except Exception:
            return None


class CsvRowCounter:
    def __init__(self, http: HttpUtil, max_bytes: int = 15000000, chunk_size: int = 65536):
        self.http = http
        self.max_bytes = max_bytes
        self.chunk_size = chunk_size

    def is_csv_url(self, url: Optional[str]) -> bool:
        if not url:
            return False
        u = url.lower()
        return u.endswith(".csv")

    def count_rows(self, url: str) -> Optional[int]:
        if not self.is_csv_url(url):
            return None
        cl = self.http.head_content_length(url)
        if cl is not None and cl > self.max_bytes:
            return None
        bytes_read = 0
        lines = 0
        tail_newline = True
        try:
            for chunk in self.http.stream_bytes(url, self.chunk_size):
                bytes_read += len(chunk)
                lines += chunk.count(b"\n")
                tail_newline = chunk.endswith(b"\n")
                if bytes_read > self.max_bytes:
                    return None
            if bytes_read > 0 and not tail_newline:
                lines += 1
            return lines
        except Exception:
            return None


class XlsxRowCounter:
    def __init__(self, http: HttpUtil, max_bytes: int = 20000000, chunk_size: int = 65536):
        self.http = http
        self.max_bytes = max_bytes
        self.chunk_size = chunk_size

    def is_xlsx_url(self, url: Optional[str], fmt: Optional[str] = None) -> bool:
        if fmt and isinstance(fmt, str) and fmt.lower() in {"xlsx", "excel"}:
            return True
        if not url:
            return False
        u = url.lower()
        if "?" in u:
            u = u.split("?", 1)[0]
        return u.endswith(".xlsx")

    def count_rows(self, url: str, fmt: Optional[str] = None) -> Optional[int]:
        if not self.is_xlsx_url(url, fmt):
            return None
        path = self.http.download_to_tempfile(url, max_bytes=self.max_bytes, chunk_size=self.chunk_size)
        if not path:
            return None
        try:
            try:
                import openpyxl
            except ImportError:
                return None
            wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
            try:
                ws = wb.active
                rows = 0
                for row in ws.iter_rows(values_only=True):
                    if any(cell is not None and str(cell).strip() != "" for cell in row):
                        rows += 1
                return rows
            finally:
                wb.close()
        except Exception:
            return None
        finally:
            try:
                os.unlink(path)
            except Exception:
                pass


class DatasetAuditor:
    def __init__(self, ckan: CKANClient, datastore: DataStoreClient, csv_counter: Optional['CsvRowCounter'] = None,
                 xlsx_counter: Optional['XlsxRowCounter'] = None, verbose: bool = False):
        self.ckan = ckan
        self.datastore = datastore
        self.csv_counter = csv_counter
        self.verbose = verbose
        self.xlsx_counter = xlsx_counter

    def audit_dataset(self, ds: DatasetInfo) -> DatasetMetrics:
        rows_total = 0
        for res in ds.resources:
            if self.verbose:
                print(f"[res] datastore_active={bool(res.datastore_active)} id={res.id} url={res.url}")
            if res.datastore_active and res.id:
                stats = self.datastore.get_rows_cols(res.id)
                if stats and isinstance(stats.get("rows"), int):
                    rows_total += stats["rows"]
                elif self.verbose:
                    print(f"[datastore] no rows/failed for resource {res.id}")
            elif res.url:
                used = False
                if self.csv_counter:
                    c = self.csv_counter.count_rows(res.url)
                    if isinstance(c, int) and c > 0:
                        rows_total += c
                        used = True
                        if self.verbose:
                            print(f"[fallback-csv] +{c} rows from {res.url}")
                if not used and self.xlsx_counter:
                    x = self.xlsx_counter.count_rows(res.url, getattr(res, "format", None))
                    if isinstance(x, int) and x > 0:
                        rows_total += x
                        used = True
                        if self.verbose:
                            print(f"[fallback-xlsx] +{x} rows from {res.url}")
                if not used and self.verbose:
                    print(f"[fallback] no count for url={res.url}")
        if self.verbose:
            print(f"[dataset-total] {ds.title} ({ds.id}) rows_total={rows_total}")
            if rows_total == 0:
                print(f"[dataset-zero] {ds.title} ({ds.id}) has 0 rows after all fallbacks")
        return DatasetMetrics(
            dataset_id=ds.id,
            dataset_title=ds.title,
            n_resources=len(ds.resources),
            rows_total=rows_total
        )

    def audit_all(self, limit: Optional[int] = None) -> Iterable[DatasetMetrics]:
        for ds in self.ckan.iter_datasets(limit=limit):
            print(f"Auditing dataset: id={ds.id}, title={ds.title}")
            yield self.audit_dataset(ds)


class CSVReporter:
    def __init__(self, path: str):
        self.path = path
        self._ensure_header()
        import os
        print(f"[csv] writing to {os.path.abspath(self.path)}")

    def _ensure_header(self):
        import os
        need_header = False
        if not os.path.exists(self.path):
            need_header = True
            mode = "w"
        else:
            mode = "a"
            if os.path.getsize(self.path) == 0:
                need_header = True
        if need_header:
            with open(self.path, mode, newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["dataset_id", "dataset_title", "n_resources", "rows_total"])

    def append(self, m: DatasetMetrics):
        with open(self.path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([m.dataset_id, m.dataset_title, m.n_resources, m.rows_total])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--max-bytes", type=int, default=15000000)
    parser.add_argument("--chunk-size", type=int, default=65536)
    parser.add_argument("--out", type=str, default="data_gov_ua_datastore_audit_2.csv")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    base = "https://data.gov.ua/api/3/action"
    ckan = CKANClient(base)
    ds = DataStoreClient(base)
    http = HttpUtil()
    csv_counter = CsvRowCounter(http, max_bytes=args.max_bytes, chunk_size=args.chunk_size)
    xlsx_counter = XlsxRowCounter(http, max_bytes=max(args.max_bytes, 20000000), chunk_size=args.chunk_size)
    auditor = DatasetAuditor(ckan, ds, csv_counter, xlsx_counter, verbose=args.verbose)
    reporter = CSVReporter(args.out)
    for metrics in auditor.audit_all(limit=args.limit):
        reporter.append(metrics)


if __name__ == "__main__":
    main()
