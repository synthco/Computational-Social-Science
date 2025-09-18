import csv
import time
from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Iterable

import requests


@dataclass
class  ResourceInfo:
    id: str
    url: Optional[str]
    datastore_active: bool

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
                r = requests.get(url, params=params, timeout=self.timeout)
                if r.status_code == 200:
                    return r.json()
            except Exception:
                time.sleep(backoff)
                backoff *= 2
        return None

    def iter_datasets(self) -> Iterable[DatasetInfo]:
        first = self._get_json("/package_search", {"rows": 1, "start": 0})
        if not first or not first.get("success"):
            return
        total = first["result"]["count"]
        start = 0
        while start < total:
            payload = self._get_json("/package_search", {"rows": self.page_size, "start": start})
            if not payload  or not payload.get("success"):
                break
            results = payload["result"]["results"]
            if not results:
                break
            for pkg in results:
                yield self._to_dataset(pkg)
            start += self.page_size
            if self.delay:
                time.sleep(self.delay)

    def _to_dataset(self, pkg: Dict[str, Any]) -> DatasetInfo:
        resources = []
        for res in pkg.get("resources") or []:
            resources.append(
                ResourceInfo(
                    id=res.get("id"),
                    url=res.get("url"),
                    datastore_active=res.get("datastore_active"),
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


class DatasetAuditor:
    def __init__(self, ckan: CKANClient, datastore: DataStoreClient, csv_counter: Optional[CsvRowCounter] = None):
        self.ckan = ckan
        self.datastore = datastore
        self.csv_counter = csv_counter

    def audit_dataset(self, ds: DatasetInfo) -> DatasetMetrics:
        rows_total = 0
        for res in ds.resources:
            if res.datastore_active and res.id:
                stats = self.datastore.get_rows_cols(res.id)
                if stats and isinstance(stats.get("rows"), int):
                    rows_total += stats["rows"]
            elif self.csv_counter and res.url:
                c = self.csv_counter.count_rows(res.url)
                if isinstance(c, int) and c > 0:
                    rows_total += c
        return DatasetMetrics(
            dataset_id=ds.id,
            dataset_title=ds.title,
            n_resources=len(ds.resources),
            rows_total=rows_total
        )

    def audit_all(self) -> Iterable[DatasetMetrics]:
        for ds in self.ckan.iter_datasets():
            print(f"Auditing dataset: id={ds.id}, title={ds.title}")
            yield self.audit_dataset(ds)


class CSVReporter:
    def __init__(self, path: str):
        self.path = path
        self._ensure_header()

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
    base = "https://data.gov.ua/api/3/action"
    ckan = CKANClient(base)
    ds = DataStoreClient(base)
    http = HttpUtil()
    csv_counter = CsvRowCounter(http, max_bytes=15000000, chunk_size=65536)
    auditor = DatasetAuditor(ckan, ds, csv_counter)
    reporter = CSVReporter("data_gov_ua_datastore_audit.csv")
    for metrics in auditor.audit_all():
        reporter.append(metrics)

if __name__ == "__main__":
    main()
