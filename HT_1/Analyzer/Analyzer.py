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


class DatasetAuditor:
    def __init__(self, ckan: CKANClient, datastore: DataStoreClient):
        self.ckan = ckan
        self.datastore = datastore

    def audit_dataset(self, ds: DatasetInfo) -> DatasetMetrics:
        rows = []
        for res in ds.resources:
            if not res.datastore_active or not res.id:
                continue
            stats = self.datastore.get_rows_cols(res.id)
            if stats:
                rows.append(stats["rows"])
        rows_total = sum(rows) if rows else 0
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
        try:
            with open(self.path, "x", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["dataset_id", "dataset_title", "n_resources", "rows_total"])
        except FileExistsError:
            pass


    def append(self, m: DatasetMetrics):
        with open(self.path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([m.dataset_id, m.dataset_title, m.n_resources, m.rows_total])



def main():
    base = "https://data.gov.ua/api/3/action"
    ckan = CKANClient(base)
    ds = DataStoreClient(base)
    auditor = DatasetAuditor(ckan, ds)
    reporter = CSVReporter("data_gov_ua_datastore_audit.csv")
    for metrics in auditor.audit_all():
        reporter.append(metrics)

if __name__ == "__main__":
    main()


