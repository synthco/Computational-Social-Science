#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# data_gov_ua_dataset_audit_no_cli.py
#
# Zero-argument script: double-click or run with Python; it will:
#  1) Pull all dataset IDs from data.gov.ua (CKAN)
#  2) Create ./output/data_gov_ua_audit.csv
#  3) For each dataset, compute:
#       - total bytes across resources (using resources.size; if missing, try HTTP HEAD Content-Length)
#       - total rows across tabular resources (datastore_active)
#       - min/median/max number of columns across tabular resources
#       - number of resources
#     and write one CSV row: name, id, n_resources, size_bytes_total, rows_total, cols_min, cols_median, cols_max
#  4) Print progress to console
#
# Requirements:
#   pip install requests
#
# Caveats:
# - resources.size is optional; HEAD fallback may fail or be blocked by some servers (handled gracefully)
# - Row/column counts only for resources with datastore_active=true
# - Be mindful of rate limits; adjust DELAY if needed
#
import csv
import os
import sys
import time
import json
import re
import statistics
from typing import Any, Dict, Iterable, List, Optional

try:
    import requests
except ImportError:
    print("This script requires the 'requests' library. Install it with: pip install requests", file=sys.stderr)
    sys.exit(1)

CKAN_BASE = "https://data.gov.ua/api/3/action"
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "data_gov_ua_audit.csv")
ROWS_PER_PAGE = 1000     # How many datasets per page from package_search
DELAY = 0.1              # Seconds between API calls/pages
TRY_HEAD_FOR_SIZE = True # Try HTTP HEAD on resources URLs if size is missing

HUMAN_SIZE_RE = re.compile(r"^\s*([0-9]*\.?[0-9]+)\s*([KMGTPE]?B|[KMGTPE]?iB)?\s*$", re.IGNORECASE)
UNIT_MULTIPLIERS = {
    None: 1,
    "B": 1,
    "KB": 1000, "MB": 1000**2, "GB": 1000**3, "TB": 1000**4, "PB": 1000**5,
    "KIB": 1024, "MIB": 1024**2, "GIB": 1024**3, "TIB": 1024**4, "PIB": 1024**5,
}

def to_bytes(value: Any) -> Optional[int]:
    """Parse CKAN resources.size (int or strings like '12 MB') into bytes."""
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            v = int(float(value))
            return v if v >= 0 else None
        except Exception:
            return None
    if isinstance(value, str):
        s = value.strip()
        if s.isdigit():
            try:
                v = int(s)
                return v if v >= 0 else None
            except Exception:
                pass
        m = HUMAN_SIZE_RE.match(s.upper())
        if m:
            num = float(m.group(1))
            unit = (m.group(2) or "B").upper()
            mult = UNIT_MULTIPLIERS.get(unit)
            if mult is not None:
                b = int(num * mult)
                return b if b >= 0 else None
    return None

def head_content_length(url: str, timeout: float = 15) -> Optional[int]:
    """Try to HEAD the resources URL and return Content-Length if available."""
    try:
        r = requests.head(url, allow_redirects=True, timeout=timeout)
        if r.status_code // 100 == 2:
            cl = r.headers.get("Content-Length")
            if cl and cl.isdigit():
                return int(cl)
    except Exception as e:
        print(f"[HEAD warn] {e} — {url}", file=sys.stderr)
    return None

def request_json(url: str, params=None, timeout=30) -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(url, params=params, timeout=timeout)
        if r.status_code == 200:
            return r.json()
        print(f"[WARN] HTTP {r.status_code} for {url}", file=sys.stderr)
    except Exception as e:
        print(f"[WARN] {e} for {url}", file=sys.stderr)
    return None

def iterate_packages(rows_per_page: int, delay: float) -> Iterable[Dict[str, Any]]:
    """Yield CKAN datasets (packages)."""
    first = request_json(f"{CKAN_BASE}/package_search", params={"rows": 1, "start": 0})
    if not first or not first.get("success"):
        print("[ERROR] Could not query package_search", file=sys.stderr)
        return
    total = first["result"]["count"]
    print(f"[INFO] Total datasets reported by portal: {total}")
    start = 0
    while start < total:
        payload = request_json(f"{CKAN_BASE}/package_search", params={"rows": rows_per_page, "start": start})
        if not payload or not payload.get("success"):
            print(f"[ERROR] Failed page at start={start}", file=sys.stderr)
            break
        results = payload["result"]["results"]
        if not results:
            break
        for pkg in results:
            pkg
        start += rows_per_page
        if delay:
            time.sleep(delay)

def get_datastore_counts(resource_id: str) -> Optional[Dict[str, int]]:
    """Return {'rows': int, 'cols': int} for a DataStore resources via datastore_search(limit=0)."""
    payload = request_json(f"{CKAN_BASE}/datastore_search", params={"resource_id": resource_id, "limit": 0})
    if payload and payload.get("success"):
        res = payload.get("result", {})
        total = res.get("total")
        fields = res.get("fields") or []
        if isinstance(total, int):
            return {"rows": total, "cols": len(fields)}
    return None

def ensure_output():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if not os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "dataset_id","dataset_name","n_resources",
                "size_bytes_total",
                "rows_total_datastore",
                "cols_min_datastore","cols_median_datastore","cols_max_datastore"
            ])

def append_row(row: List[Any]):
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(row)

def main():
    print("[INFO] Starting audit…")
    ensure_output()

    scanned = 0
    for pkg in iterate_packages(ROWS_PER_PAGE, DELAY):
        dataset_id = pkg.get("id")
        dataset_name = pkg.get("title") or pkg.get("name") or dataset_id

        sizes = []
        ds_cols = []   # per-resources column counts (DataStore only)
        ds_rows = []   # per-resources row counts (DataStore only)
        print(dataset_id)
        for res in (pkg.get("resources") or []):
            # SIZE
            b = to_bytes(res.get("size"))
            if b is None and TRY_HEAD_FOR_SIZE and res.get("url"):
                b = head_content_length(res["url"])
            if b is not None:
                sizes.append(b)

            # DATASTORE rows/cols
            if res.get("datastore_active") and res.get("id"):
                stats = get_datastore_counts(res["id"])
                if stats:
                    ds_rows.append(stats["rows"])
                    ds_cols.append(stats["cols"])

            if DELAY: time.sleep(DELAY)

        n_resources = len(pkg.get("resources") or [])
        size_total = sum(sizes) if sizes else 0
        rows_total = sum(ds_rows) if ds_rows else 0

        if ds_cols:
            cols_min = min(ds_cols)
            cols_max = max(ds_cols)
            cols_median = int(statistics.median(ds_cols))
        else:
            cols_min = cols_median = cols_max = 0

        append_row([
            dataset_id, dataset_name, n_resources,
            size_total,
            rows_total,
            cols_min, cols_median, cols_max
        ])

        scanned += 1
        if scanned % 200 == 0:
            print(f"[INFO] Processed {scanned} datasets…")

    print("[INFO] Done. CSV:", OUTPUT_CSV)

if __name__ == "__main__":
    main()
