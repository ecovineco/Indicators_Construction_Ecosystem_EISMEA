"""
Eurostat API client.

Reusable functions for fetching any dataset from the Eurostat Statistics API
(JSON-stat 2.0). No dataset-specific logic lives here — just the mechanics
of calling the API, parsing the response, and handling quirks like the
F41+F42+F43 aggregation.

Usage:
    from extraction.clients.eurostat import fetch_eurostat

    df, geo_labels = fetch_eurostat(
        dataset_code="lfsa_egan22d",
        params=[("age", "Y_GE15"), ("sex", "M"), ("sex", "F")],
        nace_codes=["C25", "C28", "F41", "F42", "F43"],
        aggregate_f=True,
    )
"""

import itertools
import time
import requests
import pandas as pd

BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
COMEXT_BASE_URL = "https://ec.europa.eu/eurostat/api/comext/dissemination/statistics/1.0/data"

F_SUB_CODES = ["F41", "F42", "F43"]


def fetch_eurostat(
    dataset_code: str,
    params: list[tuple[str, str]] | None = None,
    nace_codes: list[str] | None = None,
    aggregate_f: bool = True,
    comext: bool = False,
    timeout: int = 180,
) -> tuple[pd.DataFrame, dict[str, str]]:
    """
    Fetch a dataset from the Eurostat Statistics API.

    Parameters
    ----------
    dataset_code : str
        Eurostat dataset code (e.g. "lfsa_egan22d").
    params : list of (key, value) tuples
        Dataset-specific query parameters (filters, unit, age, sex, etc.).
        Do NOT include format, lang, or nace_r2 here.
    nace_codes : list of str
        NACE codes to request. If None, no nace_r2 filter is applied.
    aggregate_f : bool
        If True and F41/F42/F43 are in the data, sum them into a single "F" row.
    comext : bool
        If True, use the Comext/Prodcom endpoint (for DS-prefixed datasets).
    timeout : int
        Request timeout in seconds.

    Returns
    -------
    (DataFrame, geo_labels)
        DataFrame with one column per dimension + a "value" column.
        geo_labels is a dict mapping geo codes to country names.
    """
    base = COMEXT_BASE_URL if comext else BASE_URL
    url = f"{base}/{dataset_code}"

    query = [("format", "JSON"), ("lang", "EN")]
    if params:
        query += params
    if nace_codes:
        query += [("nace_r2", c) for c in nace_codes]

    print(f"  Fetching {dataset_code} from Eurostat...")
    raw_json = _call_api(url, query, timeout)

    df, geo_labels = _parse_jsonstat(raw_json)
    print(f"  {dataset_code}: {len(df)} raw rows")

    if aggregate_f and "nace_r2" in df.columns:
        df = _aggregate_f(df)

    return df, geo_labels


def _call_api(url: str, params: list, timeout: int) -> dict:
    """Make the HTTP request, handling async responses."""
    resp = requests.get(url, params=params, timeout=timeout)

    if resp.status_code == 200:
        return resp.json()

    if resp.status_code in (400, 413) and "ASYNCHRONOUS" in resp.text.upper():
        print("  Large dataset — waiting 30s for async processing...")
        time.sleep(30)
        resp = requests.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    resp.raise_for_status()


def _parse_jsonstat(data: dict) -> tuple[pd.DataFrame, dict]:
    """Parse a JSON-stat 2.0 response into a flat DataFrame."""
    dims = data["id"]
    values = data["value"]

    dim_codes = {}
    for d in dims:
        cat = data["dimension"][d]["category"]
        idx = cat["index"]
        dim_codes[d] = (
            sorted(idx.keys(), key=lambda k: idx[k])
            if isinstance(idx, dict)
            else idx
        )

    geo_labels = {}
    if "geo" in dims:
        geo_labels = data["dimension"]["geo"]["category"].get("label", {})

    rows = []
    for flat_idx, combo in enumerate(itertools.product(*[dim_codes[d] for d in dims])):
        val = values.get(str(flat_idx))
        if val is not None:
            row = dict(zip(dims, combo))
            row["value"] = val
            rows.append(row)

    return pd.DataFrame(rows), geo_labels


def _aggregate_f(df: pd.DataFrame) -> pd.DataFrame:
    """Sum F41+F42+F43 into a single 'F' row if those codes are present."""
    f_present = df[df["nace_r2"].isin(F_SUB_CODES)]
    if f_present.empty:
        return df

    group_cols = [c for c in df.columns if c not in ("nace_r2", "value")]
    f_agg = f_present.groupby(group_cols, as_index=False)["value"].sum()
    f_agg["nace_r2"] = "F"
    return pd.concat([df, f_agg], ignore_index=True)


def find_time_col(df: pd.DataFrame) -> str:
    """Find the time dimension column name (varies across datasets)."""
    for c in df.columns:
        if "time" in c.lower():
            return c
    return "time"
