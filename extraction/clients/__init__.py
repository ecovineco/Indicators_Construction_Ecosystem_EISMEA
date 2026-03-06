"""
API clients — one module per data source.

Each client handles the mechanics of calling a specific API and
returning a standardised pandas DataFrame. No dataset-specific
logic belongs here.

Available clients:
    eurostat  — Eurostat Statistics API (JSON-stat 2.0)
    # oecd   — (future) OECD.Stat API
    # wb     — (future) World Bank API
"""
