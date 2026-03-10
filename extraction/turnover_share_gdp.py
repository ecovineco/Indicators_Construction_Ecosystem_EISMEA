"""
Extract: Share of national construction market turnover in national GDP.

Turnover datasets: ``sbs_sc_con_r2`` (construction F),
    ``sbs_sc_ind_r2`` (industry B-E),
    ``sbs_na_1a_se_r2`` (services H-N, S95).
GDP dataset: ``nama_10_gdp``
Source:   Eurostat
Turnover indicator: ``indic_sb`` = ``V12110`` (Turnover — million euro)
GDP indicator: ``na_item`` = ``B1GQ`` (GDP at market prices, million euro)
Unit:     Percentage (share of national GDP)
Weight data type: ``turnover``

For each (country, year, NACE code) combination, computes::

    share = country_nace_turnover / country_GDP * 100

Both turnover and GDP are in EUR million, so the ratio is dimensionless.

The three SBS datasets cover non-overlapping NACE ranges.  This extractor
fetches turnover from all three and GDP from ``nama_10_gdp``, then computes
the share for each EU-27 member state.
"""

import pandas as pd

from extraction.base import BaseExtractor
from extraction.clients.eurostat import fetch_eurostat, find_time_col
from config.nace_codes import API_NACE_CODES

# EU-27 member state geo codes
_EU27_COUNTRIES = {
    "AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "EL", "ES",
    "FI", "FR", "HR", "HU", "IE", "IT", "LT", "LU", "LV", "MT",
    "NL", "PL", "PT", "RO", "SE", "SI", "SK",
}

# SBS datasets and the NACE codes each one covers
_SBS_DATASETS = {
    "sbs_sc_con_r2": ["F41", "F42", "F43"],
    "sbs_sc_ind_r2": ["C25", "C28", "C31", "C33"],
    "sbs_na_1a_se_r2": [
        "E36", "E37", "E38", "E39",
        "M69", "M70", "M71", "M72",
        "N77", "N78", "N81",
    ],
}


def _fetch_gdp() -> pd.DataFrame:
    """Fetch GDP at market prices (million EUR) from Eurostat.

    Uses dataset ``nama_10_gdp`` with indicator ``B1GQ`` (GDP at market
    prices) in unit ``CP_MEUR`` (current prices, million EUR).

    Returns:
        DataFrame with columns: ``Country Code``, ``Year``, ``GDP``.
    """
    raw, geo_labels = fetch_eurostat(
        dataset_code="nama_10_gdp",
        params=[
            ("na_item", "B1GQ"),
            ("unit", "CP_MEUR"),
        ],
        nace_codes=None,
        aggregate_f=False,
    )

    time_col = find_time_col(raw)

    gdp = pd.DataFrame({
        "Country Code": raw["geo"],
        "Year": raw[time_col].astype(int),
        "GDP": raw["value"],
    })

    # Keep only EU-27 countries
    gdp = gdp[gdp["Country Code"].isin(_EU27_COUNTRIES)].copy()
    gdp.reset_index(drop=True, inplace=True)
    print(f"  GDP: {len(gdp)} rows fetched")
    return gdp


class TurnoverShareGdpExtractor(BaseExtractor):
    """Extractor for each country's construction turnover as a share of GDP.

    For every (country, year, NACE code), the value is::

        country_nace_turnover / country_GDP * 100

    Fetches turnover (V12110, EUR million) from three SBS datasets and
    GDP at market prices (B1GQ, EUR million) from ``nama_10_gdp``.

    Class attributes:
        dataset_label: ``"Turnover Share of GDP"``
        weight_data_type: ``"turnover"``
        unit: ``"Percentage"``
        extra_id_columns: ``[]``
    """

    dataset_label = "Turnover Share of GDP"
    weight_data_type = "turnover"
    unit = "Percentage"
    extra_id_columns: list[str] = []

    def extract(self) -> pd.DataFrame:
        """Fetch turnover and GDP data and compute the turnover/GDP share.

        Returns:
            DataFrame with columns: ``Country Code``, ``Country Name``,
            ``Year``, ``NACE Code``, ``Value``.

            Values are percentages (NACE turnover / national GDP × 100).
        """
        # --- Fetch turnover from three SBS datasets --------------------------
        frames: list[pd.DataFrame] = []

        for dataset_code, nace_subset in _SBS_DATASETS.items():
            raw, geo_labels = fetch_eurostat(
                dataset_code=dataset_code,
                params=[("indic_sb", "V12110")],
                nace_codes=nace_subset,
                aggregate_f=True,
            )

            time_col = find_time_col(raw)

            chunk = pd.DataFrame({
                "Country Code": raw["geo"],
                "Country Name": raw["geo"].map(
                    lambda g, gl=geo_labels: gl.get(g, g)
                ),
                "Year": raw[time_col].astype(int),
                "NACE Code": raw["nace_r2"],
                "Value": raw["value"],
            })
            frames.append(chunk)

        turnover = pd.concat(frames, ignore_index=True)

        # Keep only EU-27 member states
        turnover = turnover[
            turnover["Country Code"].isin(_EU27_COUNTRIES)
        ].copy()

        # Preserve country name mapping for final output
        country_names = (
            turnover[["Country Code", "Country Name"]]
            .drop_duplicates()
            .set_index("Country Code")["Country Name"]
            .to_dict()
        )

        # --- Fetch GDP -------------------------------------------------------
        gdp = _fetch_gdp()

        # --- Compute share ---------------------------------------------------
        merged = turnover.merge(
            gdp, on=["Country Code", "Year"], how="left"
        )
        merged["Value"] = (
            (merged["Value"] / merged["GDP"]) * 100
        ).round(4)

        out = merged[
            ["Country Code", "Country Name", "Year", "NACE Code", "Value"]
        ].copy()

        # Drop rows where share could not be computed (missing GDP)
        out.dropna(subset=["Value"], inplace=True)

        out.sort_values(
            ["Country Code", "Year", "NACE Code"], inplace=True
        )
        out.reset_index(drop=True, inplace=True)
        print(f"  Turnover Share of GDP: {len(out)} rows extracted")
        return out
