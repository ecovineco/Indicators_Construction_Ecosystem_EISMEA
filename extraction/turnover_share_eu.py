"""
Extract: Share of national construction market turnover in EU construction market turnover.

Datasets: ``sbs_sc_con_r2`` (construction F), ``sbs_sc_ind_r2`` (industry B-E),
          ``sbs_na_1a_se_r2`` (services H-N, S95).
Source:   Eurostat
Indicator: ``indic_sb`` = ``V12110`` (Turnover — million euro)
Unit:     Percentage (share of EU-27 total)
Weight data type: ``turnover``

For each (country, year, NACE code) combination, computes::

    share = country_turnover / EU27_turnover * 100

The EU-27 aggregate (geo code ``EU27_2020``) is fetched alongside individual
country data and used as the denominator.  Only EU-27 member states appear
in the final output; the EU-27 aggregate row itself is excluded.

The three SBS datasets cover non-overlapping NACE ranges.  This extractor
fetches from all three and concatenates the results, keeping only the NACE
codes relevant to the construction ecosystem.
"""

import pandas as pd

from extraction.base import BaseExtractor
from extraction.clients.eurostat import fetch_eurostat, find_time_col
from config.nace_codes import API_NACE_CODES

# EU-27 member state geo codes (used to filter out non-member rows)
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


class TurnoverShareEuExtractor(BaseExtractor):
    """Extractor for each country's share of the EU-27 construction turnover.

    For every (country, year, NACE code), the value is::

        country_turnover / EU27_2020_turnover * 100

    Fetches turnover indicator V12110 from three SBS datasets, extracts
    the EU27_2020 aggregate as the denominator, and computes the share
    for each member state.

    Class attributes:
        dataset_label: ``"Turnover Share of EU"``
        weight_data_type: ``"turnover"``
        unit: ``"Percentage"``
        extra_id_columns: ``[]``
    """

    dataset_label = "Turnover Share of EU"
    weight_data_type = "turnover"
    unit = "Percentage"
    extra_id_columns: list[str] = []

    def extract(self) -> pd.DataFrame:
        """Fetch turnover data and compute each country's EU market share.

        Returns:
            DataFrame with columns: ``Country Code``, ``Country Name``,
            ``Year``, ``NACE Code``, ``Value``.

            Values are percentages (country turnover / EU-27 turnover × 100).
        """
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

        # --- Split EU-27 aggregate (denominator) from member states ----------
        eu_total = turnover[turnover["Country Code"] == "EU27_2020"].copy()
        eu_total = eu_total.rename(columns={"Value": "EU27 Turnover"})
        eu_total = eu_total[["Year", "NACE Code", "EU27 Turnover"]]

        country_data = turnover[
            turnover["Country Code"].isin(_EU27_COUNTRIES)
        ].copy()

        # --- Compute share ---------------------------------------------------
        merged = country_data.merge(
            eu_total, on=["Year", "NACE Code"], how="left"
        )
        merged["Value"] = (
            (merged["Value"] / merged["EU27 Turnover"]) * 100
        ).round(4)

        out = merged[
            ["Country Code", "Country Name", "Year", "NACE Code", "Value"]
        ].copy()

        # Drop rows where share could not be computed (missing EU total)
        out.dropna(subset=["Value"], inplace=True)

        out.sort_values(
            ["Country Code", "Year", "NACE Code"], inplace=True
        )
        out.reset_index(drop=True, inplace=True)
        print(f"  Turnover Share of EU: {len(out)} rows extracted")
        return out
