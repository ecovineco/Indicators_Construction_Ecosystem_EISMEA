"""
Extract: Annual turnover by NACE activity from Structural Business Statistics.

Datasets: ``sbs_sc_con_r2`` (construction F), ``sbs_sc_ind_r2`` (industry B-E),
          ``sbs_na_1a_se_r2`` (services H-N, S95).
Source:   Eurostat
Indicator: ``indic_sb`` = ``V12110`` (Turnover — million euro)
Unit:     EUR million (no conversion needed)
Weight data type: ``turnover``

The three SBS datasets cover non-overlapping NACE ranges. This extractor
fetches from all three and concatenates the results, keeping only the NACE
codes relevant to the construction ecosystem.
"""

import pandas as pd

from extraction.base import BaseExtractor
from extraction.clients.eurostat import fetch_eurostat, find_time_col
from config.nace_codes import API_NACE_CODES

# SBS datasets and the NACE codes each one covers
_SBS_DATASETS = {
    "sbs_sc_con_r2": ["F41", "F42", "F43"],              # Construction
    "sbs_sc_ind_r2": ["C25", "C28", "C31", "C33"],       # Industry (B-E)
    "sbs_na_1a_se_r2": [                                  # Services (H-N)
        "E36", "E37", "E38", "E39",
        "M69", "M70", "M71", "M72",
        "N77", "N78", "N81",
    ],
}


class TurnoverExtractor(BaseExtractor):
    """Extractor for annual turnover from three SBS Eurostat datasets.

    Fetches indicator V12110 (Turnover, million EUR) from each SBS dataset,
    concatenates the results, and returns a unified long-format DataFrame.

    Class attributes:
        dataset_label: ``"Turnover"``
        weight_data_type: ``"turnover"``
        unit: ``"EUR million"``
        extra_id_columns: ``[]``
    """

    dataset_label = "Turnover"
    weight_data_type = "turnover"
    unit = "EUR million"
    extra_id_columns: list[str] = []

    def extract(self) -> pd.DataFrame:
        """Fetch turnover data from three SBS datasets and return combined output.

        Returns:
            DataFrame with columns: ``Country Code``, ``Country Name``,
            ``Year``, ``NACE Code``, ``Value``.

            Values are in EUR million (source unit unchanged).
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

        out = pd.concat(frames, ignore_index=True)
        out.sort_values(
            ["Country Code", "Year", "NACE Code"], inplace=True
        )
        out.reset_index(drop=True, inplace=True)
        print(f"  Turnover: {len(out)} rows extracted")
        return out
