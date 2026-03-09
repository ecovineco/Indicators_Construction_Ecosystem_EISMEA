"""
Extract: Number of enterprises by size class and NACE activity.

Datasets: ``sbs_sc_con_r2`` (construction F), ``sbs_sc_ind_r2`` (industry B-E).
Source:   Eurostat
Indicator: ``indic_sb`` = ``V11110`` (Enterprises — number)
Unit:     Number of enterprises (no conversion needed)
Weight data type: ``enterprises``

Note: ``sbs_na_1a_se_r2`` (services) does NOT have a ``size_emp`` dimension
and is therefore excluded from this extractor. Only the two datasets with
size-class breakdowns are used.

Extra dimension: ``size_emp`` — includes individual size classes plus Total.
"""

import pandas as pd

from extraction.base import BaseExtractor
from extraction.clients.eurostat import fetch_eurostat, find_time_col
from config.nace_codes import SIZE_CLASS_LABELS

# SBS datasets with size_emp dimension and their relevant NACE codes
_SBS_DATASETS_WITH_SIZE = {
    "sbs_sc_con_r2": ["F41", "F42", "F43"],
    "sbs_sc_ind_r2": ["C25", "C28", "C31", "C33"],
}

_SIZE_CLASSES = list(SIZE_CLASS_LABELS.keys())


class EnterprisesBySizeExtractor(BaseExtractor):
    """Extractor for enterprise counts by size class from two SBS datasets.

    Fetches indicator V11110 (Number of enterprises) broken down by
    ``size_emp`` from sbs_sc_con_r2 and sbs_sc_ind_r2, concatenates the
    results, and returns a unified long-format DataFrame.

    Class attributes:
        dataset_label: ``"Enterprises by Size"``
        weight_data_type: ``"enterprises"``
        unit: ``"Number"``
        extra_id_columns: ``["Size Class"]``
    """

    dataset_label = "Enterprises by Size"
    weight_data_type = "enterprises"
    unit = "Number"
    extra_id_columns = ["Size Class"]

    def extract(self) -> pd.DataFrame:
        """Fetch enterprise data by size class and return combined output.

        Returns:
            DataFrame with columns: ``Country Code``, ``Country Name``,
            ``Year``, ``Size Class``, ``NACE Code``, ``Value``.

            Values are raw counts (number of enterprises).
        """
        frames: list[pd.DataFrame] = []

        for dataset_code, nace_subset in _SBS_DATASETS_WITH_SIZE.items():
            raw, geo_labels = fetch_eurostat(
                dataset_code=dataset_code,
                params=[
                    ("indic_sb", "V11110"),
                    *[("size_emp", sc) for sc in _SIZE_CLASSES],
                ],
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
                "Size Class": raw["size_emp"].map(SIZE_CLASS_LABELS),
                "NACE Code": raw["nace_r2"],
                "Value": raw["value"],
            })
            frames.append(chunk)

        out = pd.concat(frames, ignore_index=True)
        out.sort_values(
            ["Country Code", "Year", "NACE Code", "Size Class"], inplace=True
        )
        out.reset_index(drop=True, inplace=True)
        print(f"  Enterprises by Size: {len(out)} rows extracted")
        return out
