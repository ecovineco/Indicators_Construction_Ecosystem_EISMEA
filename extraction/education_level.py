"""
Extract: Employees by education level (ISCED 2011) and NACE activity.

Dataset: ``edat_lfs_9910``
Source:   Eurostat — Percentage of employees by education level
Unit:    Percentage (no conversion needed)
Weight data type: ``other``

Extra dimensions: ``Sex`` (Males, Females, Total), ``Age`` (25-64 working-age
bracket), ``Education Level`` (three ISCED groupings).

Note: This dataset only provides NACE codes at the 1-letter section level.
Only "F" (Construction) is extracted because other letters (C, E, M, N)
mix core and horizontal sub-codes and cannot be meaningfully decomposed.
"""

import pandas as pd

from extraction.base import BaseExtractor
from extraction.clients.eurostat import fetch_eurostat, find_time_col
from config.nace_codes import API_NACE_CODES_1LETTER, SEX_LABELS, ISCED_LABELS


class EducationLevelExtractor(BaseExtractor):
    """Extractor for education level shares from Eurostat (edat_lfs_9910).

    Produces a long-format DataFrame with one row per
    (country, year, sex, education level, NACE code) combination.

    Class attributes:
        dataset_label: ``"Education Level"``
        weight_data_type: ``"other"``
        unit: ``"Percentage"``
        extra_id_columns: ``["Sex", "Education Level"]``
    """

    dataset_label = "Education Level"
    weight_data_type = "other"
    unit = "Percentage"
    extra_id_columns = ["Sex", "Education Level"]

    def extract(self) -> pd.DataFrame:
        """Fetch education level data from Eurostat.

        Returns:
            DataFrame with columns: ``Country Code``, ``Country Name``,
            ``Year``, ``Sex``, ``Education Level``, ``NACE Code``, ``Value``.

            Values are percentages of total employment.
        """
        raw, geo_labels = fetch_eurostat(
            dataset_code="edat_lfs_9910",
            params=[
                ("unit", "PC"),
                ("age", "Y25-64"),
                *[("sex", s) for s in SEX_LABELS],
                *[("isced11", c) for c in ISCED_LABELS],
            ],
            nace_codes=API_NACE_CODES_1LETTER,
            aggregate_f=False,
        )

        time_col = find_time_col(raw)

        out = pd.DataFrame({
            "Country Code": raw["geo"],
            "Country Name": raw["geo"].map(lambda g: geo_labels.get(g, g)),
            "Year": raw[time_col].astype(int),
            "Sex": raw["sex"].map(SEX_LABELS),
            "Education Level": raw["isced11"].map(ISCED_LABELS),
            "NACE Code": raw["nace_r2"],
            "Value": raw["value"],
        })

        out.sort_values(
            ["Country Code", "Year", "NACE Code", "Sex", "Education Level"],
            inplace=True,
        )
        out.reset_index(drop=True, inplace=True)
        print(f"  Education Level: {len(out)} rows extracted")
        return out
