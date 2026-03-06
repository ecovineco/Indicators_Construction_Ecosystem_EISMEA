"""
Extract: Employment by sex, age and detailed economic activity (NACE Rev. 2).

Dataset: ``lfsa_egan22d``
Source:  Eurostat
Unit:    Thousands of persons (converted to full persons)
Weight data type: ``persons_employed``
"""

import pandas as pd

from extraction.base import BaseExtractor
from extraction.clients.eurostat import fetch_eurostat, find_time_col
from config.nace_codes import API_NACE_CODES, SEX_LABELS


class EmploymentBySexExtractor(BaseExtractor):
    """Extractor for Eurostat employment-by-sex dataset (lfsa_egan22d).

    Produces a long-format DataFrame with one row per
    (country, year, sex, NACE code) combination.

    Class attributes:
        dataset_label: ``"Employment by Sex"``
        weight_data_type: ``"persons_employed"``
        unit: ``"Persons"``
        extra_id_columns: ``["Sex"]``
    """

    dataset_label = "Employment by Sex"
    weight_data_type = "persons_employed"
    unit = "Persons"
    extra_id_columns = ["Sex"]

    def extract(self) -> pd.DataFrame:
        """Fetch employment data from Eurostat and return standardised output.

        Returns:
            DataFrame with columns: ``Country Code``, ``Country Name``,
            ``Year``, ``Sex``, ``NACE Code``, ``Value``.

            Values are in full persons (source unit *thousands* × 1 000).
        """
        raw, geo_labels = fetch_eurostat(
            dataset_code="lfsa_egan22d",
            params=[
                ("age", "Y_GE15"),
                ("unit", "THS_PER"),
                ("sex", "M"),
                ("sex", "F"),
                ("sex", "T"),
            ],
            nace_codes=API_NACE_CODES,
            aggregate_f=True,
        )

        time_col = find_time_col(raw)

        out = pd.DataFrame({
            "Country Code": raw["geo"],
            "Country Name": raw["geo"].map(lambda g: geo_labels.get(g, g)),
            "Year": raw[time_col].astype(int),
            "Sex": raw["sex"].map(SEX_LABELS),
            "NACE Code": raw["nace_r2"],
            "Value": (raw["value"] * 1000).round(0),
        })

        out.sort_values(
            ["Country Code", "Year", "NACE Code", "Sex"], inplace=True
        )
        out.reset_index(drop=True, inplace=True)
        print(f"  Employment by Sex: {len(out)} rows extracted")
        return out