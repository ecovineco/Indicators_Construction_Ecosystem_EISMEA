"""
Extract: Employment by age class and sex from the Labour Force Survey.

Dataset: ``lfsa_egan22d``
Source:   Eurostat
Unit:     Thousands of persons (converted to full persons)
Weight data type: ``persons_employed``

Extra dimensions: ``Sex`` (Males, Females, Total) and ``Age`` (five classes).
This extractor uses the same underlying dataset as ``employment_by_sex.py``
but adds an age breakdown.
"""

import pandas as pd

from extraction.base import BaseExtractor
from extraction.clients.eurostat import fetch_eurostat, find_time_col
from config.nace_codes import API_NACE_CODES, SEX_LABELS, AGE_LABELS


class EmploymentByAgeSexExtractor(BaseExtractor):
    """Extractor for Eurostat employment-by-age-and-sex dataset (lfsa_egan22d).

    Produces a long-format DataFrame with one row per
    (country, year, sex, age, NACE code) combination.

    Class attributes:
        dataset_label: ``"Employment by Age and Sex"``
        weight_data_type: ``"persons_employed"``
        unit: ``"Persons"``
        extra_id_columns: ``["Sex", "Age"]``
    """

    dataset_label = "Employment by Age and Sex"
    weight_data_type = "persons_employed"
    unit = "Persons"
    extra_id_columns = ["Sex", "Age"]

    def extract(self) -> pd.DataFrame:
        """Fetch employment data by age and sex from Eurostat.

        Returns:
            DataFrame with columns: ``Country Code``, ``Country Name``,
            ``Year``, ``Sex``, ``Age``, ``NACE Code``, ``Value``.

            Values are in full persons (source unit *thousands* × 1 000).
        """
        raw, geo_labels = fetch_eurostat(
            dataset_code="lfsa_egan22d",
            params=[
                ("unit", "THS_PER"),
                *[("sex", s) for s in SEX_LABELS],
                *[("age", a) for a in AGE_LABELS],
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
            "Age": raw["age"].map(AGE_LABELS),
            "NACE Code": raw["nace_r2"],
            "Value": (raw["value"] * 1000).round(0),
        })

        out.sort_values(
            ["Country Code", "Year", "NACE Code", "Sex", "Age"], inplace=True
        )
        out.reset_index(drop=True, inplace=True)
        print(f"  Employment by Age and Sex: {len(out)} rows extracted")
        return out
