"""
Extract: Temporary employment by NACE activity.

Dataset: ``lfsa_etgan2``
Source:   Eurostat — Temporary employees by sex, age and NACE Rev. 2
Unit:    Thousands of persons (converted to full persons)
Weight data type: ``persons_employed``

Extra dimensions: ``Sex`` (Males, Females, Total).

Filters applied:
  - ``age`` = ``Y_GE15`` (all ages 15+)
  - ``unit`` = ``THS_PER`` (thousand persons)

Note: This dataset only provides NACE codes at the 1-letter section level.
Only "F" (Construction) is extracted because other letters (C, E, M, N)
mix core and horizontal sub-codes and cannot be meaningfully decomposed.
"""

import pandas as pd

from extraction.base import BaseExtractor
from extraction.clients.eurostat import fetch_eurostat, find_time_col
from config.nace_codes import API_NACE_CODES_1LETTER, SEX_LABELS


class TemporaryEmploymentExtractor(BaseExtractor):
    """Extractor for temporary employment from Eurostat (lfsa_etgan2).

    Produces a long-format DataFrame with one row per
    (country, year, sex, NACE code) combination.

    Class attributes:
        dataset_label: ``"Temporary Employment"``
        weight_data_type: ``"persons_employed"``
        unit: ``"Persons"``
        extra_id_columns: ``["Sex"]``
    """

    dataset_label = "Temporary Employment"
    weight_data_type = "persons_employed"
    unit = "Persons"
    extra_id_columns = ["Sex"]

    def extract(self) -> pd.DataFrame:
        """Fetch temporary employment data from Eurostat.

        Returns:
            DataFrame with columns: ``Country Code``, ``Country Name``,
            ``Year``, ``Sex``, ``NACE Code``, ``Value``.

            Values are in full persons (source unit *thousands* x 1 000).
        """
        raw, geo_labels = fetch_eurostat(
            dataset_code="lfsa_etgan2",
            params=[
                ("age", "Y_GE15"),
                ("unit", "THS_PER"),
                *[("sex", s) for s in SEX_LABELS],
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
            "NACE Code": raw["nace_r2"],
            "Value": (raw["value"] * 1000).round(0),
        })

        out.sort_values(
            ["Country Code", "Year", "NACE Code", "Sex"], inplace=True
        )
        out.reset_index(drop=True, inplace=True)
        print(f"  Temporary Employment: {len(out)} rows extracted")
        return out
