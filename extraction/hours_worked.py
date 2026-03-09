"""
Extract: Average usual weekly hours worked in the main job by NACE activity.

Dataset: ``lfsa_ewhun2``
Source:   Eurostat
Unit:     Hours (no conversion needed)
Weight data type: ``other``

Filters applied:
  - ``wstatus`` = ``EMP`` (all employed persons)
  - ``worktime`` = ``TOTAL`` (full-time and part-time combined)
  - ``age`` = ``Y_GE15`` (all ages 15+)
  - ``sex`` = ``T`` (total, both sexes)

Note: This dataset only provides NACE codes at the 1-letter section level,
not at the 2-digit level used by other extractors. Only "F" (Construction)
is extracted because other letters (C, E, M, N) mix core and horizontal
sub-codes and cannot be meaningfully decomposed. The remaining NACE codes
in the ecosystem will appear as NaN in the wide output.
"""

import pandas as pd

from extraction.base import BaseExtractor
from extraction.clients.eurostat import fetch_eurostat, find_time_col
from config.nace_codes import API_NACE_CODES_1LETTER


class HoursWorkedExtractor(BaseExtractor):
    """Extractor for average weekly hours worked from Eurostat (lfsa_ewhun2).

    Produces a long-format DataFrame with one row per
    (country, year, NACE code) combination.

    Class attributes:
        dataset_label: ``"Hours Worked"``
        weight_data_type: ``"other"``
        unit: ``"Hours"``
        extra_id_columns: ``[]``
    """

    dataset_label = "Hours Worked"
    weight_data_type = "other"
    unit = "Hours"
    extra_id_columns: list[str] = []

    def extract(self) -> pd.DataFrame:
        """Fetch weekly hours worked data from Eurostat.

        Returns:
            DataFrame with columns: ``Country Code``, ``Country Name``,
            ``Year``, ``NACE Code``, ``Value``.

            Values are average usual weekly hours.
        """
        raw, geo_labels = fetch_eurostat(
            dataset_code="lfsa_ewhun2",
            params=[
                ("unit", "HR"),
                ("wstatus", "EMP"),
                ("worktime", "TOTAL"),
                ("age", "Y_GE15"),
                ("sex", "T"),
            ],
            nace_codes=API_NACE_CODES_1LETTER,
            aggregate_f=False,
        )

        time_col = find_time_col(raw)

        out = pd.DataFrame({
            "Country Code": raw["geo"],
            "Country Name": raw["geo"].map(lambda g: geo_labels.get(g, g)),
            "Year": raw[time_col].astype(int),
            "NACE Code": raw["nace_r2"],
            "Value": raw["value"],
        })

        out.sort_values(
            ["Country Code", "Year", "NACE Code"], inplace=True
        )
        out.reset_index(drop=True, inplace=True)
        print(f"  Hours Worked: {len(out)} rows extracted")
        return out
