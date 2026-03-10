"""
Extract: Innovation expenditure share by NACE Rev. 2 activity and size class (CIS2022).

Dataset: ``inn_cis13_exp``
Source:   Eurostat — Innovation active enterprises' expenditure on R&D and
          innovation by type, NACE Rev. 2 activity and size class
Indicator: ``expend`` = ``INN`` (Total innovation expenditure including R&D)
Unit:    Percentage of total expenditure (no conversion needed)
Weight data type: ``other``

Extra dimension: ``sizeclas`` — includes individual size classes plus Total.

Note: This dataset has "F" (Construction) directly available at the
1-letter level — no F41/F42/F43 breakdown. All other ecosystem NACE
codes are available at the 2-digit level.
"""

import pandas as pd

from extraction.base import BaseExtractor
from extraction.clients.eurostat import fetch_eurostat, find_time_col
from config.nace_codes import ALL_NACE_CODES, INNOVATION_SIZE_CLASS_LABELS

_SIZE_CLASSES = list(INNOVATION_SIZE_CLASS_LABELS.keys())


class InnovationExpenditurePctExtractor(BaseExtractor):
    """Extractor for innovation expenditure shares from Eurostat (inn_cis13_exp).

    Fetches total innovation expenditure (including R&D) as a percentage
    of total expenditure, broken down by size class.

    Class attributes:
        dataset_label: ``"Innovation Expend. Pct"``
        weight_data_type: ``"other"``
        unit: ``"Percentage of total expenditure"``
        extra_id_columns: ``["Size Class"]``
    """

    dataset_label = "Innovation Expend. Pct"
    weight_data_type = "other"
    unit = "Percentage of total expenditure"
    extra_id_columns = ["Size Class"]

    def extract(self) -> pd.DataFrame:
        """Fetch innovation expenditure percentage data from Eurostat.

        Returns:
            DataFrame with columns: ``Country Code``, ``Country Name``,
            ``Year``, ``Size Class``, ``NACE Code``, ``Value``.

            Values are percentages of total expenditure.
        """
        raw, geo_labels = fetch_eurostat(
            dataset_code="inn_cis13_exp",
            params=[
                ("unit", "PC_EXP"),
                ("expend", "INN"),
                *[("sizeclas", sc) for sc in _SIZE_CLASSES],
            ],
            nace_codes=ALL_NACE_CODES,
            aggregate_f=False,
        )

        time_col = find_time_col(raw)

        out = pd.DataFrame({
            "Country Code": raw["geo"],
            "Country Name": raw["geo"].map(lambda g: geo_labels.get(g, g)),
            "Year": raw[time_col].astype(int),
            "Size Class": raw["sizeclas"].map(INNOVATION_SIZE_CLASS_LABELS),
            "NACE Code": raw["nace_r2"],
            "Value": raw["value"],
        })

        out.sort_values(
            ["Country Code", "Year", "NACE Code", "Size Class"],
            inplace=True,
        )
        out.reset_index(drop=True, inplace=True)
        print(f"  Innovation Expend. Pct: {len(out)} rows extracted")
        return out
