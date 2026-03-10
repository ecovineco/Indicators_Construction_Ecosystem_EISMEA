"""
Extract: Production in construction — production volume index (monthly).

Dataset: ``sts_copr_m``
Source:   Eurostat — Production in construction and industry, monthly data
Indicator: ``indic_bt`` = ``PRD`` (Production volume)
Unit:    Index, 2021=100 (no conversion needed)
Weight data type: ``other``

Seasonal adjustment: ``s_adj`` = ``SCA`` (seasonally and calendar adjusted)

Conversion: This dataset contains monthly data (time format ``YYYY-MM``).
Values are converted to yearly by averaging all available months within
each year.

Note: This dataset only provides F-section NACE codes (F, F41, F42, F43)
from the construction ecosystem. F41+F42+F43 are aggregated into a single
"F" row using the standard pipeline aggregation.
"""

import pandas as pd

from extraction.base import BaseExtractor
from extraction.clients.eurostat import fetch_eurostat, find_time_col
from config.nace_codes import F_SUB_CODES


class ProductionIndexExtractor(BaseExtractor):
    """Extractor for construction production volume index (sts_copr_m).

    Fetches monthly production volume index (seasonally and calendar
    adjusted, 2021=100) and averages monthly values into yearly figures.

    Class attributes:
        dataset_label: ``"Production Index"``
        weight_data_type: ``"other"``
        unit: ``"Index (2021=100)"``
        extra_id_columns: ``[]``
    """

    dataset_label = "Production Index"
    weight_data_type = "other"
    unit = "Index (2021=100)"
    extra_id_columns: list[str] = []

    def extract(self) -> pd.DataFrame:
        """Fetch monthly production index and average to yearly values.

        Returns:
            DataFrame with columns: ``Country Code``, ``Country Name``,
            ``Year``, ``NACE Code``, ``Value``.

            Values are yearly averages of the monthly production volume
            index (2021=100, seasonally and calendar adjusted).
        """
        raw, geo_labels = fetch_eurostat(
            dataset_code="sts_copr_m",
            params=[
                ("unit", "I21"),
                ("s_adj", "SCA"),
                ("indic_bt", "PRD"),
            ],
            nace_codes=F_SUB_CODES,
            aggregate_f=True,
        )

        time_col = find_time_col(raw)

        # Parse YYYY-MM time format into Year
        raw["Year"] = raw[time_col].str[:4].astype(int)

        # Average monthly values within each (geo, nace_r2, year) group
        grouped = (
            raw.groupby(["geo", "nace_r2", "Year"], as_index=False)["value"]
            .mean()
        )

        out = pd.DataFrame({
            "Country Code": grouped["geo"],
            "Country Name": grouped["geo"].map(
                lambda g: geo_labels.get(g, g)
            ),
            "Year": grouped["Year"],
            "NACE Code": grouped["nace_r2"],
            "Value": grouped["value"].round(2),
        })

        out.sort_values(
            ["Country Code", "Year", "NACE Code"], inplace=True
        )
        out.reset_index(drop=True, inplace=True)
        print(f"  Production Index: {len(out)} rows extracted")
        return out
