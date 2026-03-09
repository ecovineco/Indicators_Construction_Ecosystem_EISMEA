"""
Extract: Business enterprise R&D expenditure (BERD) by NACE Rev. 2 activity.

Dataset: ``rd_e_berdindr2``
Source:   Eurostat
Unit:     Million euro (no conversion needed)
Weight data type: ``other``

Note: This dataset has a mixed set of NACE codes. Not all standard 2-digit
ecosystem codes are available individually:
  - E codes only exist as grouped "E37-E39" (E36 not available separately)
  - F is available directly (no F41/F42/F43 breakdown)
  - M69/M70 are not available individually
Only the codes that are available at the individual level are extracted
(see ``API_NACE_CODES_RD`` in ``config/nace_codes.py``).
"""

import pandas as pd

from extraction.base import BaseExtractor
from extraction.clients.eurostat import fetch_eurostat, find_time_col
from config.nace_codes import API_NACE_CODES_RD


class RdExpenditureExtractor(BaseExtractor):
    """Extractor for R&D expenditure from Eurostat (rd_e_berdindr2).

    Fetches BERD in million EUR for the available construction ecosystem
    NACE codes.

    Class attributes:
        dataset_label: ``"R&D Expenditure"``
        weight_data_type: ``"other"``
        unit: ``"EUR million"``
        extra_id_columns: ``[]``
    """

    dataset_label = "R&D Expenditure"
    weight_data_type = "other"
    unit = "EUR million"
    extra_id_columns: list[str] = []

    def extract(self) -> pd.DataFrame:
        """Fetch R&D expenditure data from Eurostat.

        Returns:
            DataFrame with columns: ``Country Code``, ``Country Name``,
            ``Year``, ``NACE Code``, ``Value``.

            Values are in EUR million (source unit unchanged).
        """
        raw, geo_labels = fetch_eurostat(
            dataset_code="rd_e_berdindr2",
            params=[("unit", "MIO_EUR")],
            nace_codes=API_NACE_CODES_RD,
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
        print(f"  R&D Expenditure: {len(out)} rows extracted")
        return out
