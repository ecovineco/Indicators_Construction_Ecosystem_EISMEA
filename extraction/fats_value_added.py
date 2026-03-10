"""
Extract: Value added by foreign affiliates (Inward FATS) by controlling country.

Dataset: ``fats_activ``
Source:   Eurostat — Foreign control of enterprises, inward FATS,
          by economic activity and a selection of controlling countries
Indicator: ``indic_sbs`` = ``AV_MEUR`` (Value added — million euro)
Unit:    EUR million (no conversion needed)
Weight data type: ``other``

Extra dimension: ``c_ctrl`` — country of control of the foreign affiliate.
All available controlling country entries are included.

Note: This dataset groups several ecosystem NACE codes:
  - C24_C25 (C25 not available individually)
  - C31_C32 (C31 not available individually)
  - M69-M71 group (M69, M70, M71 not available individually)
  - N78-N82 group (N78, N81 not available individually)
  - E codes only at section level "E"
Only the ecosystem codes available individually are extracted
(see ``API_NACE_CODES_FATS`` in ``config/nace_codes.py``).
"""

import pandas as pd

from extraction.base import BaseExtractor
from extraction.clients.eurostat import fetch_eurostat, find_time_col
from config.nace_codes import API_NACE_CODES_FATS, COUNTRY_CONTROL_LABELS

_CTRL_COUNTRIES = list(COUNTRY_CONTROL_LABELS.keys())


class FatsValueAddedExtractor(BaseExtractor):
    """Extractor for inward FATS value added from Eurostat (fats_activ).

    Fetches value added (EUR million) by foreign-controlled enterprises,
    broken down by the controlling country.

    Class attributes:
        dataset_label: ``"FATS Value Added"``
        weight_data_type: ``"other"``
        unit: ``"EUR million"``
        extra_id_columns: ``["Controlling Country"]``
    """

    dataset_label = "FATS Value Added"
    weight_data_type = "other"
    unit = "EUR million"
    extra_id_columns = ["Controlling Country"]

    def extract(self) -> pd.DataFrame:
        """Fetch inward FATS value added data from Eurostat.

        Returns:
            DataFrame with columns: ``Country Code``, ``Country Name``,
            ``Year``, ``Controlling Country``, ``NACE Code``, ``Value``.

            Values are in EUR million (source unit unchanged).
        """
        raw, geo_labels = fetch_eurostat(
            dataset_code="fats_activ",
            params=[
                ("indic_sbs", "AV_MEUR"),
                *[("c_ctrl", c) for c in _CTRL_COUNTRIES],
            ],
            nace_codes=API_NACE_CODES_FATS,
            aggregate_f=False,
        )

        time_col = find_time_col(raw)

        out = pd.DataFrame({
            "Country Code": raw["geo"],
            "Country Name": raw["geo"].map(lambda g: geo_labels.get(g, g)),
            "Year": raw[time_col].astype(int),
            "Controlling Country": raw["c_ctrl"].map(
                COUNTRY_CONTROL_LABELS
            ),
            "NACE Code": raw["nace_r2"],
            "Value": raw["value"],
        })

        out.sort_values(
            ["Country Code", "Year", "NACE Code", "Controlling Country"],
            inplace=True,
        )
        out.reset_index(drop=True, inplace=True)
        print(f"  FATS Value Added: {len(out)} rows extracted")
        return out
