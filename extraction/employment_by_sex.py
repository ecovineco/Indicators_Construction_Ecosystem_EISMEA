"""
Extract: Employment by sex, age and detailed economic activity (NACE Rev. 2)
Dataset: lfsa_egan22d
Source:  Eurostat
Unit:    Thousands of persons → converted to full persons
"""

import pandas as pd
from extraction.base import BaseExtractor
from extraction.clients.eurostat import fetch_eurostat, find_time_col
from config.nace_codes import API_NACE_CODES, NACE_LABELS, SEX_LABELS


class EmploymentBySexExtractor(BaseExtractor):

    dataset_label = "Employment by Sex"

    def extract(self) -> pd.DataFrame:
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
            "Country":                   raw["geo"].map(lambda g: geo_labels.get(g, g)),
            "Year":                      raw[time_col].astype(int),
            "Sex":                       raw["sex"].map(SEX_LABELS),
            "NACE Code":                 raw["nace_r2"],
            "NACE Label":                raw["nace_r2"].map(NACE_LABELS),
            "Amount of People Employed": (raw["value"] * 1000).round(0).astype(int),
        })

        out.sort_values(["Country", "Year", "NACE Code", "Sex"], inplace=True)
        print(f"  Employment by Sex: {len(out)} clean rows")
        return out
