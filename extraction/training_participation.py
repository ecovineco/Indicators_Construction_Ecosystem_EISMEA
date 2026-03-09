"""
Extract: Participation rate of employees in education and training.

Dataset: ``trng_lfs_08b``
Source:   Eurostat
Unit:     Percentage (participation rate, no conversion needed)
Weight data type: ``other``

Extra dimensions: ``Sex`` (Males, Females, Total) and ``Age``.

Note: This dataset only provides NACE codes at the 1-letter section level,
not at the 2-digit level used by other extractors. Only "F" (Construction)
is extracted because other letters (C, E, M, N) mix core and horizontal
sub-codes and cannot be meaningfully decomposed. The remaining NACE codes
in the ecosystem will appear as NaN in the wide output.
"""

import pandas as pd

from extraction.base import BaseExtractor
from extraction.clients.eurostat import fetch_eurostat, find_time_col
from config.nace_codes import API_NACE_CODES_1LETTER, SEX_LABELS, AGE_LABELS_TRAINING


class TrainingParticipationExtractor(BaseExtractor):
    """Extractor for training participation rates from Eurostat (trng_lfs_08b).

    Produces a long-format DataFrame with one row per
    (country, year, sex, age, NACE code) combination.

    Class attributes:
        dataset_label: ``"Training Participation"``
        weight_data_type: ``"other"``
        unit: ``"Percentage"``
        extra_id_columns: ``["Sex", "Age"]``
    """

    dataset_label = "Training Participation"
    weight_data_type = "other"
    unit = "Percentage"
    extra_id_columns = ["Sex", "Age"]

    def extract(self) -> pd.DataFrame:
        """Fetch training participation data from Eurostat.

        Returns:
            DataFrame with columns: ``Country Code``, ``Country Name``,
            ``Year``, ``Sex``, ``Age``, ``NACE Code``, ``Value``.

            Values are percentages (participation rate).
        """
        raw, geo_labels = fetch_eurostat(
            dataset_code="trng_lfs_08b",
            params=[
                ("unit", "PC"),
                *[("sex", s) for s in SEX_LABELS],
                *[("age", a) for a in AGE_LABELS_TRAINING],
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
            "Age": raw["age"].map(AGE_LABELS_TRAINING),
            "NACE Code": raw["nace_r2"],
            "Value": raw["value"],
        })

        out.sort_values(
            ["Country Code", "Year", "NACE Code", "Sex", "Age"], inplace=True
        )
        out.reset_index(drop=True, inplace=True)
        print(f"  Training Participation: {len(out)} rows extracted")
        return out
