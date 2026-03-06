"""
Base extractor class.

Provides a standard interface and metadata that all dataset extractors
must follow.  The actual API calls are delegated to the appropriate
client (e.g. ``extraction.clients.eurostat``).

Each concrete extractor must set the class-level metadata attributes
and implement :meth:`extract`.

Standard long-format output columns
------------------------------------
Every extractor's :meth:`extract` must return a DataFrame containing
**at least** these columns:

    =================  ================================================
    Column             Description
    =================  ================================================
    ``Country Code``   ISO 2-letter geo code (e.g. ``"AT"``)
    ``Country Name``   Full country name (e.g. ``"Austria"``)
    ``Year``           Calendar year as ``int``
    ``NACE Code``      NACE Rev. 2 code (e.g. ``"C25"``, ``"F41"``)
    ``Value``          Numeric measurement value
    =================  ================================================

Additional dimension columns (e.g. ``Sex``) are listed in
:attr:`extra_id_columns`.
"""

import pandas as pd
from abc import ABC, abstractmethod


class BaseExtractor(ABC):
    """Abstract base class for all dataset extractors.

    Class attributes:
        dataset_label (str): Human-readable name used for sheet names
            and log messages (e.g. ``"Employment by Sex"``).
        weight_data_type (str): Key into the weight matrix identifying
            which weight sheet applies to this dataset (e.g.
            ``"persons_employed"``, ``"turnover"``).
        unit (str): Unit of measurement for the ``Value`` column
            (e.g. ``"Persons"``, ``"EUR"``).
        extra_id_columns (list[str]): Names of any dimension columns
            beyond ``Country Code``, ``Country Name``, and ``Year``
            that appear in the extraction output (e.g. ``["Sex"]``).
    """

    dataset_label: str = ""
    weight_data_type: str = ""
    unit: str = ""
    extra_id_columns: list[str] = []

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Run the extraction and return a standardised long-format DataFrame.

        Returns:
            DataFrame with columns ``Country Code``, ``Country Name``,
            ``Year``, ``NACE Code``, ``Value``, plus any columns named
            in :attr:`extra_id_columns`.
        """
        ...