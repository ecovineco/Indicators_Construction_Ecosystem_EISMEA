"""
Base extractor class.

Provides a standard interface that all dataset extractors follow.
The actual API calls are delegated to the appropriate client
(e.g. extraction.clients.eurostat).

Subclasses just implement extract() → pd.DataFrame.
"""

import pandas as pd
from abc import ABC, abstractmethod


class BaseExtractor(ABC):
    """
    All extractors must implement:
        dataset_label: str  — human-readable name (used for sheet names, logs)
        extract() -> pd.DataFrame
    """

    dataset_label: str = ""

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Run the extraction and return a clean DataFrame."""
        ...
