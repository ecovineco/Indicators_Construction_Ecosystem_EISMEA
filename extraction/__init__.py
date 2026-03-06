"""
Data extraction module.

Each file in this package extracts one Eurostat dataset.
Add new extractors here as you build them.
"""

from .employment_by_sex import EmploymentBySexExtractor

# ── Registry: add every new extractor here ──────────────────────
# key = short name used in the rest of the pipeline
EXTRACTORS = {
    "employment_by_sex": EmploymentBySexExtractor,
    # "turnover":        TurnoverExtractor,
    # "enterprises":     EnterprisesExtractor,
    # ...
}
