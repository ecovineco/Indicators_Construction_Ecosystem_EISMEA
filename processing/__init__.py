"""
Data processing module.

Each file processes one indicator by:
1. Taking the raw extracted DataFrame
2. Applying weights via the weights module
3. Returning a clean weighted DataFrame

Add new processors here as you build them.
"""

from .employment_by_sex import process_employment_by_sex

# ── Registry: add every new processor here ──────────────────────
# key must match the extraction registry key
PROCESSORS = {
    "employment_by_sex": process_employment_by_sex,
    # "turnover":        process_turnover,
    # "enterprises":     process_enterprises,
    # ...
}
