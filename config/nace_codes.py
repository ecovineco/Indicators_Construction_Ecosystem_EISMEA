"""
Central NACE code definitions for the construction ecosystem.
"""

# NACE codes that ALWAYS have weight 1 (core construction sector)
CORE_NACE_CODES = ["C31", "F", "M71", "N81"]

# Horizontal construction activities (variable weights)
HORIZONTAL_NACE_CODES = [
    "C25", "C28", "C33",
    "E36", "E37", "E38", "E39",
    "M69", "M70", "M72",
    "N77", "N78",
]

ALL_NACE_CODES = CORE_NACE_CODES + HORIZONTAL_NACE_CODES

# For datasets at 2-digit level, "F" must be built from sub-codes
F_SUB_CODES = ["F41", "F42", "F43"]

# Codes to request from Eurostat (replacing F with its sub-codes)
API_NACE_CODES = [c for c in ALL_NACE_CODES if c != "F"] + F_SUB_CODES

NACE_LABELS = {
    "C25": "Manufacture of fabricated metal products",
    "C28": "Manufacture of machinery",
    "C31": "Manufacture of furniture",
    "C33": "Repair and installation of machinery and equipment",
    "E36": "Water collection, treatment and supply",
    "E37": "Sewerage",
    "E38": "Waste collection, treatment and disposal; materials recovery",
    "E39": "Remediation activities and other waste management services",
    "F":   "Construction",
    "F41": "Construction of buildings",
    "F42": "Civil engineering",
    "F43": "Specialised construction activities",
    "M69": "Legal and accounting activities",
    "M70": "Activities of head offices and management consulting",
    "M71": "Architectural and engineering activities",
    "M72": "Scientific research and development",
    "N77": "Rental and leasing activities",
    "N78": "Employment activities",
    "N81": "Services to buildings and landscape activities",
}

NACE_CATEGORY = {code: "core" for code in CORE_NACE_CODES}
NACE_CATEGORY.update({code: "horizontal" for code in HORIZONTAL_NACE_CODES})

SEX_LABELS = {"M": "Males", "F": "Females", "T": "Total"}
