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

# For datasets that only have 1-letter NACE codes (e.g. lfsa_ewhun2,
# trng_lfs_08b), only "F" (Construction) can be used — it maps directly
# to core code F.  Other letters (C, E, M, N) mix core and horizontal
# sub-codes and cannot be meaningfully decomposed, so they are excluded.
API_NACE_CODES_1LETTER = ["F"]

# Custom NACE codes for rd_e_berdindr2 (R&D expenditure).
# This dataset has some 2-digit codes but groups others:
#   - E codes are only available as combined "E37-E39" (no E36)
#   - F is available as "F" directly (no F41/F42/F43)
#   - M69/M70 not available individually (M69-M71 group exists, but M71 is
#     also available alone — we take M71 only to avoid double-counting)
API_NACE_CODES_RD = [
    "C25", "C28", "C31", "C33",
    "F",
    "M71", "M72",
    "N77", "N78", "N81",
]

# Size class labels for SBS datasets
SIZE_CLASS_LABELS = {
    "TOTAL": "Total",
    "0-9": "0-9 employees",
    "10-19": "10-19 employees",
    "20-49": "20-49 employees",
    "50-249": "50-249 employees",
    "GE250": "250+ employees",
}

SEX_LABELS = {"M": "Males", "F": "Females", "T": "Total"}

# Age labels for lfsa_egan22d (employment by age and sex)
AGE_LABELS = {
    "Y15-24": "15-24",
    "Y25-49": "25-49",
    "Y50-64": "50-64",
    "Y_GE65": "65+",
    "Y_GE15": "Total (15+)",
}

# Age labels for trng_lfs_08b (training participation).
# This dataset uses different age brackets starting at 18, capped at 74.
AGE_LABELS_TRAINING = {
    "Y18-24": "18-24",
    "Y25-54": "25-54",
    "Y55-74": "55-74",
    "Y18-74": "Total (18-74)",
}

# ISCED 2011 education level labels for edat_lfs_9910
ISCED_LABELS = {
    "ED0-2": "Less than upper secondary",
    "ED3_4": "Upper secondary and post-secondary non-tertiary",
    "ED5-8": "Tertiary",
}
