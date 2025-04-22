from sqlalchemy import Text, Float, Numeric, BigInteger

AUDIT_RENAMING = {
    "contractcode":     "Contract Code",
    "contract_code":     "Contract Code",
    "farmername":       "Farmer Name",
    "plantingyear":     "Planting Year",
    "trees_sampled":    "Trees Sampled",
    "total_deads":      "Total Deads",
    "total_alive":      "Total Alive",
    "contracted_trees": "Contracted Trees",
    "sample_size":      "Sample Size",
    "mortality":        "Mortality",
    "survival":         "Survival",
    "remaining_trees":  "Remaining Trees",
}

AUDIT_ORDER = [
    "Contract Code", "Farmer Name", "Planting Year",
    "Trees Sampled", "Contracted Trees",
    "Sample Size",   "Total Deads", "Mortality",
    "Total Alive",   "Survival",    "Remaining Trees",
]

AUDIT_DTYPES = {
    "Contract Code":     Text(),
    "Farmer Name":       Text(),
    "Planting Year":     BigInteger(),
    "Trees Sampled":     Float(),
    "Contracted Trees":  Float(),
    "Sample Size":       Text(),
    "Total Deads":       Float(),
    "Mortality":         Text(),
    "Total Alive":       Float(),
    "Survival":          Text(),
    "Remaining Trees":   Float(),
}

def prepare_audit_for_sql(df):
    df2 = df.rename(columns=AUDIT_RENAMING)
    cols = [c for c in AUDIT_ORDER if c in df2.columns]
    df2  = df2[cols]
    dtype = {c: AUDIT_DTYPES[c] for c in cols if c in AUDIT_DTYPES}
    return df2, dtype
