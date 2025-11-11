# MonthlyReport/utils_monthly_base.py
# Construye la "monthly base table" (MBT) en memoria a partir de CTI + SC + CA.

from core.libs import pd, np
from core.db import get_engine

engine = get_engine()

# =========================
# Región (solo iniciales)
# =========================
try:
    from core.region import region_from_code as _region_from_code
except Exception:
    _region_from_code = None

_VALID_PREFIXES = {"US", "MX", "CR", "GT"}

def _compute_region_from_code(code) -> str | None:
    if pd.isna(code):
        return None
    s = str(code).strip().upper()
    if _region_from_code:
        try:
            pref = _region_from_code(s)
            return pref if pref in _VALID_PREFIXES else None
        except Exception:
            pass
    if not s:
        return None
    pref = "US" if s.startswith("USA") else s[:2]
    return pref if pref in _VALID_PREFIXES else None


# =========================
# Lectores base
# =========================
def _read_cti():
    q = """
    SELECT contract_code, trees_contract, planted, planting_year, etp_year, status, "Filter"
    FROM masterdatabase.contract_tree_information
    """
    df = pd.read_sql(q, engine)
    num_cols = ["trees_contract","planted","planting_year","etp_year"]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
    df["status"] = df["status"].astype(str).str.strip()
    df["region"] = df["contract_code"].apply(_compute_region_from_code)
    return df

def _read_sc():
    q = """
    SELECT contract_code, current_surviving_trees, survival_metric_source
    FROM masterdatabase.survival_current
    """
    df = pd.read_sql(q, engine)
    df["current_surviving_trees"] = pd.to_numeric(df["current_surviving_trees"], errors="coerce")
    return df

def _read_ca():
    q = """
    SELECT
        contract_code,
        usa_allocation_pct,
        usa_trees_contracted, usa_trees_planted,
        canada_trees_contracted, total_can_allocation,
        canada_2017_trees,
        etp_type,
        contracted_cop, planted_cop,
        contracted_etp, planted_etp,
        surviving_etp, surviving_cop,
        loaded_at
    FROM masterdatabase.contract_allocation
    """
    df = pd.read_sql(q, engine)
    for c in [
        "usa_allocation_pct",
        "usa_trees_contracted","usa_trees_planted",
        "canada_trees_contracted","total_can_allocation",
        "canada_2017_trees",
        "contracted_cop","planted_cop",
        "contracted_etp","planted_etp",
        "surviving_etp","surviving_cop"
    ]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "loaded_at" in df.columns:
        df = (
            df.sort_values("loaded_at")
              .drop_duplicates(subset=["contract_code"], keep="last")
              .reset_index(drop=True)
        )
    return df

# =========================
# Builder principal (EN MEMORIA)
# =========================
def build_monthly_base_table() -> pd.DataFrame:
    """
    Devuelve la Monthly Base Table (MBT) en memoria.
    Claves por contrato: region, status (CTI), planting_year, etp_year, etp_type,
    absolutos SC (alive, dead, sampled, survival_sc), y métricas COP/ETP (CA).
    """
    # Recalcula surviving_etp / surviving_cop en CA antes de leer
    refresh_surviving_split()

    # --- Lecturas base ---
    cti = _read_cti()
    sc  = _read_sc()
    ca  = _read_ca()

    # --- Merge canónico ---
    mbt = (
        cti.merge(sc, on="contract_code", how="left")
           .merge(ca, on="contract_code", how="left")
    )

    # --- Tipos/nulos seguros ---
    for c in ["trees_contract", "planting_year", "etp_year", "current_surviving_trees"]:
        if c in mbt.columns:
            mbt[c] = pd.to_numeric(mbt[c], errors="coerce")

    if "planted" in mbt.columns:
        mbt["planted"] = pd.to_numeric(mbt["planted"], errors="coerce").fillna(0)
    else:
        mbt["planted"] = 0

    # --- Derivados SC globales (por contrato) ---
    mbt["alive_sc"]   = mbt["current_surviving_trees"]
    mbt["sampled_sc"] = mbt["trees_contract"]
    mbt["dead_sc"]    = (mbt["sampled_sc"] - mbt["alive_sc"]).clip(lower=0)
    mbt["survival_sc"] = np.where(
        mbt["sampled_sc"].fillna(0) > 0,
        (mbt["alive_sc"] / mbt["sampled_sc"]).round(4),
        np.nan,
    )

    # --- Campos clave para T1 (¡sin cambiar el año real!) ---
    mbt["etp_year_raw"] = mbt["etp_year"]
    mbt["etp_2017"] = mbt["canada_2017_trees"].fillna(0) > 0  # contratos “purificados” a 2017

    # --- Fill de etp_type (regla histórica) ---
    mbt["etp_type"] = mbt["etp_type"].astype(str).str.strip()
    mask_na = mbt["etp_type"].isin(["", "nan", "<NA>"])
    mbt.loc[mask_na & mbt["etp_year"].isin([2015, 2017]), "etp_type"] = "COP"
    mbt.loc[mask_na & mbt["etp_year"].isin([2016, 2018]), "etp_type"] = "ETP/COP"
    mbt.loc[mask_na & (mbt["etp_year"] >= 2019),           "etp_type"] = "ETP"

    # --- Métricas COP/ETP desde CA (asegurar no nulos) ---
    for c in ["contracted_cop","planted_cop","contracted_etp","planted_etp","surviving_cop","surviving_etp"]:
        if c in mbt.columns:
            mbt[c] = pd.to_numeric(mbt[c], errors="coerce").fillna(0)

    # --- Orden final ---
    cols = [
        "contract_code","region","status","Filter",
        "planting_year","etp_year","etp_year_raw","etp_2017","etp_type",
        "trees_contract","planted",
        "current_surviving_trees","alive_sc","dead_sc","sampled_sc","survival_sc",
        "contracted_cop","planted_cop","contracted_etp","planted_etp",
        "usa_trees_contracted","usa_trees_planted","usa_allocation_pct",
        "canada_trees_contracted","total_can_allocation","canada_2017_trees",
        "surviving_cop","surviving_etp", "survival_metric_source"
    ]
    return mbt[[c for c in cols if c in mbt.columns]]

from sqlalchemy import text as sqltext
from core.db import get_engine

def refresh_surviving_split():
    """
    Recalcula surviving_etp y surviving_cop en contract_allocation
    usando survival_current y usa_allocation_pct.
    """
    engine = get_engine()
    q = sqltext("""
        WITH sc AS (
            SELECT contract_code, current_surviving_trees
            FROM masterdatabase.survival_current
        )
        UPDATE masterdatabase.contract_allocation ca
        SET surviving_etp = CASE
                WHEN etp_type = 'ETP' THEN sc.current_surviving_trees
                WHEN etp_type = 'COP' THEN 0
                WHEN etp_type = 'ETP/COP' THEN CEIL(sc.current_surviving_trees * COALESCE(usa_allocation_pct,0))
                ELSE 0
            END,
            surviving_cop = CASE
                WHEN etp_type = 'ETP' THEN 0
                WHEN etp_type = 'COP' THEN sc.current_surviving_trees
                WHEN etp_type = 'ETP/COP' THEN sc.current_surviving_trees - CEIL(sc.current_surviving_trees * COALESCE(usa_allocation_pct,0))
                ELSE 0
            END
        FROM sc
        WHERE ca.contract_code = sc.contract_code
    """)
    with engine.begin() as conn:
        conn.execute(q)
