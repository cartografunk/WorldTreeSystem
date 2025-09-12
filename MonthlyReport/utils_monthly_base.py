# MonthlyReport/utils_monthly_base.py
# Construye la "monthly base table" (MBT) en memoria a partir de CTI + SC + IMC.

from core.libs import pd, np
from core.db import get_engine

engine = get_engine()

# =========================
# Región (solo iniciales)
# =========================
try:
    # Usamos la función oficial del core si existe
    from core.region import region_from_code as _region_from_code
except Exception:
    _region_from_code = None

_VALID_PREFIXES = {"US", "MX", "CR", "GT"}

def _compute_region_from_code(code) -> str | None:
    """
    Deriva la región a partir del contract_code.
    Regla: tomar los primeros 2 (o 'USA'→'US') y validar contra {US, MX, CR, GT}.
    """
    if pd.isna(code):
        return None
    s = str(code).strip().upper()

    # Preferimos la función del core si está disponible
    if _region_from_code:
        try:
            pref = _region_from_code(s)  # devuelve 'US'|'MX'|'CR'|'GT'|None
            return pref if pref in _VALID_PREFIXES else None
        except Exception:
            pass

    # Fallback mínimo sin core.region
    if not s:
        return None
    pref = "US" if s.startswith("USA") else s[:2]
    return pref if pref in _VALID_PREFIXES else None


# =========================
# Allocation
# =========================
def get_allocation_type(etp_year):
    if pd.isna(etp_year):
        return []
    if etp_year in [2015, 2017]:
        return ["COP"]
    elif etp_year in [2016, 2018]:
        return ["COP", "ETP"]
    else:
        return ["ETP"]


# =========================
# Normalizador de supervivencia (IMC)
# =========================
def _coerce_survival_column(x):
    if pd.isna(x):
        return np.nan
    try:
        if isinstance(x, str):
            s = x.strip()
            if s.endswith("%"):
                return float(s[:-1]) / 100.0
            s = s.replace(",", ".")
            v = float(s)
            return v / 100.0 if v > 1 else v
        v = float(x)
        return v / 100.0 if v > 1 else v
    except Exception:
        return np.nan


# =========================
# Lectores base
# =========================
def _read_cti():
    q = """
    SELECT contract_code, trees_contract, planted, planting_year, etp_year, status
    FROM masterdatabase.contract_tree_information
    """
    df = pd.read_sql(q, engine)

    # Solo convierte a numéricas las que sí lo son
    num_cols = ["trees_contract","planted","planting_year","etp_year"]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")

    # status se queda como texto:
    df["status"] = df["status"].astype(str).str.strip()

    df["region"] = df["contract_code"].apply(_compute_region_from_code)
    return df



def _read_sc():
    q = """
    SELECT contract_code, current_surviving_trees
    FROM masterdatabase.survival_current
    """
    df = pd.read_sql(q, engine)
    df["current_surviving_trees"] = pd.to_numeric(
        df["current_surviving_trees"], errors="coerce"
    )
    return df


def _read_imc():
    q = """
    SELECT contract_code, planting_year, dbh_mean, tht_mean, survival
    FROM masterdatabase.inventory_metrics_current
    """
    df = pd.read_sql(q, engine)
    df["planting_year"] = pd.to_numeric(df["planting_year"], errors="coerce")
    df["dbh_mean"] = pd.to_numeric(df["dbh_mean"], errors="coerce")
    df["tht_mean"] = pd.to_numeric(df["tht_mean"], errors="coerce")
    df["survival_im"] = df["survival"].apply(_coerce_survival_column)
    return df

def _read_ca():
    q = """
    SELECT
        contract_code,
        usa_trees_contracted,
        usa_trees_planted,
        canada_trees_contracted,
        total_can_allocation,
        loaded_at
    FROM masterdatabase.contract_allocation
    """
    df = pd.read_sql(q, engine)

    # Tipos numéricos seguros
    for c in ["usa_trees_contracted","usa_trees_planted","canada_trees_contracted","total_can_allocation"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Si hay múltiples filas por contrato, quédate con la última por loaded_at
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
    Claves por contrato: region, status (CTI), planting_year, etp_year, allocation_type,
    absolutos SC (alive, dead, sampled, survival_sc), métricas IMC, y métricas COP (CA).
    """
    # --- Lecturas base ---
    cti = _read_cti()   # debe traer: contract_code, trees_contract, planted?, planting_year, etp_year, status, region
    sc  = _read_sc()    # contract_code, current_surviving_trees
    imc = _read_imc()   # contract_code, planting_year, dbh_mean, tht_mean, survival → survival_im
    ca  = _read_ca()    # contract_code, etp_year, usa_trees_contracted, usa_trees_planted, canada_trees_contracted, total_can_allocation

    # --- Merge canónico ---
    mbt = (
        cti.merge(sc, on="contract_code", how="left")
        .merge(imc, on=["contract_code", "planting_year"], how="left")
        .merge(ca, on="contract_code", how="left")  # 👈 antes usábamos ["contract_code","etp_year"]
    )

    # --- Tipos/nulos seguros ---
    for c in ["trees_contract", "planting_year", "etp_year", "current_surviving_trees",
              "dbh_mean", "tht_mean"]:
        if c in mbt.columns:
            mbt[c] = pd.to_numeric(mbt[c], errors="coerce")

    # planted puede no existir en CTI → garantiza columna numérica
    if "planted" in mbt.columns:
        mbt["planted"] = pd.to_numeric(mbt["planted"], errors="coerce").fillna(0)
    else:
        mbt["planted"] = 0

    # --- Derivados SC ---
    mbt["alive_sc"]   = mbt["current_surviving_trees"]
    mbt["sampled_sc"] = mbt["trees_contract"]
    mbt["dead_sc"]    = (mbt["sampled_sc"] - mbt["alive_sc"]).clip(lower=0)
    mbt["survival_sc"] = np.where(
        mbt["sampled_sc"].fillna(0) > 0,
        (mbt["alive_sc"] / mbt["sampled_sc"]).round(4),
        np.nan,
    )

    # --- Allocation ---
    # Asegúrate de tener importado: from MonthlyReport.tables_process import get_allocation_type
    mbt["allocation_type"] = mbt["etp_year"].apply(get_allocation_type)
    mbt["allocation_type_str"] = mbt["allocation_type"].apply(
        lambda xs: "|".join(xs) if isinstance(xs, list) else ""
    )

    # --- Métricas COP (desde CA) ---
    # Si las columnas no existen (años sin CA), el .get devolverá None → fillna(0) después
    mbt["contracted_cop"] = (
        mbt[["canada_trees_contracted", "usa_trees_contracted"]].fillna(0).sum(axis=1)
    )
    mbt["planted_cop"] = (
        mbt[["total_can_allocation", "usa_trees_planted"]].fillna(0).sum(axis=1)
    )
    mbt["has_cop"] = (
        mbt[["canada_trees_contracted", "usa_trees_contracted", "total_can_allocation", "usa_trees_planted"]]
        .notna().any(axis=1)
    )

    # mbt_check = mbt[["contract_code", "etp_year", "region",
    #                  "trees_contract", "planted", "alive_sc",
    #                  "contracted_cop", "planted_cop", "has_cop"]].head(50)
    # print(mbt_check.to_string())

    # --- Orden sugerido (solo columnas presentes) ---
    cols = [
        "contract_code", "region", "status",
        "planting_year", "etp_year", "allocation_type_str",
        # CTI (ETP)
        "trees_contract", "planted",
        # SC
        "current_surviving_trees", "alive_sc", "dead_sc", "sampled_sc", "survival_sc",
        # IMC
        "dbh_mean", "tht_mean", "survival_im", "survival",
        # CA (COP)
        "usa_trees_contracted", "usa_trees_planted",
        "canada_trees_contracted", "total_can_allocation",
        "contracted_cop", "planted_cop", "has_cop",
    ]
    return mbt[[c for c in cols if c in mbt.columns]]

