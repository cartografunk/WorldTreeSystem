# MonthlyReport/master_table_v3.py
# Tabla maestra (v3) a partir de FUENTES OFICIALES:
#   - masterdatabase.contract_tree_information (CTI)
#   - masterdatabase.survival_current         (SC)
#   - masterdatabase.inventory_metrics_current (IMC)
# Incluye: region (prefijo contrato o core.region), status (CTI),
#          allocation_type (COP/ETP), métricas de supervivencia y medias.

from core.libs import pd, np, Path
from core.db import get_engine
from core.paths import DATABASE_EXPORTS_DIR
import warnings
warnings.filterwarnings("ignore", message="Data Validation extension is not supported and will be removed")

# ========= CONFIG =========
EXPORT_XLSX = Path(DATABASE_EXPORTS_DIR) / "master_table_v3.xlsx"
engine = get_engine()

# ========= Región helper =========
try:
    from core.region import get_prefix as _get_prefix
except Exception:
    _get_prefix = None

def _compute_region_from_code(code: str) -> str:
    if pd.isna(code):
        return None
    code = str(code).strip()
    if _get_prefix:
        try:
            return _get_prefix(code)
        except Exception:
            pass
    return code[:2].upper() if len(code) >= 2 else None

# ========= allocation_type =========
# Si la tienes en MonthlyReport/tables_process.py puedes importarla;
# aquí la dejamos embebida por simplicidad.
def get_allocation_type(etp_year):
    if pd.isna(etp_year):
        return []
    if etp_year in [2015, 2017]:
        return ['COP']
    elif etp_year in [2016, 2018]:
        return ['COP', 'ETP']
    else:
        return ['ETP']

# ========= survival normalizer para IMC =========
def _coerce_survival_column(x):
    """
    Convierte supervivencia de IMC a proporción:
    - '85.4%' -> 0.854
    - '0.854' -> 0.854
    - 85.4    -> 0.854 (si >1 asumimos %)
    - otro/sucio -> NaN
    """
    if pd.isna(x):
        return np.nan
    try:
        if isinstance(x, str):
            s = x.strip()
            if s.endswith('%'):
                return float(s[:-1]) / 100.0
            s = s.replace(',', '.')
            v = float(s)
            return v/100.0 if v > 1 else v
        v = float(x)
        return v/100.0 if v > 1 else v
    except Exception:
        return np.nan

# ========= lectores base =========
def read_cti():
    q = """
    SELECT
        contract_code,
        trees_contract,
        planting_year,
        etp_year,
        status
    FROM masterdatabase.contract_tree_information
    """
    df = pd.read_sql(q, engine)
    for c in ["trees_contract","planting_year","etp_year"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["region"] = df["contract_code"].apply(_compute_region_from_code)
    return df

def read_sc():
    q = """
    SELECT
        contract_code,
        current_surviving_trees
    FROM masterdatabase.survival_current
    """
    df = pd.read_sql(q, engine)
    df["current_surviving_trees"] = pd.to_numeric(df["current_surviving_trees"], errors="coerce")
    return df

def read_imc():
    q = """
    SELECT
        contract_code,
        planting_year,
        dbh_mean,
        tht_mean,
        survival
    FROM masterdatabase.inventory_metrics_current
    """
    df = pd.read_sql(q, engine)
    df["planting_year"] = pd.to_numeric(df["planting_year"], errors="coerce")
    df["dbh_mean"] = pd.to_numeric(df["dbh_mean"], errors="coerce")
    df["tht_mean"] = pd.to_numeric(df["tht_mean"], errors="coerce")
    df["survival_im"] = df["survival"].apply(_coerce_survival_column)
    return df

# ========= builder =========
def build_master_table() -> pd.DataFrame:
    cti = read_cti()
    sc  = read_sc()
    imc = read_imc()

    # Merge maestro: CTI ⟵ SC ⟵ IMC (enlazamos IMC también por planting_year)
    master = cti.merge(sc, on="contract_code", how="left") \
                .merge(imc, on=["contract_code","planting_year"], how="left", suffixes=("","_imc"))

    # Derivados desde absolutos (SC + CTI)
    master["alive_sc"]    = master["current_surviving_trees"]
    master["sampled_sc"]  = master["trees_contract"]
    master["dead_sc"]     = (master["sampled_sc"] - master["alive_sc"]).clip(lower=0)
    master["survival_sc"] = np.where(
        master["sampled_sc"].fillna(0) > 0,
        (master["alive_sc"] / master["sampled_sc"]).round(4),
        np.nan
    )

    # Allocation
    master["allocation_type"]     = master["etp_year"].apply(get_allocation_type)
    master["allocation_type_str"] = master["allocation_type"].apply(lambda xs: "|".join(xs) if isinstance(xs, list) else "")

    # Region consistente (por si IMC no trae)
    if "region" not in master.columns:
        master["region"] = master["contract_code"].apply(_compute_region_from_code)

    # Orden final (sólo los que existan)
    cols = [
        "contract_code","region","status",
        "planting_year","etp_year","allocation_type_str",
        "trees_contract","current_surviving_trees",
        "alive_sc","dead_sc","sampled_sc","survival_sc",
        "dbh_mean","tht_mean","survival_im","survival"
    ]
    return master[[c for c in cols if c in master.columns]]

# ========= export =========
def main():
    print("💻 Conectado a la base de datos helloworldtree")
    print("🧱 Generando master_table_v3 (CTI + SC + IMC)…")

    master = build_master_table()
    print(f"ℹ️ Contratos en master: {master['contract_code'].nunique() if 'contract_code' in master.columns else len(master)}")

    with pd.ExcelWriter(EXPORT_XLSX, engine="xlsxwriter") as xw:
        master.to_excel(xw, index=False, sheet_name="01_master")

    print(f"✅ Listo: {EXPORT_XLSX}")

if __name__ == "__main__":
    main()
