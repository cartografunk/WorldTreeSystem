# MonthlyReport/audit_monthly_report.py
# QA/Audit reproducible para el Monthly Report
from core.libs import pd, np, tqdm, Path
from core.db import get_engine
from core.paths import DATABASE_EXPORTS_DIR
from core.schema_helpers import rename_columns_using_schema
from sqlalchemy import text
import warnings
warnings.filterwarnings("ignore", message="Data Validation extension is not supported and will be removed")

# ======== CONFIG ========
YEAR = 2025                 # año a auditar (ajustable)
TOL_PCT = 0.005             # 0.5% de tolerancia para survival/mortality
TOL_COUNT = 1               # tolerancia en conteos (alive/dead/sampled)
EXPORT_XLSX = Path(DATABASE_EXPORTS_DIR) / "monthly_report_audit.xlsx"

engine = get_engine()

# ======== HELPERS ========
def list_inventory_tables(engine, year:int):
    q = text("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type='BASE TABLE'
          AND table_name ILIKE :pat
        ORDER BY 1,2
    """)
    # ejemplos esperados: inventory_gt_2025, inventory_cr_2025, etc.
    df = pd.read_sql(q, engine, params={"pat": f"inventory_%_{year}"})
    return [(r["table_schema"], r["table_name"]) for _, r in df.iterrows()]

def read_inventory_table(schema, name):
    df = pd.read_sql(f'SELECT * FROM "{schema}"."{name}"', engine)
    df = rename_columns_using_schema(df)  # ← usa COLUMNS/aliases del repo
    return df

def safe_int(x):
    try:
        return int(x)
    except Exception:
        return np.nan

def rollup_from_inventory(inv_df: pd.DataFrame) -> pd.DataFrame:
    # Esperados tras rename: contractcode, alive_tree, dead_tree, id_status, dbh_in, tht_ft, doyle_bf, cruisedate (si existe)
    base_cols = [c for c in ["contractcode","alive_tree","dead_tree","id_status","dbh_in","tht_ft","doyle_bf","cruisedate"] if c in inv_df.columns]
    inv = inv_df[base_cols].copy()

    # Preferencia 1: columnas binarias alive/dead
    if "alive_tree" in inv.columns or "dead_tree" in inv.columns:
        inv["alive_tree"] = inv.get("alive_tree", 0).fillna(0).astype(int)
        inv["dead_tree"]  = inv.get("dead_tree", 0).fillna(0).astype(int)
        grp = inv.groupby("contractcode", dropna=False).agg(
            alive=("alive_tree","sum"),
            dead=("dead_tree","sum"),
            sampled=("alive_tree","count"),
            mean_dbh=("dbh_in","mean"),
            mean_height=("tht_ft","mean"),
            doyle_bf=("doyle_bf","sum")
        ).reset_index()
    else:
        # Fallback: pivote por id_status (sin asumir mapeo específico)
        # Dejamos conteos por status y derivamos 'alive'/'dead' si luego detectamos mapeo
        inv["id_status"] = inv["id_status"].apply(safe_int)
        counts = inv.groupby(["contractcode","id_status"], dropna=False).size().reset_index(name="n")
        pivot = counts.pivot_table(index="contractcode", columns="id_status", values="n", fill_value=0)
        pivot.columns = [f"status_{c}" for c in pivot.columns]
        # Agregados básicos
        agg = inv.groupby("contractcode", dropna=False).agg(
            sampled=("id_status","count"),
            mean_dbh=("dbh_in","mean"),
            mean_height=("tht_ft","mean"),
            doyle_bf=("doyle_bf","sum")
        )
        grp = agg.join(pivot, how="left").reset_index()
        # NOTA: aquí aún no sabemos cuáles status son alive/dead; se resolverá en reconcile()

    return grp

def attach_contracts_info(df):
    # CFI + CTI mínimos para contexto
    q = """
    SELECT cfi.contract_code, cfi.farmer_number, cfi.contract_name, cfi.representative, cfi.status,
           cti.planting_year, cti.trees_contract, cti.planted, cti.species, cti.strain, cti.region
    FROM masterdatabase.contract_farmer_information cfi
    LEFT JOIN masterdatabase.contract_tree_information cti
      ON cti.contract_code = cfi.contract_code
    """
    meta = pd.read_sql(q, engine)
    return df.merge(meta, left_on="contractcode", right_on="contract_code", how="left")

def read_survival_current():
    q = """
    SELECT contract_code, current_surviving_trees, total_deads, total_sampled, current_survival
    FROM masterdatabase.survival_current
    """
    return pd.read_sql(q, engine)

def read_inventory_metrics():
    # opcional: si ya generas mean_dbh/height por contrato/año en inventory_metrics
    try:
        q = "SELECT contract_code, mean_dbh, mean_height FROM inventory_metrics"
        return pd.read_sql(q, engine)
    except Exception:
        return pd.DataFrame(columns=["contract_code","mean_dbh","mean_height"])

def reconcile(rollup_df, surv_df, invm_df):
    df = rollup_df.copy()

    # Si no teníamos alive/dead (pivot por status_), intenta deducir:
    if ("alive" not in df.columns) or ("dead" not in df.columns):
        # Heurística común: considera 'status_1' como vivos y 'status_2' como muertos si existen.
        # Ajusta aquí si tu catálogo difiere; lo importante es centralizar esta regla.
        alive_cols = [c for c in df.columns if c.lower() in ("status_1","status_alive","status_vivo")]
        dead_cols  = [c for c in df.columns if c.lower() in ("status_2","status_dead","status_muerto")]
        df["alive"] = df[alive_cols].sum(axis=1) if alive_cols else np.nan
        df["dead"]  = df[dead_cols].sum(axis=1)  if dead_cols  else np.nan

    # Métricas derivadas canónicas
    df["sampled_calc"] = df[["alive","dead"]].sum(axis=1, min_count=1)
    df["survival_calc"] = (df["alive"] / df["sampled_calc"]).round(4)

    # Join con contracts info
    df = attach_contracts_info(df)

    # Reconciliar contra survival_current
    surv = surv_df.rename(columns={
        "contract_code":"contractcode",
        "surviving_trees":"alive_sc",
        "total_deads":"dead_sc",
        "total_sampled":"sampled_sc",
        "current_survival":"survival_sc"
    })
    df = df.merge(surv, on="contractcode", how="left")

    # Reconciliar contra inventory_metrics (medias)
    invm = invm_df.rename(columns={"contract_code":"contractcode"})
    df = df.merge(invm, on="contractcode", how="left", suffixes=("","_im"))

    # Deltas
    df["diff_alive"]    = df["alive"]   - df["alive_sc"]
    df["diff_dead"]     = df["dead"]    - df["dead_sc"]
    df["diff_sampled"]  = df["sampled_calc"] - df["sampled_sc"]
    df["diff_survival"] = (df["survival_calc"] - df["survival_sc"]).round(4)

    # Bandera PASS/FAIL
    df["qa_pass"] = (
        (df["diff_alive"].abs().fillna(0)   <= TOL_COUNT) &
        (df["diff_dead"].abs().fillna(0)    <= TOL_COUNT) &
        (df["diff_sampled"].abs().fillna(0) <= TOL_COUNT) &
        (df["diff_survival"].abs().fillna(0) <= TOL_PCT)
    )

    return df

def snapshot_sources():
    # Conteos rápidos por tabla clave
    rows = []
    # survival_current
    rows.append(("masterdatabase.survival_current", pd.read_sql("SELECT COUNT(*) AS n FROM masterdatabase.survival_current", engine).iloc[0]["n"]))
    # inventory_metrics (si existe)
    try:
        rows.append(("inventory_metrics", pd.read_sql("SELECT COUNT(*) AS n FROM inventory_metrics", engine).iloc[0]["n"]))
    except Exception:
        rows.append(("inventory_metrics", 0))
    return pd.DataFrame(rows, columns=["table","n_rows"])

# ======== RUN ========
def main():
    print("🔎 Audit Monthly Report – QA")
    tabs = list_inventory_tables(engine, YEAR)
    if not tabs:
        print(f"⚠️ No se encontraron tablas inventory_%_{YEAR}. Usaré solo survival_current para snapshot.")
    frames = []
    for (sch, name) in tqdm(tabs, desc=f"Cargando inventarios {YEAR}"):
        df = read_inventory_table(sch, name)
        if df.empty:
            continue
        frames.append(df)

    if frames:
        inv_all = pd.concat(frames, ignore_index=True)
    else:
        inv_all = pd.DataFrame()

    # 00 snapshot
    snap = snapshot_sources()

    # 01 rollup desde inventario
    if not inv_all.empty:
        roll = rollup_from_inventory(inv_all)
    else:
        roll = pd.DataFrame(columns=["contractcode","alive","dead","sampled","mean_dbh","mean_height","doyle_bf"])

    # 02/03 métricas + 04 reconciliación
    surv = read_survival_current()
    invm = read_inventory_metrics()
    recon = reconcile(roll, surv, invm)

    # 05 discrepancies
    disc = recon[~recon["qa_pass"].fillna(False)].copy()

    # 06 lineage (parametría)
    lineage = pd.DataFrame({
        "param":["YEAR","TOL_PCT","TOL_COUNT"],
        "value":[YEAR,TOL_PCT,TOL_COUNT]
    })

    # Export
    with pd.ExcelWriter(EXPORT_XLSX, engine="xlsxwriter") as xw:
        snap.to_excel(xw, index=False, sheet_name="00_sources_snapshot")
        roll.to_excel(xw, index=False, sheet_name="01_inventory_rollup")
        recon.to_excel(xw, index=False, sheet_name="04_reconciliation")
        disc.to_excel(xw, index=False, sheet_name="05_discrepancies")
        lineage.to_excel(xw, index=False, sheet_name="06_lineage")

    # Resumen consola
    total = len(recon)
    fails = len(disc)
    print(f"✅ QA completado. Contratos auditados: {total} | Fails: {fails}")
    if fails:
        print("⚠️ Hay discrepancias. Revisa 05_discrepancies en el XLSX.")

if __name__ == "__main__":
    main()
