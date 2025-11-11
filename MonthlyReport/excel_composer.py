# MonthlyReport/excel_composer.py

from core.libs import pd
from datetime import datetime, date
from core.paths import ensure_all_paths_exist
from core.db import get_engine
from core.db_objects import ensure_fpi_expanded_view

from MonthlyReport.utils_monthly_base import build_monthly_base_table
from MonthlyReport.tables_process import apply_aliases
from MonthlyReport.tables.t1_etp_summary import build_etp_summary
from MonthlyReport.tables.t2_trees_by_etp_raise import build_etp_trees_table2
from MonthlyReport.tables.t2a_trees_by_cop_raise import build_cop_trees_table2
from MonthlyReport.tables.t3_trees_by_planting_year import build_t3_trees_by_planting_year
from MonthlyReport.tables.t4_change_by_etp import build_t4_change_by_etp, format_t4_matrix
from MonthlyReport.tables.t5_change_by_planting_year import build_t5_change_by_planting_year, format_t5_matrix

from MonthlyReport.excel_styler import apply_to_template


def generate_monthly_excel_report():
    engine = get_engine()
    ensure_all_paths_exist()
    ensure_fpi_expanded_view(engine)

    # ========= ÚNICA fase de BD =========
    mbt = build_monthly_base_table()

    # === series_obligation a dict ===
    so_df = pd.read_sql(
        "SELECT etp_year, series_obligation FROM masterdatabase.series_obligation",
        engine
    )
    so_df["etp_year"] = pd.to_numeric(so_df["etp_year"], errors="coerce")
    so_df["series_obligation"] = pd.to_numeric(so_df["series_obligation"], errors="coerce")
    so_by_year = (
        so_df.dropna(subset=["etp_year"])
             .drop_duplicates(subset=["etp_year"])
             .set_index("etp_year")["series_obligation"]
             .to_dict()
    )

    # ========= Fechas =========
    today = datetime.today()
    run_month = today.replace(day=1)  # primer día del mes

    # ========= Tablas (todas desde MBT) =========
    df1 = build_etp_summary(mbt)

    # T2 (ETP, con splits 2016 y 2018 ETP>0)
    df2 = build_etp_trees_table2(mbt, so_by_year=so_by_year)

    # T2a (COP, con splits 2015 / 2017-canada_2017_trees / 2016-2018 con COP>0)
    t2a = build_cop_trees_table2(mbt, so_by_year=so_by_year)

    # T3 (planting year) — SIN filtro (como antes)
    t3 = build_t3_trees_by_planting_year(mbt)

    # T4 / T5
    t4_long = build_t4_change_by_etp(
        engine=engine,
        run_month=run_month,
        mbt=mbt,
        so_by_year=so_by_year,
        materialize=True,  # escribe masterdatabase.t4_diff_from_t2
    )
    t4_wide = format_t4_matrix(t4_long, run_month=run_month)

    t5_long = build_t5_change_by_planting_year(
        engine=engine,
        run_month=run_month,
        mbt=mbt,
        materialize=True
    )
    t5_wide = format_t5_matrix(t5_long, run_month=run_month)

    # Aliases para export
    df2_xls = apply_aliases(df2)
    t2a_xls = apply_aliases(t2a)

    # ========= Export con template (preserva estilos/tabla) =========
    DATASETS = {
        "01_monthly_base_table": mbt,  # (opcional) si está en tu template
        "ETP Summary": df1,
        "Trees by ETP (Summary)": df2_xls,
        "Trees by Canadian COP Raise": t2a_xls,
        "Trees by Planting Year": t3,
        "Change of Trees by ETP US Raise": t4_wide,   # 31 chars exactos
        "Change of Trees by Planting Yea": t5_wide,   # truncado a 31
    }

    out_path = apply_to_template(DATASETS, run_date=date.today())
    print("✅ Reporte generado con estilo en:\n")
    print(f'"{out_path}"')


if __name__ == "__main__":
    generate_monthly_excel_report()
