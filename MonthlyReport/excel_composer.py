#MonthlyReport/excel_composer.py

from MonthlyReport.tables.t1_etp_summary import build_etp_summary
from MonthlyReport.tables.t2_trees_by_etp_raise import build_etp_trees_table2
from MonthlyReport.tables_process import get_allocation_type
from core.libs import pd
from core.paths import MONTHLY_REPORT_DIR, ensure_all_paths_exist
from core.db import get_engine


def generate_monthly_excel_report():
    engine = get_engine()
    ensure_all_paths_exist()
    output_path = "monthly_report_ALL.xlsx"

    # Tabla 1: TODOS los años, sin filtro
    df1 = build_etp_summary(engine)

    # Tabla 2: TODOS los años y tipos, usando tabla 1 para survival
    df2 = build_etp_trees_table2(engine, df1)

    with pd.ExcelWriter(output_path) as writer:
        df1.to_excel(writer, sheet_name="ETP Summary", index=False)
        df2.to_excel(writer, sheet_name="Trees by ETP", index=False)

    print(f"✅ Reporte generado en {output_path}")


