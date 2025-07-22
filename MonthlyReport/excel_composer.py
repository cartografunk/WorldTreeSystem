#MonthlyReport/excel_composer.py

from core.libs import pd, openpyxl, Path
from core.paths import MONTHLY_REPORT_DIR, ensure_all_paths_exist
from MonthlyReport.tables.t1_etp_summary import build_etp_summary
from MonthlyReport.tables_process import get_allocation_type

def generate_monthly_excel_report(year):
    ensure_all_paths_exist()
    output_path = MONTHLY_REPORT_DIR / f"monthly_report_{year}.xlsx"
    print(f"Guardando en: {output_path} ({type(output_path)})")
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Genera todas las hojas necesarias según allocation_type
        for allocation_type in get_allocation_type(year):
            df = build_etp_summary(year, allocation_type)
            sheet_name = f"ETP Summary {allocation_type}" if allocation_type else "ETP Summary"
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        # Si agregas más tablas, igual puedes hacer un loop aquí
    print(f"✅ Reporte generado en {output_path}")

if __name__ == "__main__":
    generate_monthly_excel_report()
