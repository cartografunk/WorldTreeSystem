#MonthlyReport/__main__.py
from core.libs import argparse
from MonthlyReport.excel_composer import generate_monthly_excel_report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=int, required=True)
    parser.add_argument('--pipeline', type=str, default="monthly", help="monthly/otro")
    args = parser.parse_args()
    if args.pipeline == "monthly":
        generate_monthly_excel_report(args.year)
    elif args.pipeline == "otro":
        generar_reporte_otro(args.year)
