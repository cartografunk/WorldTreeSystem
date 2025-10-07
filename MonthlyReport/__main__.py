# MonthlyReport/__main__.py
from core.libs import argparse
from MonthlyReport.excel_composer import generate_monthly_excel_report

# (opcionales) imports de sanidad
try:
    from core.db import get_engine
    from MasterDatabaseManagement.sanidad import preflight
    from MasterDatabaseManagement.sanidad.reporters import export_report
    from datetime import datetime
except Exception:
    preflight = None
    get_engine = None
    export_report = None

def _tag(prefix: str) -> str:
    return f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

def main():
    parser = argparse.ArgumentParser("MonthlyReport")
    parser.add_argument("--sanity", action="store_true",
                        help="Corre checks de sanidad (solo lectura) antes del reporte")
    parser.add_argument("--export-sanity", default=None,
                        help="Carpeta para guardar CSVs de sanidad (e.g., Exports)")
    args = parser.parse_args()

    # Checks previos (no bloqueantes). Si no están disponibles los módulos, se omite.
    if preflight and args.sanity:
        try:
            engine = get_engine()
            results = preflight(engine, job="new_contracts", strict=False)
            if export_report and args.export_sanity:
                path = export_report(results, outdir=args.export_sanity, tag=_tag("monthly"))
                print(f"🧪 Sanidad mensual: resumen en {path}")
        except Exception as e:
            # No bloquea el reporte
            print(f"⚠️ Sanidad previa falló (continuo con el reporte): {e}")

    # Genera el reporte mensual (comportamiento actual)
    generate_monthly_excel_report()

if __name__ == "__main__":
    main()
