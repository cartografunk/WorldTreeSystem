# __main__.py
print("🌎 Hello World Tree!")

import argparse
from GeneradordeReportes.utils.db import get_engine
from GeneradordeReportes.utils.libs import pd
from GeneradordeReportes.report_writer import crear_reporte
from Cruises.utils.schema import COLUMNS
from GeneradordeReportes.utils.db import get_engine

def main():
    print("🌎 Iniciando...")
    parser = argparse.ArgumentParser(
        description="Generar reportes .docx para contratos de inventario."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--country", "-c",
        help="Código de país (cr, gt, mx, us) para construir la tabla automáticamente."
    )
    group.add_argument(
        "--table", "-t",
        help="Nombre completo de la tabla de detalle (schema.table), p.ej. public.inventory_cr_2025"
    )
    parser.add_argument(
        "--year", "-y",
        default="2025",
        help="Año de inventario (por defecto 2025)."
    )
    args = parser.parse_args()

    engine = get_engine()

    # Determinar tabla de detalle
    if args.table:
        detail_table = args.table
    else:
        suffix = f"inventory_{args.country}_{args.year}"
        detail_table = f"public.{suffix}"

    # Leer contratos distintos de la tabla de detalle
    contract_field = next(c["sql_name"] for c in COLUMNS if c["key"] == "contractcode")
    sql = f'SELECT DISTINCT "{contract_field}" FROM {detail_table}'
    contracts_df = pd.read_sql(sql, engine)

    engine = get_engine()
    for code in contracts_df[contract_field]:
        print(f"\n🟢 Procesando contrato: {code}")
        crear_reporte(code, args.country, args.year, engine)


if __name__ == "__main__":
    main()