#ReportGenerator/__main__.py

import argparse
from core.db import get_engine
from core.libs import pd, sys
from ReportGenerator.report_writer import crear_reporte
from core.schema import COLUMNS
from core.schema_helpers import get_column

def main():
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
    parser.add_argument(
        "--contractcode", "-k",
        help="Contract code específico a procesar (opcional). Si se omite, se generan todos."
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
    contract_field = get_column("contractcode")  # Esto te da 'contractcode'

    if args.contractcode:
        # Solo ese contrato, verifica que exista por si acaso
        sql = f"SELECT DISTINCT {contract_field} FROM {detail_table} WHERE {contract_field} = %(contractcode)s"
        contracts_df = pd.read_sql(sql, engine, params={'contractcode': args.contractcode})
        if contracts_df.empty:
            print(f"❌ No se encontró el contrato: {args.contractcode} en {detail_table}")
            return
    else:
        sql = f"SELECT DISTINCT {contract_field} FROM {detail_table}"
        contracts_df = pd.read_sql(sql, engine)

    for code in contracts_df[contract_field]:
        print(f"\n🟢 Procesando contrato: {code}")
        crear_reporte(code, args.country, args.year, engine)

if __name__ == "__main__":
    main()
