# __main__.py

import argparse
from utils.db   import get_engine
from utils.libs import pd
from report_writer import crear_reporte

def main():
    parser = argparse.ArgumentParser(
        description="Generar reportes .docx para contratos de inventario."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--table", "-t",
        help="Nombre completo de la tabla de detalle (schema.table)."
    )
    group.add_argument(
        "--country", "-c",
        help="Código de país (ej. cr, us) para construir la tabla automáticamente."
    )
    parser.add_argument(
        "--year", "-y",
        default="2025",
        help="Año de inventario (por defecto 2025)."
    )
    args = parser.parse_args()

    engine = get_engine()

    if args.table:
        table_name = args.table
    else:
        # Asume naming: schema.cat_<country>_inventory_<year> para catálogo
        #            : schema.<country>_inventory_<year>    para detalle
        c = args.country.lower()
        table_name = f"public.{c}_inventory_{args.year}"

    # Leer contratos distintos de la tabla de detalle
    sql = f'SELECT DISTINCT "id_contract" FROM {table_name}'
    contracts_df = pd.read_sql(sql, engine)

    for code in contracts_df["id_contract"]:
        print(f"\n🟢 Procesando contrato: {code}")
        crear_reporte(code)

if __name__ == "__main__":
    main()
