from GeneradordeReportes.utils.libs import pd, os
from GeneradordeReportes.utils.db import get_engine
from GeneradordeReportes.utils.helpers import get_inventory_table_name

def generar_tabla_sanidad(contract_code: str, country: str, year: int, output_root: str = "outputs"):
    engine = get_engine()

    output_dir = os.path.join(output_root, contract_code, "Resumen")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"G4_Sanidad_{contract_code}.csv")

    table_name = get_inventory_table_name(country, year)

    # Total de árboles censados
    query_total = f"""
    SELECT COUNT(*) as total FROM public.{table_name}
    WHERE "Contract Code" = '{contract_code}'
    """
    total = pd.read_sql(query_total, engine).iloc[0]['total']

    if total == 0:
        print(f"⚠️ Sin árboles censados para contrato {contract_code}.")
        return

    # Template para cada grupo
    query_template = """
    SELECT cat.nombre, COUNT(*) as total
    FROM public.{table_name} inv
    JOIN public.{catalogo} cat ON inv."{campo}" = cat.id
    WHERE inv."Contract Code" = '{contract_code}'
    GROUP BY cat.nombre
    ORDER BY total DESC
    """

    grupos = {
        "Enfermedad": ("cat_disease", "cat_disease_id"),
        "Defecto": ("cat_defect", "cat_defect_id"),
        "Plaga": ("cat_pest", "cat_pest_id"),
    }

    resultados = []

    for grupo, (catalogo, campo) in grupos.items():
        query = query_template.format(
            table_name=table_name,
            catalogo=catalogo,
            campo=campo,
            contract_code=contract_code
        )
        df = pd.read_sql(query, engine)
        if not df.empty:
            df['Grupo'] = grupo
            df['Porcentaje'] = (df['total'] / total * 100).round(2)
            df.rename(columns={'nombre': 'Tipo', 'total': 'Total'}, inplace=True)
            resultados.append(df)

    if resultados:
        tabla = pd.concat(resultados, ignore_index=True)[['Grupo', 'Tipo', 'Total', 'Porcentaje']]
        tabla.to_csv(output_path, index=False)
        print(f"✅ Tabla de sanidad guardada: {output_path}")
        return tabla  # 👈 Esta línea nueva es clave
    else:
        print(f"⚠️ No se encontraron registros de sanidad para {contract_code}.")
        return None
