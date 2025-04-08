from utils.libs import pd, os
from utils.db import get_engine


def generar_tabla_sanidad(contract_code: str, output_root: str = "outputs"):
    engine = get_engine()

    output_dir = os.path.join(output_root, contract_code, "Resumen")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"G4_Sanidad_{contract_code}.csv")

    # Obtener total de árboles censados para porcentaje
    query_total = f"""
    SELECT COUNT(*) as total FROM public.cr_inventory_2025
    WHERE "Contract Code" = '{contract_code}'
    """
    total = pd.read_sql(query_total, engine).iloc[0]['total']

    if total == 0:
        print(f"⚠️ Sin árboles censados para contrato {contract_code}.")
        return

    # Queries por grupo
    query_template = """
    SELECT cat.nombre, COUNT(*) as total
    FROM public.cr_inventory_2025 inv
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
        query = query_template.format(catalogo=catalogo, campo=campo, contract_code=contract_code)
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
    else:
        print(f"⚠️ No se encontraron registros de sanidad para {contract_code}.")


if __name__ == "__main__":
    engine = get_engine()
    contracts_df = pd.read_sql('SELECT DISTINCT "id_contract" FROM public.cat_cr_inventory2025', engine)
    for code in contracts_df["id_contract"]:
        generar_tabla_sanidad(code)
