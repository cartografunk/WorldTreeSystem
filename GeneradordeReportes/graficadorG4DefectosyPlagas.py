#GeneradordeReportes/graficadorG4DefectosyPlagas

from core.libs import pd, os, np, text
from core.db import get_engine
from core.schema import COLUMNS
from core.schema_helpers import get_column
from core.paths import get_resumen_path
from GeneradordeReportes.utils.helpers import get_inventory_table_name
from GeneradordeReportes.utils.helpers import get_region_language

def generar_tabla_sanidad(contract_code: str, country: str, year: int, engine):
    output_dir = get_resumen_path(contract_code)
    os.makedirs(str(output_dir), exist_ok=True)
    output_path = output_dir / f"G4_Sanidad_{contract_code}.csv"

    table_name = get_inventory_table_name(country, year)

    # Total de árboles censados - Use parameterized query
    query_total = text("""
    SELECT COUNT(*) as total FROM public.{table_name}
    WHERE contractcode = :contract_code
    """.format(table_name=table_name))
    total = pd.read_sql(query_total, engine, params={"contract_code": contract_code}).iloc[0]['total']

    if total == 0:
        print(f"⚠️ Sin árboles censados para contrato {contract_code}.")
        return

    lang = get_region_language(country)
    nombre_field = "nombre_en" if lang == "en" else "nombre"

    # FIXED: Use double quotes for case-sensitive columns
    query_template = """
    SELECT cat.{nombre_field}, COUNT(*) as total
    FROM public.{table_name} inv
    JOIN public.{catalogo} cat ON inv."{campo}"::int = cat.id
    WHERE inv.contractcode = :contract_code
    GROUP BY cat.{nombre_field}
    ORDER BY total DESC
    """

    with engine.connect() as conn:
        ct_sql = text("""
            SELECT trees_contract
            FROM masterdatabase.contract_tree_information
            WHERE contract_code = :code
        """)
        contract_trees = conn.execute(ct_sql, {"code": contract_code}).scalar_one_or_none() or 0

    grupos = {
        "Enfermedad": ("cat_disease", "Disease_id"),
        "Defecto": ("cat_defect", "Defect_id"),
        "Plaga": ("cat_pest", "Pests_id"),
    }

    resultados = []

    for grupo, (catalogo, campo) in grupos.items():
        # Asegúrate que campo sea, por ejemplo, "Defect_id", "Pest_id", "Disease_id"
        query = text(query_template.format(
            table_name=table_name,
            catalogo=catalogo,
            campo=campo,
            nombre_field=nombre_field
        ))
        df = pd.read_sql(query, engine, params={"contract_code": contract_code})
        if not df.empty:
            df['Grupo'] = grupo
            df.rename(columns={nombre_field: 'Tipo', 'total': 'Total'}, inplace=True)
            df['Porcentaje'] = (df['Total'] / total * 100).round(2).astype(str) + '%'
            df['Proyección'] = ((df['Total'] / total) * contract_trees).apply(np.floor).astype(int)
            resultados.append(df)

    if resultados:
        tabla = pd.concat(resultados, ignore_index=True)[['Grupo', 'Tipo', 'Total', 'Porcentaje', 'Proyección']]
        tabla.to_csv(str(output_path), index=False)
        print(f"✅ Tabla de sanidad guardada: {output_path}")
        return tabla
    else:
        print(f"⚠️ No se encontraron registros de sanidad para {contract_code}.")
        return None