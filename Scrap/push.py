import pandas as pd
from core.db import get_engine

# 1. Leer el Excel y el changelog
df_log = pd.read_excel("masterdatabase_export.xlsx", sheet_name="ChangeLog")
fields_catalog = pd.read_excel("masterdatabase_export.xlsx", sheet_name="FieldsCatalog")
engine = get_engine()

# 2. Filtrar solo cambios pendientes
pendientes = df_log[df_log['change_in_db'].isna()]

for idx, row in pendientes.iterrows():
    contract_code = row['contract_code']
    target_field = row['target_field']
    change = row['change']

    # Buscar la tabla en el FieldsCatalog
    tabla = fields_catalog[fields_catalog['field'] == target_field]['table'].values
    if not len(tabla):
        print(f"❌ Campo '{target_field}' no encontrado en FieldsCatalog.")
        continue
    tabla = tabla[0]

    # Chequeo rápido si existe el contract_code
    exists = pd.read_sql(
        f"SELECT 1 FROM masterdatabase.\"{tabla}\" WHERE contract_code = %s LIMIT 1",
        engine, params=[contract_code]
    )
    if exists.empty:
        print(f"❌ Contract_code '{contract_code}' no encontrado en {tabla}.")
        continue

    # Aplica el cambio en SQL
    query = f"""
        UPDATE masterdatabase."{tabla}"
        SET "{target_field}" = %s
        WHERE contract_code = %s
    """
    with engine.begin() as conn:
        conn.execute(query, (change, contract_code))
        print(f"✅ Cambio aplicado: {tabla}.{target_field} = '{change}' para contract_code '{contract_code}'")

    # Marca el cambio como hecho en el DataFrame
    df_log.at[idx, 'change_in_db'] = "Done"

# 5. Guarda ChangeLog actualizado
with pd.ExcelWriter("masterdatabase_export_UPDATED.xlsx", engine="openpyxl", mode='a', if_sheet_exists='replace') as writer:
    df_log.to_excel(writer, sheet_name="ChangeLog", index=False)
