import os
import pandas as pd
import ast

# Definir rutas de archivos
input_file = r"C:\Users\HeyCe\World Tree Technologies Inc\Forest Inventory - Documentos\Costa Rica\2024_ForestInventoryQ1_25\control_data.xlsx"
output_file = r"C:\Users\HeyCe\World Tree Technologies Inc\Forest Inventory - Documentos\Costa Rica\2024_ForestInventoryQ1_25\control_data_expanded.xlsx"

# Verificar si el archivo de salida ya existe
if os.path.exists(output_file):
    print(f"⚠️ El archivo {output_file} ya existe. No se volverá a procesar.")
else:
    # Cargar el archivo original y verificar las hojas
    xls = pd.ExcelFile(input_file)
    if "columnas_detectadas_uniquevalue" in xls.sheet_names:
        # Si la hoja existe, cargarla directamente
        df = pd.read_excel(input_file, sheet_name="columnas_detectadas_uniquevalue")
    else:
        # Si no existe, reconstruirla desde la columna "columnas_detectadas"
        df_control = pd.read_excel(input_file)  # Cargar la primera hoja
        if "columnas_detectadas" not in df_control.columns:
            raise ValueError("⚠️ No se encontró la columna 'columnas_detectadas' en control_data.xlsx.")

        # Extraer valores únicos y reconstruir la hoja
        unique_values_list = df_control["columnas_detectadas"].dropna().unique()
        df = pd.DataFrame({"columnas_detectadas_uniquevalue": unique_values_list})

    # Convertir la columna "columnas_detectadas_uniquevalue" de texto a listas reales
    df["columnas_detectadas_uniquevalue"] = df["columnas_detectadas_uniquevalue"].apply(
        lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith("[") else []
    )

    # Expandir la columna de listas en columnas separadas
    expanded_cols = df["columnas_detectadas_uniquevalue"].apply(pd.Series)

    # Renombrar columnas como F1, F2, F3...
    num_cols = expanded_cols.shape[1]
    expanded_cols.columns = [f"F{i+1}" for i in range(num_cols)]

    # Unir con el DataFrame original sin la columna de listas
    df_final = df.drop(columns=["columnas_detectadas_uniquevalue"]).join(expanded_cols)

    # Generar registros únicos a partir de las columnas expandidas
    unique_records = pd.DataFrame(expanded_cols.stack().unique(), columns=["Registros_Unicos"])

    # Guardar todo en un nuevo archivo sin modificar control_data.xlsx
    with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
        df_final.to_excel(writer, index=False, sheet_name="Expanded")
        unique_records.to_excel(writer, index=False, sheet_name="Registros_Unicos")
        df.to_excel(writer, index=False, sheet_name="columnas_detectadas_uniquevalue")

    print(f"✅ Archivo procesado y guardado en: {output_file}")
