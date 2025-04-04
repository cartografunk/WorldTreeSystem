import os
import pandas as pd
from rapidfuzz import fuzz


def fuzzy_group_merge(df, key_column, threshold=90):
    """
    Agrupa filas que son similares en la columna 'key_column' (o en la clave definida)
    y fusiona la información de las columnas, tomando el primer valor no nulo.

    Args:
        df (pd.DataFrame): DataFrame a procesar.
        key_column (str): Nombre de la columna que se utilizará para comparar similitud.
        threshold (int): Umbral de similitud (0 a 100).

    Returns:
        pd.DataFrame: DataFrame con filas fusionadas.
    """
    # Lista para almacenar los índices ya agrupados
    used_indices = set()
    merged_rows = []

    # Iterar sobre cada fila
    for idx, row in df.iterrows():
        if idx in used_indices:
            continue
        # Definir el grupo actual con la fila actual
        group_indices = [idx]
        used_indices.add(idx)
        valor_ref = str(row[key_column])

        # Comparar con las filas siguientes
        for idx2, row2 in df.iloc[idx + 1:].iterrows():
            if idx2 in used_indices:
                continue
            valor_comp = str(row2[key_column])
            similitud = fuzz.ratio(valor_ref, valor_comp)
            if similitud >= threshold:
                group_indices.append(idx2)
                used_indices.add(idx2)

        # Fusionar filas del grupo: para cada columna, tomar el primer valor no nulo encontrado
        merged = {}
        for col in df.columns:
            # Puedes ajustar la lógica: si deseas concatenar valores distintos, aquí podrías unirlos.
            valores = [df.loc[i, col] for i in group_indices if pd.notnull(df.loc[i, col])]
            merged[col] = valores[0] if valores else None
        merged_rows.append(merged)

    return pd.DataFrame(merged_rows)


# --- Parte del script que lee y concatena los DataFrames ---

# Ruta de la carpeta con los archivos Excel
folder_path = r'C:\Users\HeyCe\World Tree Technologies Inc\Forest Inventory - Documentos\Costa Rica\2024_ForestInventoryQ1_25'

dataframes = []  # Almacenará los DataFrames leídos
control_data = []  # Almacenará la información de control de cada hoja

# Recorrer todos los archivos en la carpeta
for filename in os.listdir(folder_path):
    if filename.lower().endswith('.xlsx'):
        file_path = os.path.join(folder_path, filename)
        xl = pd.ExcelFile(file_path)

        # Procesar cada hoja que no se llame 'Summary', 'productores' o 'control_data'
        for sheet in xl.sheet_names:
            if sheet.lower() not in ('summary', 'productores', 'control_data'):
                df = pd.read_excel(file_path, sheet_name=sheet)

                # Registrar la información de control sin modificar nombres de columnas
                control_data.append({
                    'nombre_archivo': filename,
                    'nombre_hoja': sheet,
                    'columnas_detectadas': list(df.columns),
                    'n_filas': df.shape[0]
                })

                # Agregar una columna para identificar el origen
                df['nombre_archivo'] = filename
                dataframes.append(df)

                # Mostrar un head para inspección
                print(f"Archivo: {filename}, Hoja: {sheet}")
                #print(df.head(), "\n")

# Crear el DataFrame de control
control_df = pd.DataFrame(control_data)
print("Tabla de control:")
print(control_df)

# Guardar el DataFrame de control en un archivo Excel
control_file = os.path.join(folder_path, 'control_data.xlsx')
with pd.ExcelWriter(control_file, engine='xlsxwriter') as writer:
    control_df.to_excel(writer, index=False, sheet_name='Control')
print(f"Archivo de control guardado en: {control_file}")

# Concatenar todos los DataFrames sin modificar los nombres de columnas
if dataframes:
    combined_df = pd.concat(dataframes, ignore_index=True)

    # Aquí puedes definir la columna que usarás para comparar. En tu ejemplo,
    # podrías utilizar 'nombre_archivo' o, si deseas comparar sobre varias columnas,
    # crear una nueva columna clave que sea la concatenación de los campos relevantes.
    # Por ejemplo, si quisieras usar "# Posición" y "# Árbol":
    # combined_df["clave"] = combined_df["# Posición"].astype(str) + "_" + combined_df["# Árbol"].astype(str)
    #
    # En este ejemplo usaré "nombre_archivo" para ilustrar el proceso:
    combined_df_merged = fuzzy_group_merge(combined_df, key_column='nombre_archivo', threshold=90)

    # Guardar el DataFrame combinado fusionado en un archivo Excel
    combined_file = os.path.join(folder_path, 'combined_data.xlsx')
    with pd.ExcelWriter(combined_file, engine='xlsxwriter') as writer:
        combined_df_merged.to_excel(writer, index=False, sheet_name='Combined')
    print(f"Dataframe combinado (fusionado) guardado en: {combined_file}")

    print("Vista del DataFrame combinado (después de fusionar filas similares):")
    print(combined_df_merged.head())
else:
    print("No se encontraron archivos Excel en la carpeta.")
