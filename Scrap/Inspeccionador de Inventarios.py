import os
import pandas as pd

# Ruta de la carpeta con los archivos Excel
folder_path = r'C:\Users\HeyCe\World Tree Technologies Inc\Forest Inventory - Documentos\Costa Rica\2024_ForestInventoryQ1_25'

dataframes = []  # Almacenará los DataFrames leídos
control_data = []  # Almacenará la información de control de cada hoja

# Recorrer todos los archivos en la carpeta
for filename in os.listdir(folder_path):
    if filename.lower().endswith('.xlsx'):
        file_path = os.path.join(folder_path, filename)
        xl = pd.ExcelFile(file_path)

        # Procesar cada hoja que no se llame 'Summary' (sin importar mayúsculas/minúsculas)
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
                print(df.head(), "\n")

# Crear el DataFrame de control
control_df = pd.DataFrame(control_data)
print("Tabla de control:")
print(control_df)
# Ruta para guardar el archivo de control
control_file = os.path.join(folder_path, 'control_data.xlsx')

# Guardar el DataFrame en el archivo Excel
with pd.ExcelWriter(control_file, engine='xlsxwriter') as writer:
    control_df.to_excel(writer, index=False, sheet_name='Control')

print(f"Archivo de control guardado en: {control_file}")

# Concatenar todos los DataFrames sin modificar los nombres de columnas
if dataframes:
    combined_df = pd.concat(dataframes, ignore_index=True)

    # Especificar la ruta y nombre del archivo Excel donde se guardará el DataFrame combinado
    combined_file = os.path.join(folder_path, 'combined_data.xlsx')

    with pd.ExcelWriter(combined_file, engine='xlsxwriter') as writer:
        combined_df.to_excel(writer, index=False, sheet_name='Combined')

    print(f"Dataframe combinado guardado en: {combined_file}")

    print("Vista del DataFrame combinado:")
    print(combined_df.head())
else:
    print("No se encontraron archivos Excel en la carpeta.")