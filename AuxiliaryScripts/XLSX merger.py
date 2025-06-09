import pandas as pd
import os

# Ruta de entrada
input_dir = r"D:\OneDrive Local\OneDrive - World Tree Technologies Inc\Operations - Documentos 1\Main Database\SQL exports"
output_file = os.path.join(input_dir, "combined_output.xlsx")

# Archivos Excel en la carpeta
xlsx_files = [f for f in os.listdir(input_dir) if f.endswith('.xlsx')]

# Combinar cada hoja 'Result 1' en un solo archivo con múltiples hojas
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    for file in xlsx_files:
        file_path = os.path.join(input_dir, file)
        sheet_name = os.path.splitext(file)[0][:31]  # Nombre de hoja limitado a 31 caracteres
        try:
            df = pd.read_excel(file_path, sheet_name="Result 1")
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        except Exception as e:
            print(f"⚠️ Error con archivo {file}: {e}")

print(f"✅ Archivo combinado creado: {output_file}")
