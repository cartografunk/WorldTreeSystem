import os
import pandas as pd
from tqdm import tqdm
import warnings

warnings.simplefilter("ignore")  # Ignorar warnings de openpyxl

base_path = r"C:\Users\HeyCe\World Tree Technologies Inc\Forest Inventory - Documentos\USA\2024_ForestInventoryQ1_25\WT Cruises"
output_file = os.path.join(base_path, "Combined_Inventory.xlsx")

df_list = []

for root, dirs, files in os.walk(base_path):
    for file in files:
        if file.lower().endswith(".xlsx"):
            file_path = os.path.join(root, file)
            try:
                sheets = pd.read_excel(file_path, sheet_name=None)
                matched_sheet = next((sheets[s] for s in sheets if s.lower() == "input"), None)
                if matched_sheet is not None:
                    # Obtener el nombre del folder directo que contiene el archivo
                    folder_name = os.path.basename(os.path.dirname(file_path))
                    if " - " in folder_name:
                        farmer_name, contract_code = folder_name.split(" - ", 1)
                    else:
                        farmer_name, contract_code = folder_name, "UNKNOWN"
                    matched_sheet.insert(0, "ContractCode", contract_code.strip())
                    matched_sheet.insert(0, "FarmerName", farmer_name.strip())
                    df_list.append(matched_sheet)
            except Exception as e:
                print(f"[ERROR] No se pudo leer: {file_path}\n{e}")

if df_list:
    final_df = pd.concat(df_list, ignore_index=True)
    final_df.to_excel(output_file, index=False)
    print(f"[OK] Archivo combinado guardado en:\n{output_file}")
else:
    print("[AVISO] No se encontraron hojas 'Input' o 'input' en los archivos.")
