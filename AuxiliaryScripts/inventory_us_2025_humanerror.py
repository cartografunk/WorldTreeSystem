

# Acumular errores
errores = []

# Buscar todos los .xlsx recursivamente
xlsx_files = list(BASE_PATH.rglob("*.xlsx"))

for file in tqdm(xlsx_files, desc="📂 Escaneando archivos OneDrive"):
    try:
        wb = openpyxl.load_workbook(file, data_only=True)
        if "Summary" not in wb.sheetnames:
            errores.append((file, "❌ Falta hoja 'Summary'"))
            continue

        sheet = wb["Summary"]
        d3_value = sheet["D3"].value

        # Si está vacío, no es texto, o es el encabezado mal copiado
        if not d3_value or not isinstance(d3_value, str) or d3_value.strip().lower() == "contract code":
            errores.append((file, f"⚠️ Valor inválido en D3: '{d3_value}'"))

    except Exception as e:
        errores.append((file, f"💥 Error al leer archivo: {e}"))

# Reporte final
print("\n📋 Resultados del escaneo:")
for path, motivo in errores:
    print(f" - {path}\n   ↳ {motivo}")