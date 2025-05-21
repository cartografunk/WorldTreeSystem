#Cruises/status_para_batch_imports
from core.libs import Path, json

def marcar_lote_completado(json_path: str, tabla_destino: str, dir_sql: str):
    """
    Marca como completado un lote dentro del archivo batch_imports.json.
    """
    path = Path(json_path)
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    actualizados = 0
    for lote in data:
        if lote.get("tabla_destino") == tabla_destino:
            lote["status"] = True
            lote["dir"] = dir_sql
            actualizados += 1

    if actualizados:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"✅ Lote '{tabla_destino}' marcado como completado en {path.name}")
    else:
        print(f"⚠️ No se encontró el lote '{tabla_destino}' para marcarlo.")
