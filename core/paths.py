# core/paths.py

from core.libs import Path, os, safe_mkdir

# === Rutas base ===
INVENTORY_BASE = Path(r"D:\OneDrive Local\OneDrive - World Tree Technologies Inc\Forest Inventory - Documentos")

BASE_DIR = Path(__file__).resolve().parent.parent  # WorldTreeSystem/
DATA_DIR = BASE_DIR / "data"
OUTPUTS_DIR = BASE_DIR / "outputs"
TEMP_DIR = BASE_DIR / "tmp"
REPORTS_DIR = BASE_DIR / "reports"

# === Funciones dinámicas ===

def get_contract_output_path(contract_code):
    """Carpeta raíz para un contrato"""
    return OUTPUTS_DIR / contract_code

def get_resumen_path(contract_code):
    """Ruta a la carpeta Resumen de un contrato"""
    return get_contract_output_path(contract_code) / "Resumen"

def get_graph_path(contract_code, graph_key):
    """Ruta para guardar un gráfico PNG con clave tipo G1, G2..."""
    return get_resumen_path(contract_code) / f"{graph_key}_{contract_code}.png"

def ensure_all_paths_exist():
    """Crea las carpetas si no existen"""
    for path in [DATA_DIR, OUTPUTS_DIR, TEMP_DIR, REPORTS_DIR]:
        safe_mkdir(path)
