#Cruises/onedriver.py
from core.libs import Path, ctypes

def force_download(path: Path) -> bool:
    """
    Intenta forzar la descarga de un archivo desde OneDrive.
    Devuelve True si el archivo está disponible localmente.
    """
    try:
        FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS = 0x00400000
        attrs = ctypes.windll.kernel32.GetFileAttributesW(str(path))
        if attrs & FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS:
            # Intentamos forzar la descarga
            with open(path, "rb"):
                pass
        return path.exists() and path.stat().st_size > 0
    except Exception as e:
        print(f"⚠️ No se pudo forzar descarga de: {path.name} → {e}")
        return False
