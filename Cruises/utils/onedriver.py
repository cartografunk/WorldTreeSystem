#Cruises/OneDriver
from core.libs import Path, ctypes, time

def force_open_excel(path):
    try:
        import win32com.client
    except ImportError:
        return False  # No Excel automation available

    try:
        excel = win32com.client.Dispatch("Excel.Application")
        excel.Visible = False
        wb = excel.Workbooks.Open(str(path))
        time.sleep(1)  # Esperar a que se descargue bien
        wb.Close(SaveChanges=False)
        excel.Quit()
        return True
    except Exception:
        return False


def force_download(path: Path) -> bool:
    """
    Intenta forzar la descarga de un archivo desde OneDrive.
    Devuelve True si el archivo está disponible localmente.
    """
    try:
        FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS = 0x00400000
        attrs = ctypes.windll.kernel32.GetFileAttributesW(str(path))
        if attrs & FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS:
            # Intentamos leer para forzar la descarga
            with open(path, "rb"):
                pass

        if path.exists() and path.stat().st_size > 0:
            return True

        # Último recurso: abrir con Excel si todavía no está local
        print(f"🟡 Intentando forzar descarga con Excel: {path.name}")
        return force_open_excel(path)

    except Exception as e:
        print(f"⚠️ No se pudo forzar descarga de: {path.name} → {e}")
        return False
