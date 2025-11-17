import sys, os, pathlib
import pyproj

# === 🧭 Fijar rutas de PROJ y GDAL ===
BASE = pathlib.Path(sys.prefix)
PROJ_CONDA = BASE / "Library" / "share" / "proj"
GDAL_CONDA = BASE / "Library" / "share" / "gdal"

os.environ["PROJ_LIB"] = str(PROJ_CONDA)
os.environ["PROJ_DATA"] = str(PROJ_CONDA)
os.environ["GDAL_DATA"] = str(GDAL_CONDA)
os.environ["CPL_CONFIG_OPTIONS"] = f"PROJ_LIB={PROJ_CONDA} GDAL_DATA={GDAL_CONDA}"

print("PROJ_LIB:", os.environ["PROJ_LIB"])
print("GDAL_DATA:", os.environ["GDAL_DATA"])

# === 🧩 Forzar inicialización manual del contexto de PROJ ===
pyproj.datadir.set_data_dir(str(PROJ_CONDA))
print("pyproj data dir →", pyproj.datadir.get_data_dir())

# === ✅ Verificación de que EPSG:4326 ya se puede crear ===
crs_test = pyproj.CRS.from_epsg(4326)
print("CRS cargado correctamente:", crs_test)