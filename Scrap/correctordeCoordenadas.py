import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from pyproj import Transformer
from sqlalchemy import create_engine

# Conexión a PostgreSQL
db_connection_url = "postgresql://postgres:tu_contraseña@localhost:5432/gisdb"
engine = create_engine(db_connection_url)

# Leer CSV
df = pd.read_csv(r"C:\Users\HeyCe\World Tree Technologies Inc\Forest Inventory - Documentos\GIS\Scrap\cat_inventory_cr_2025_wkt.csv")

# Reemplazar coma por punto y dividir coordenadas
df['PlotCoordinate'] = df['PlotCoordinate'].str.replace(',', '.', regex=False).str.strip()
df[['coord1', 'coord2']] = df['NewCoordinate'].astype(str).str.strip().str.split(' ', expand=True).astype(float)

# Transformadores
transformer_8908_to_4326 = Transformer.from_crs("EPSG:8908", "EPSG:4326", always_xy=True)

# Inicializar listas para nuevas columnas
coords_4326 = []
coords_8908 = []
geoms = []

# Procesar fila por fila
for _, row in df.iterrows():
    x, y = row['coord1'], row['coord2']
    epsg = row.get('EPSG', 4326)  # asume 4326 si no viene el campo

    if epsg == 4326:
        lon, lat = x, y
        coords_4326.append(f"{lon} {lat}")
        coords_8908.append(None)
        geoms.append(Point(lon, lat))

    elif epsg == 8908:
        lon4326, lat4326 = transformer_8908_to_4326.transform(x, y)
        coords_4326.append(f"{lon4326} {lat4326}")
        coords_8908.append(f"{x} {y}")
        geoms.append(Point(lon4326, lat4326))

    else:
        coords_4326.append(None)
        coords_8908.append(None)
        geoms.append(None)

# Agregar columnas
df["PlotCoordinate_4326"] = coords_4326
df["PlotCoordinate_8908"] = coords_8908
df["geometry"] = geoms

# Crear GeoDataFrame con geometría en 4326
gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

# Subir a PostGIS
gdf.to_postgis(name="inventory_cr_2025", con=engine, schema='public', if_exists='replace', index=False)

print("✅ Tabla actualizada en PostgreSQL con geometría en EPSG:4326 y coordenadas limpias.")
