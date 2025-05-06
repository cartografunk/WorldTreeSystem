# utils/config.py

DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "user": "postgres",
    "password": "pauwlonia",
    "dbname": "gisdb"
}

EXPORT_DPI = 300
EXPORT_FORMAT = "png"
EXPORT_WIDTH_CM = 15.56
EXPORT_WIDTH_INCHES = EXPORT_WIDTH_CM / 2.54  # ≈ 6.13 pulgadas
EXPORT_HEIGHT_CM = 10.5
EXPORT_HEIGHT_INCHES = EXPORT_HEIGHT_CM / 2.54  # ≈ 4.13 pulgadas