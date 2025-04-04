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
