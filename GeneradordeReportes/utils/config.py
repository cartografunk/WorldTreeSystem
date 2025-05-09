# utils/config.py
import os

# Añade esto al archivo existente
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


EXPORT_DPI = 300
EXPORT_FORMAT = "png"
EXPORT_WIDTH_CM = 15.56
EXPORT_WIDTH_INCHES = EXPORT_WIDTH_CM / 2.54  # ≈ 6.13 pulgadas
EXPORT_HEIGHT_CM = 5.15
EXPORT_HEIGHT_INCHES = EXPORT_HEIGHT_CM / 2.54  # ≈ 4.13 pulgadas