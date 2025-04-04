# utils/plot.py

import matplotlib.pyplot as plt

EXPORT_WIDTH_CM = 15.56
EXPORT_WIDTH_INCHES = EXPORT_WIDTH_CM / 2.54
DEFAULT_HEIGHT_INCHES = 4.5  # puedes ajustar a lo que se vea bien

def crear_figura(ancho=EXPORT_WIDTH_INCHES, alto=DEFAULT_HEIGHT_INCHES):
    return plt.subplots(figsize=(ancho, alto))

def guardar_figura(fig, output_path):
    fig.tight_layout()
    fig.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close(fig)
