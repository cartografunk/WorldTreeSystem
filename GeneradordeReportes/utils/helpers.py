import os
import matplotlib.pyplot as plt

def guardar_figura(path, fig, facecolor='white'):
    if not os.path.exists(path):
        fig.savefig(path, dpi=300, bbox_inches='tight', facecolor=facecolor)
        print(f"✅ Guardado: {path}")
    else:
        print(f"⚠️ Ya existe y no se sobreescribió: {path}")
    plt.close(fig)


def get_region_language(country_code: str = "CR") -> str:
    """
    Retorna 'es' para CR, GT, MX; 'en' para US; por defecto 'es'.
    """
    mapping = {
        "CR": "es",
        "GT": "es",
        "MX": "es",
        "US": "en",
    }
    return mapping.get(country_code.upper(), "es")
