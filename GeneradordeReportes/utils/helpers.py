#GeneradordeReportes/utils/helpers.py
from utils.libs import os, plt

def guardar_figura(path, fig, facecolor='white'):
    if not os.path.exists(path):
        fig.savefig(path, dpi=300, bbox_inches='tight', facecolor=facecolor)
        print(f"✅ Guardado: {path}")
    else:
        print(f"⚠️ Ya existe y no se sobreescribió: {path}")
    plt.close(fig)

# Mapa país → idioma
_REGION_LANG = {
    "CR": "es",
    "GT": "es",
    "MX": "es",
    "US": "en",
}

def get_region_language(country_code: str = "CR") -> str:
    """
    Retorna 'es' o 'en' según el country_code (p. ej. 'CR', 'MX', 'US', 'GT').
    """
    return _REGION_LANG.get(country_code.upper(), "es")