# utils/libs.py

import os
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import rcParams
from sqlalchemy import create_engine

# Configuración global de pandas para mostrar datos completos
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)

# Configuración de matplotlib para coherencia en los gráficos
rcParams['figure.dpi'] = 100  # Ajuste de DPI si es necesario


def get_engine():
    """
    Crea y devuelve un engine de SQLAlchemy conectado a la base de datos.
    """
    engine = create_engine(
        "postgresql+psycopg2://postgres:pauwlonia@localhost:5432/helloworldtree"
    )
    print("💻 Conectado a la base de datos helloworldtree")
    return engine
