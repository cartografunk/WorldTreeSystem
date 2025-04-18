# utils/libs.py

import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
from matplotlib import rcParams
import plotly.express as px
import plotly.graph_objects as go

from datetime import datetime
import os
import glob
from pathlib import Path
import argparse
import re

import unicodedata
import warnings

from sqlalchemy import text

# Nuevas librer├¡as para IO y DB
import openpyxl
from openpyxl import load_workbook
from openpyxl.utils import range_boundaries

import psycopg2
from psycopg2.extras import execute_values


# Configuraciones globales de pandas y matplotlib
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)

rcParams.update({
    'figure.figsize': (8, 5),
    'axes.titlesize': 'large',
    'axes.labelsize': 'medium',
    'xtick.labelsize': 'small',
    'ytick.labelsize': 'small',
})

# Utilidades generales
def timestamp_now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def safe_mkdir(path):
    Path(path).mkdir(parents=True, exist_ok=True)

def list_excel_files(folder):
    return glob.glob(os.path.join(folder, "*.xls*"))
