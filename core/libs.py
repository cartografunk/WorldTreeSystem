# core/libs.py

# ───── Data Analysis ─────
import pandas as pd
import numpy as np

# ───── Plotting ─────
import matplotlib.pyplot as plt
from matplotlib import rcParams
import plotly.express as px
import plotly.graph_objects as go

# ───── System & IO ─────
import os
import glob
import warnings
import unicodedata
from pathlib import Path
from datetime import datetime

# ───── Regex & CLI ─────
import re
import argparse

# ───── SQL ─────
from sqlalchemy import create_engine, inspect, text
import psycopg2
from psycopg2.extras import execute_values

# ───── Excel ─────
import openpyxl
from openpyxl import load_workbook
from openpyxl.utils import range_boundaries

# ───── UX ─────
from tqdm import tqdm


# ───── Global Config ─────
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


# ───── Utilities ─────
def timestamp_now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def safe_mkdir(path):
    Path(path).mkdir(parents=True, exist_ok=True)

def list_excel_files(folder):
    return glob.glob(os.path.join(folder, "*.xls*"))
