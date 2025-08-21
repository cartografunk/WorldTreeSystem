#ReportGenerator/utils/crecimiento_esperado.py

import pandas as pd

df_dbh = pd.DataFrame({
    "Año": [1, 2, 3, 4,5,6,7,8,9,10],
    "Min": [6, 8, 10, 13,17,22,26,31,36,40],
    "Ideal": [8, 9.5, 13, 16,20.5,25.5,29.5,34.5,39.5,43.5],
    "Max": [10, 11, 16, 19,24,29,33,38,43,47]
})

df_altura = pd.DataFrame({
    "Año": [1, 2, 3, 4],
    "Min": [5.36,5.99,5.11,5.12],
    "Max": [7.51, 7.21, 8.6, 8.32]
})