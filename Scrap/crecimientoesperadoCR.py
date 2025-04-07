import pandas as pd

def get_expected_diameter_growth():
    """
    Retorna un DataFrame con los valores mínimos, ideales y máximos
    esperados de crecimiento en diámetro (cm) según la edad de la plantación.
    """
    return pd.DataFrame({
        "Año": [1, 2, 3, 4],
        "Min": [6, 8, 10, 13],
        "Ideal": [10, 9.5, 13, 16],
        "Max": [10, 11, 16, 19]
    })
