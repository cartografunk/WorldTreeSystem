# __main__.py

from utils.db import get_engine
from utils.libs import pd
from graficadorG1Mortalidad import generar_mortalidad
from graficadorG2Altura import generar_altura
from graficadorG3Crecimiento import generar_crecimiento
from graficadorG4PlagasDefectos import generar_reporte_plagas_defectos

def main():
    engine = get_engine()
    contracts_df = pd.read_sql('SELECT DISTINCT "id_contract" FROM public.cat_cr_inventory2025', engine)

    for code in contracts_df["id_contract"]:
        print(f"\n🟢 Procesando contrato: {code}")
        generar_mortalidad(code)
        generar_altura(code)
        generar_crecimiento(code)
        generar_reporte_plagas_defectos(code)

if __name__ == "__main__":
    main()
