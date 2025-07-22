#WorldTreeSystem/main.py
import argparse
from CruisesProcessor.importador_batch import correr_batch

def main():
    parser = argparse.ArgumentParser(description="Procesador de importación por lotes")
    parser.add_argument("--country", required=True, help="Código del país (cr, mx, gt, us)")
    parser.add_argument("--year", required=True, type=int, help="Año del inventario")

    args = parser.parse_args()
    correr_batch(args.country.lower(), args.year)

if __name__ == "__main__":
    main()
