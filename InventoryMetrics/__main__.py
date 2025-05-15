from inventory_metrics.generate import main as generate_metrics
from Cruises.loader import main as load_cruises

if __name__ == "__main__":
    generate_metrics()
    load_cruises()
