El módulo de Cruises tiene como objetivo recopilar, estandarizar y montar en una base de datos los archivos de los inventarios que realiza el equipo de operaciones de arbolado para su posterior análisis

Orden de acciones:
1. Identificar los archivos Excel agrupados por país y año, por ejemplo inventario de Costa Rica de 2025: inventory_cr_2025
2. Abrir los archivos (.xlsx)
3. Recopilar la información de las hojas: Input y Summary
4. Unir archivos (.xlsx) con `union.py`
5. Se procede a limpiar la importación
6. Aplicar `filldown` + `clean_column_name` (utils)
7. Normalizar catálogos (`catalog_normalizer`)
8. Validar árboles (`tree_id.py`)
9. Calcular muertos/vivos (`dead_alive_calculator.py`)
10. Guardar en SQL (`inventory_importer`)
