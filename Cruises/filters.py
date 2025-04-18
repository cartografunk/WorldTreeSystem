# forest_inventory/filters.py
def create_filter_func(allowed_codes):
    """
    Devuelve una función que verifica si un contract_code se encuentra en allowed_codes.
    """
    return lambda code: code in allowed_codes
