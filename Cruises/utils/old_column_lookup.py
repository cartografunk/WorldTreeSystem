# utils/column_mapper.py

COLUMN_LOOKUP= {
    "Stand #": ["Stand #", "# Posición", "_posicion_"],
    "Plot #": ["Plot #", "# Parcela", "_parcela", "plot"],
    "Plot Coordinate": ["Plot Coordinate", "Coordenadas de la Parcela", "Plot Cooridnate", "Plot Cooridinate", "coordenadas_de_la_parcela"],
    "Tree #": ["Tree #", "# Árbol", "# Arbol", "_arbol", "tree", "tree_#", "arbol", "#_arbol", "tree_number", "_árbol", "árbol", "#_árbol"],
    "Status": ["Status", "Condicion", "condicion"],
    "Species": ["Species", "Especie", "especie"],
    "Defect": ["Defect", "Defecto", "defecto"],
    "Defect HT (ft)": ["Defect HT (ft)", "AT del Defecto (m)", "at_del_defecto_m"],
    "DBH (in)": ["DBH (in)", "DAP (cm)", "dap_cm", "dbh_in"],
    "THT (ft)": ["THT (ft)", "AT (m)", "at_m", "tht_ft"],
    "Merch. HT (ft)": ["Merch. HT (ft)", "Alt. Com. (m)", "alt_com_m", "merch_ht_ft"],
    "Pests": ["Pests", "Plagas", "plagas"],
    "Disease": ["Disease", "Enfermedadas", "enfermedadas"],
    "Coppiced": ["Coppiced", "Poda Basal", "poda_basal"],
    "Permanent Plot": ["Permanent Plot", "Parcela Permanente", "parcela_permanente"],
    "Short Note": ["Short Note", "Nota Breve", "nota_breve"],
    "ContractCode": ["ContractCode", "contractcode", "contract_code"],
    "FarmerName": ["FarmerName", "farmername"],
    "CruiseDate": ["CruiseDate", "cruisedate"],
    "PlantingYear": ["PlantingYear", "year"],
    "TreesContract": ["TreesContract", "TreesSampled"],
    "TreesSampled": ["TreesSampled", "Sampled"],
    "Status": [ "Status", "Condicion", "condicion", "Estado", "estado", "Condición", "condición"],
    "status_id": [ "status_id", "id_status", "id_estado", "ID Estado", "EstadoID", "status_ID"],
    # Mapeo para las columnas derivadas de cat_status
    "DeadTreeValue": [
        "DeadTreeValue", "Valor_Muerto", "valor_muerto",
        "Muerto", "muerto", "Dead_Value"
    ],

    "AliveTree": [
        "AliveTree", "Valor_Vivo", "valor_vivo",
        "Vivo", "vivo", "Alive_Value"
    ]
}

# ──────────────────────────────────────────────────
# utils/column_mapper.py
SQL_COLUMNS = {
    # metadatos
    "contractcode":      "Contract Code",
    "farmername":        "FarmerName",
    "cruisedate":        "CruiseDate",
    "id":                "id",

    # ubicación
    "stand":             "Stand#",
    "plot":              "Plot#",
    "plot_coordinate":   "PlotCoordinate",
    "tree_number":       "Tree#",

    # medidas
    "defect_ht_ft":      "Defect HT(ft)",
    "dbh_in":            "DBH (in)",
    "tht_ft":            "THT (ft)",
    "merch_ht_ft":       "Merch. HT (ft)",

    # libre
    "short_note":        "Short Note",

    # catálogos
    "status_id":           "status_id",
    "species_id":          "cat_species_id",
    "defect_id":           "cat_defect_id",
    "pests_id":            "cat_pest_id",
    "coppiced_id":         "cat_coppiced_id",
    "permanent_plot_id":   "cat_permanent_plot_id",
    "disease_id":          "cat_disease_id",

    # flags y métricas
    "doyle_bf":          "doyle_bf",
    "dead_tree":         "dead_tree",
    "alive_tree":        "alive_tree",
}

