# GeneradordeReportes/utils/text_templates.py

text_templates = {
    "title": {
        "es": "Reporte {year}",
        "en": "Report {year}",
    },
    "intro": {
        "es": (
            "Estimado productor:<br><br>"
            "{farmername}<br><br>"
            "Con gusto le compartimos un resumen de los resultados obtenidos en las visitas "
            "técnicas de seguimiento al proyecto, con el fin de informar sobre el estado actual "
            "de los árboles y su desarrollo."
        ),
        "en": (
            "Dear Farmer:<br><br>"
            "{farmername}<br><br>"
            "We are pleased to share with you a summary of the results obtained during the technical "
            "follow-up visits to the project, in order to provide an update on the current condition "
            "of the trees and their development."
        ),
    },
    # Ahora cada plantilla incluye su placeholder
    "cells_right": {
        "farmercode": {
            "es": "Código de productor: {farmercode}",
            "en": "Producer Code: {farmercode}",
        },
        "contractcode": {
            "es": "Contrato: {contractcode}",
            "en": "Contract: {contractcode}",
        },
        "planting_year": {
            "es": "Año de plantación: {planting_year}",
            "en": "Planting Year: {planting_year}",
        },
        "contract_trees": {
            "es": "Árboles contratados: {contract_trees}",
            "en": "Contracted Trees: {contract_trees}",
        },
    },
    "chart_titles":{
        "mortality":{
            "es": "Mortalidad",
            "en": "Mortality",
        },
        "height": {
            "es": "Distribución de Alturas - {code}",
            "en": "Height distribution - {code}",
        }
    },
    # Etiquetas para los gráficos
    "chart_labels": {
        "mortality": {
            "es": ["Muertos", "Vivos"],
            "en": ["Dead", "Alive"],
        },
        "height": {
            "es": ["<2 m", "2–5 m", ">5 m"],  # ejemplo
            "en": ["<2 m", "2–5 m", ">5 m"],
        },
    },
    # Etiquetas de ejes para gráficos
    "chart_axes": {
        "height_x": {
            "es": "Parcela",
            "en": "Plot",
        },
        "height_y": {
            "es": "Altura promedio (ft)",
            "en": "Average height (ft)",
        },
    },

    "chart_series": {  # 👈 Sección nueva
            "height": {
                "es": ["Altura Total", "Altura Comercial"],
                "en": ["Total Height", "Merchantable Height"]
            }
        },

    "section_headers": {
        "G1": {"es": "Sobrevivencia",    "en": "Survival"},
        "G2": {"es": "Altura y medidas", "en": "Height and measures"},
        "G3": {"es": "Crecimiento",      "en": "Growth"},
    },
    "mortality_text": {
        "es": "Esto significa que, por cada 100 árboles sembrados, hay {dead_per_100} árboles muertos, quedando remanentes en la totalidad del proyecto {alive} árboles vivos.",
        "en": "It was found that, for every 100 trees planted, there are {dead_per_100} dead trees, leaving a total of {alive} live trees in the whole project.",
    },
    
}

