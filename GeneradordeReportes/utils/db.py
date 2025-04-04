# GeneradordeReportes/utils/db.py
from sqlalchemy import create_engine

def get_engine():
    return create_engine("postgresql+psycopg2://postgres:pauwlonia@localhost:5432/gisdb")
