import requests
import zipfile
import io
import pandas as pd
import psycopg2
import json
import os
from datetime import datetime

API_KEY = os.environ.get("API_511_KEY")

OPERATORS_URL = "http://api.511.org/transit/gtfsoperators"
DATAFEED_URL = "http://api.511.org/transit/datafeeds"

DB_CONFIG = {
    "host": os.environ.get("DB_HOST"),
    "database": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "port": os.environ.get("DB_PORT")
}

def select(cur,query):
    print("Ejecutando consulta")
    cur.execute(query)
    rows = cur.fetchall()  # trae todos los resultados
    for row in rows:
        print(row)

pd.set_option('display.max_columns', None)  # mostrar todas las columnas
pd.set_option('display.width', 200)         # ancho de la tabla en consola
pd.set_option('display.max_rows', 50)      # mostrar hasta 50 filas


def init_db(conn):
    cur = conn.cursor()
    queries = [
        """
        CREATE TABLE IF NOT EXISTS shapes (
            operator_id TEXT NOT NULL,
            shape_id TEXT NOT NULL,
            shape_pt_sequence INTEGER NOT NULL,
            shape_pt_lat DOUBLE PRECISION NOT NULL,
            shape_pt_lon DOUBLE PRECISION NOT NULL,
            shape_dist_traveled DOUBLE PRECISION
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_shapes_op_shape
        ON shapes(operator_id, shape_id);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_shapes_op_shape_seq
        ON shapes(operator_id, shape_id, shape_pt_sequence);
        """
    ]
    for q in queries:
        print("Ejecutando:", q.split("\n")[0])
        cur.execute(q)
    conn.commit()
    print("Query ejecutada")

def run():

    conn = psycopg2.connect(**DB_CONFIG)

    init_db(conn)

    conn.close()