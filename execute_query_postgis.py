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

def select(cur, query):
    # Ejecutar la consulta
    cur.execute(query)
    rows = cur.fetchall()
    
    if not rows:
        print("No hay resultados")
        return

    # Obtener nombres de columnas
    col_names = [desc[0] for desc in cur.description]

    # Calcular ancho máximo de cada columna (para alinear)
    col_widths = []
    for i, col in enumerate(col_names):
        max_len = max(len(str(row[i])) for row in rows)
        col_widths.append(max(max_len, len(col)))

    # Construir la línea de encabezados
    header = " | ".join(col.ljust(col_widths[i]) for i, col in enumerate(col_names))
    separator = "-+-".join("-" * col_widths[i] for i in range(len(col_names)))

    # Construir las filas
    data_lines = []
    for row in rows:
        line = " | ".join(str(item).ljust(col_widths[i]) for i, item in enumerate(row))
        data_lines.append(line)

    # Combinar todo en un solo texto
    output = "\n".join([header, separator] + data_lines)
    print(output)

pd.set_option('display.max_columns', None)  # mostrar todas las columnas
pd.set_option('display.width', 200)         # ancho de la tabla en consola
pd.set_option('display.max_rows', 50)      # mostrar hasta 50 filas


def init_db(conn):
    cur = conn.cursor()
    queries = [
    ]
    for q in queries:
        print("Ejecutando:", q.split("\n")[0])
        cur.execute(q)
    conn.commit()

    indexes = [
    ]

    for idx in indexes:
        print(f"Ejecutando: {idx}...")
        cur.execute(idx)
        print("OK")
    
    select(cur, "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'connections' ORDER BY ordinal_position")

    select(cur, "SELECT * FROM connections LIMIT 5")

    print("Query ejecutada")

def run():

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    init_db(conn)

    conn.close()