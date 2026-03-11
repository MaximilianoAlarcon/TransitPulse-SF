import requests
import zipfile
import io
import pandas as pd
import psycopg2
import json
import os

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


def init_db(conn):

    cur = conn.cursor()
    # activar PostGIS



    print("Datos de routes")
    cur.execute("""
    SELECT * FROM routes LIMIT 10
    """)
    rows = cur.fetchall()  # trae todos los resultados
    for row in rows:
        print(row)

    print("Cantidad de datos de routes")
    cur.execute("""
    SELECT COUNT(*) FROM routes
    """)
    rows = cur.fetchall()  # trae todos los resultados
    for row in rows:
        print(row)


    conn.commit()
    cur.close()



def run():

    conn = psycopg2.connect(**DB_CONFIG)

    init_db(conn)

    conn.close()