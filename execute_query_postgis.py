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

    cur.execute("""
    UPDATE stops
    SET geom = ST_SetSRID(ST_MakePoint(stop_lon, stop_lat), 4326)
    WHERE geom IS NULL;
    """)

    # clave primaria compuesta
    print("Datos de stops")
    cur.execute("""
    SELECT * FROM stops LIMIT 10;
    """)
    rows = cur.fetchall()  # trae todos los resultados
    for row in rows:
        print(row)

    print("Cantidad de datos")
    cur.execute("""
    SELECT COUNT(*) FROM stops
    """)
    rows = cur.fetchall()  # trae todos los resultados
    for row in rows:
        print(row)


    print("Prueba de Gist")
    cur.execute("""
    SELECT stop_name, stop_lat, stop_lon
    FROM stops
    ORDER BY geom <-> ST_SetSRID(ST_MakePoint(-122.418, 37.775), 4326)
    LIMIT 1;
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