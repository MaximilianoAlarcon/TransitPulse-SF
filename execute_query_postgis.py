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

    #select(cur,"""
    #SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' ORDER BY table_name, ordinal_position;
    #""")

    cur.execute("""
ALTER TABLE stop_times ADD COLUMN IF NOT EXISTS arrival_sec INT;

UPDATE stop_times SET arrival_sec = split_part(arrival_time, ':', 1)::int * 3600 + split_part(arrival_time, ':', 2)::int * 60 + split_part(arrival_time, ':', 3)::int;

CREATE INDEX IF NOT EXISTS idx_stop_times_stop_arrival ON stop_times(stop_id, arrival_sec);

CREATE INDEX IF NOT EXISTS idx_stop_times_trip_seq ON stop_times(trip_id, stop_sequence);

ALTER TABLE stop_times ADD COLUMN IF NOT EXISTS departure_sec INT;

CREATE INDEX IF NOT EXISTS idx_stops_geom ON stops USING GIST(geom);        
    """)

    print("Query ejecutada")


def run():

    conn = psycopg2.connect(**DB_CONFIG)

    init_db(conn)

    conn.close()