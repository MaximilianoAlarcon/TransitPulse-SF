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
ALTER TABLE stop_times ADD COLUMN IF NOT EXISTS departure_sec INT;

UPDATE stop_times SET departure_sec =
    CASE
        WHEN departure_time IS NOT NULL AND departure_time <> ''
        THEN split_part(departure_time, ':', 1)::int * 3600 +
             split_part(departure_time, ':', 2)::int * 60 +
             split_part(departure_time, ':', 3)::int
        ELSE arrival_sec
    END
WHERE departure_sec IS NULL;     
    """)

    select(cur,"""
    SELECT COUNT(*) FROM stop_times WHERE departure_sec IS NULL;
    """)

    print("Query ejecutada")

    print("Iniciando la busqueda")

    query = """
    WITH origin AS (
        SELECT stop_id
        FROM stops
        WHERE ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_Point(%s,%s),4326)::geography,
            %s
        )
        LIMIT 20
    ),
    dest AS (
        SELECT stop_id
        FROM stops
        WHERE ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_Point(%s,%s),4326)::geography,
            %s
        )
        LIMIT 20
    ),
    first_leg AS (
        SELECT *
        FROM stop_times
        WHERE stop_id IN (SELECT stop_id FROM origin)
        ORDER BY arrival_sec
        LIMIT 200
    ),
    transfers AS (
        SELECT 
            st1.trip_id AS trip1,
            st2.trip_id AS trip2,
            st1.stop_id AS transfer_stop,
            st1.arrival_sec AS t1,
            st2.departure_sec AS t2,
            st1.stop_sequence AS seq1,
            st2.stop_sequence AS seq2
        FROM stop_times st1
        JOIN stop_times st2
            ON st1.stop_id = st2.stop_id
        WHERE
            st1.trip_id IN (SELECT trip_id FROM first_leg)
            AND st2.departure_sec > st1.arrival_sec
            AND st2.departure_sec < st1.arrival_sec + 3600
    ),
    final_routes AS (
        SELECT 
            t.trip1,
            t.trip2,
            t.transfer_stop,
            t.t1,
            t.t2,
            st3.arrival_sec AS dest_time,
            st3.stop_id AS dest_stop
        FROM transfers t
        JOIN stop_times st3
            ON t.trip2 = st3.trip_id
        WHERE
            st3.stop_id IN (SELECT stop_id FROM dest)
            AND st3.stop_sequence > t.seq2
    )
    SELECT *,
        (dest_time - t1) AS total_travel_time
    FROM final_routes
    ORDER BY total_travel_time
    LIMIT 20;
    """

    params = (
        -122.4120372,  # lon
        37.7803603,  # lat
        800,               # radio origen (metros)
        -122.4991027,    # lon
        37.5996453,    # lat
        800                # radio destino
    )

    df = pd.read_sql(query, conn, params=params)

    print(df.head())


def run():

    conn = psycopg2.connect(**DB_CONFIG)

    init_db(conn)

    conn.close()