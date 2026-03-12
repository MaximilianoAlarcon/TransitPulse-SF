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

CREATE INDEX IF NOT EXISTS idx_stops_geom
ON stops
USING GIST (geom);


CREATE INDEX IF NOT EXISTS idx_stop_times_stop
ON stop_times (stop_id);


CREATE INDEX IF NOT EXISTS idx_stop_times_trip
ON stop_times (trip_id);


CREATE INDEX IF NOT EXISTS idx_stop_times_trip_sequence
ON stop_times (trip_id, stop_sequence);


CREATE INDEX IF NOT EXISTS idx_trips_trip
ON trips (trip_id);


CREATE INDEX IF NOT EXISTS idx_routes_route
ON routes (route_id);


ANALYZE;
    """)
    
    print("Indices")
    cur.execute("""
    SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'stop_times';
    """)
    rows = cur.fetchall()  # trae todos los resultados
    for row in rows:
        print(row)


    print("Indices")
    cur.execute("""
    SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'stops';
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