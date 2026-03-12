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

def select(cur,query):
    print("Ejecutando consulta")
    cur.execute(query)
    rows = cur.fetchall()  # trae todos los resultados
    for row in rows:
        print(row)

def init_db(conn):

    cur = conn.cursor()
    # activar PostGIS


    cur.execute(
    """
CREATE INDEX idx_stops_geom ON stops USING gist(geom);
CREATE INDEX idx_stop_times_stop_id ON stop_times(stop_id);
CREATE INDEX idx_stop_times_trip_id_stop_seq ON stop_times(trip_id, stop_sequence);
    """
    )

    select(cur,
    """
WITH params AS (
    SELECT
        ST_SetSRID(ST_Point(-122.4782551, 37.8199286), 4326) AS origin_geom,
        ST_SetSRID(ST_Point(-122.4120372, 37.7803603), 4326) AS dest_geom
),

origin_stops AS (
    SELECT s.stop_id, s.stop_name
    FROM stops s, params p
    WHERE s.geom <-> p.origin_geom < 0.005
),

dest_stops AS (
    SELECT s.stop_id, s.stop_name
    FROM stops s, params p
    WHERE s.geom <-> p.dest_geom < 0.005
),

origin_trips AS (
    SELECT st.trip_id, st.stop_sequence, st.stop_id, os.stop_name AS origin_stop
    FROM stop_times st
    JOIN origin_stops os ON st.stop_id = os.stop_id
),

dest_trips AS (
    SELECT st.trip_id, st.stop_sequence, st.stop_id, ds.stop_name AS dest_stop
    FROM stop_times st
    JOIN dest_stops ds ON st.stop_id = ds.stop_id
)

SELECT ot.trip_id, ot.origin_stop, dt.dest_stop AS destination_stop,
       ot.stop_sequence AS origin_sequence, dt.stop_sequence AS destination_sequence
FROM origin_trips ot
JOIN LATERAL (
    SELECT dt.stop_sequence, dt.dest_stop
    FROM dest_trips dt
    WHERE dt.trip_id = ot.trip_id
      AND dt.stop_sequence > ot.stop_sequence
    ORDER BY dt.stop_sequence
    LIMIT 1
) dt ON true
LIMIT 20;
    """
    )
    




def run():

    conn = psycopg2.connect(**DB_CONFIG)

    init_db(conn)

    conn.close()