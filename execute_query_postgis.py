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


    select(cur,
    """
    ANALYZE stops;
    ANALYZE stop_times;
    ANALYZE routes;
    """
    )

    select(cur,
    """
SELECT state, query
FROM pg_stat_activity
WHERE state = 'active';
    """
    )


    cur.execute("""
WITH params AS (

    SELECT
        ST_SetSRID(ST_Point(37.8199286, -122.4782551), 4326) AS origin_geom,
        ST_SetSRID(ST_Point(37.7803603, -122.4120372), 4326) AS dest_geom,
        500 AS search_radius_m

),

-- paradas cerca del origen
origin_stops AS (

    SELECT s.stop_id, s.stop_name, s.geom
    FROM stops s, params p
    WHERE ST_DWithin(
        s.geom::geography,
        p.origin_geom::geography,
        p.search_radius_m
    )

),

-- paradas cerca del destino
dest_stops AS (

    SELECT s.stop_id, s.stop_name, s.geom
    FROM stops s, params p
    WHERE ST_DWithin(
        s.geom::geography,
        p.dest_geom::geography,
        p.search_radius_m
    )

),

-- viajes que pasan por origen
origin_trips AS (

    SELECT
        st.trip_id,
        st.stop_sequence,
        st.stop_id
    FROM stop_times st
    JOIN origin_stops os
    ON st.stop_id = os.stop_id

),

-- viajes que pasan por destino
dest_trips AS (

    SELECT
        st.trip_id,
        st.stop_sequence,
        st.stop_id
    FROM stop_times st
    JOIN dest_stops ds
    ON st.stop_id = ds.stop_id

)

SELECT DISTINCT ON (ot.trip_id)
    ot.trip_id,
    os.stop_name AS origin_stop,
    ds.stop_name AS destination_stop,
    ot.stop_sequence AS origin_sequence,
    dt.stop_sequence AS destination_sequence

FROM origin_trips ot
JOIN dest_trips dt
    ON ot.trip_id = dt.trip_id
JOIN origin_stops os
    ON os.stop_id = ot.stop_id
JOIN dest_stops ds
    ON ds.stop_id = dt.stop_id

WHERE dt.stop_sequence > ot.stop_sequence
ORDER BY ot.trip_id
LIMIT 20;
    """)
    rows = cur.fetchall()  # trae todos los resultados
    for row in rows:
        print(row)
    
    




def run():

    conn = psycopg2.connect(**DB_CONFIG)

    init_db(conn)

    conn.close()