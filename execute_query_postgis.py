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
    CREATE TABLE routes (
        operator_id TEXT,
        route_id TEXT,
        agency_id TEXT,
        route_short_name TEXT,
        route_long_name TEXT,
        route_desc TEXT,
        route_type INT,
        route_url TEXT,
        route_color TEXT,
        route_text_color TEXT,
        PRIMARY KEY (operator_id, route_id)
    );
    """)

    cur.execute("""
    CREATE TABLE trips (
        operator_id TEXT,
        trip_id TEXT,
        route_id TEXT,
        service_id TEXT,
        trip_headsign TEXT,
        direction_id INT,
        block_id TEXT,
        shape_id TEXT,
        trip_short_name TEXT,
        bikes_allowed INT,
        wheelchair_accessible INT,
        PRIMARY KEY (operator_id, trip_id)
    );
    """)

    cur.execute("""
    CREATE TABLE stop_times (
        operator_id TEXT,
        trip_id TEXT,
        arrival_time TEXT,
        departure_time TEXT,
        stop_id TEXT,
        stop_sequence INT,
        stop_headsign TEXT,
        pickup_type INT,
        drop_off_type INT,
        shape_dist_traveled FLOAT,
        timepoint INT,
        PRIMARY KEY (operator_id, trip_id, stop_sequence)
    );
    """)


    print("Cantidad de datos de stops")
    cur.execute("""
    SELECT COUNT(*) FROM stops
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