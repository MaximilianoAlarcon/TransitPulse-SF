import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import requests
import zipfile
import io
import pandas as pd
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

def find_trip_with_transfer(origin_coords, dest_coords, search_radius=2000):
    query = f"""
    WITH origin AS (
        SELECT stop_id, stop_name, stop_lat, stop_lon
        FROM stops
        WHERE ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_Point(%s, %s), 4326)::geography,
            %s
        )
    ),
    destination AS (
        SELECT stop_id, stop_name, stop_lat, stop_lon
        FROM stops
        WHERE ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_Point(%s, %s), 4326)::geography,
            %s
        )
    ),
    origin_trips AS (
        SELECT st.trip_id, st.stop_id AS origin_stop_id, st.arrival_time AS origin_arrival, st.stop_sequence AS origin_seq
        FROM stop_times st
        JOIN origin o ON st.stop_id = o.stop_id
    ),
    dest_trips AS (
        SELECT st.trip_id, st.stop_id AS dest_stop_id, st.arrival_time AS dest_arrival, st.stop_sequence AS dest_seq
        FROM stop_times st
        JOIN destination d ON st.stop_id = d.stop_id
    ),
    combined AS (
        SELECT o.trip_id, o.origin_stop_id, o.origin_arrival, o.origin_seq,
               d.dest_stop_id, d.dest_arrival, d.dest_seq
        FROM origin_trips o
        JOIN dest_trips d ON o.trip_id = d.trip_id
        WHERE d.dest_seq > o.origin_seq
    )
    SELECT c.*, 
           o.stop_name AS stop_name_origin, o.stop_lat AS lat_origin, o.stop_lon AS lon_origin,
           d.stop_name AS stop_name_dest, d.stop_lat AS lat_dest, d.stop_lon AS lon_dest,
           r.route_short_name, r.route_long_name, r.route_desc, r.route_type, r.route_url
    FROM combined c
    JOIN origin o ON c.origin_stop_id = o.stop_id
    JOIN destination d ON c.dest_stop_id = d.stop_id
    JOIN trips t ON c.trip_id = t.trip_id
    JOIN routes r ON t.route_id = r.route_id
    ORDER BY c.origin_arrival ASC
    LIMIT 1;
    """

    with psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (
                origin_coords[0], origin_coords[1], search_radius,
                dest_coords[0], dest_coords[1], search_radius
            ))
            result = cur.fetchone()
            if not result:
                return {"status":"Not found","reason":"No direct trips found."}

            # Conversión a timedelta
            now = datetime.now().time()
            origin_time = datetime.strptime(result['origin_arrival'], "%H:%M:%S").time()
            dest_time = datetime.strptime(result['dest_arrival'], "%H:%M:%S").time()

            travel_seconds = (datetime.combine(datetime.today(), dest_time) - 
                              datetime.combine(datetime.today(), origin_time)).total_seconds()
            wait_seconds = (datetime.combine(datetime.today(), origin_time) - 
                            datetime.combine(datetime.today(), now)).total_seconds()
            wait_seconds = max(wait_seconds, 0)

            return {
                "status": "Found",
                "details": {
                    "stop_name_origin": result['stop_name_origin'],
                    "arrival_time_origin": result['origin_arrival'],
                    "stop_name_dest": result['stop_name_dest'],
                    "arrival_time_dest": result['dest_arrival'],
                    "stop_lat_origin": float(result['lat_origin']),
                    "stop_lon_origin": float(result['lon_origin']),
                    "stop_lat_dest": float(result['lat_dest']),
                    "stop_lon_dest": float(result['lon_dest']),
                    "travel_time": int(travel_seconds),
                    "wait_time": int(wait_seconds),
                    "total_time": int(travel_seconds + wait_seconds),
                    "route_short_name": result['route_short_name'],
                    "route_long_name": result['route_long_name'],
                    "route_desc": result['route_desc'],
                    "route_type": int(result['route_type']),
                    "route_url": result['route_url']
                }
            }

def run():
    result = find_trip_with_transfer((-122.4120372,37.7803603),(-122.4785598,37.8199109))
    print(result)