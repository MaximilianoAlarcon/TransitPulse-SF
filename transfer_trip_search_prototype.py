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

def find_trip_with_transfer(origin_coords, dest_coords, search_radius=500):

    conn = psycopg2.connect(**DB_CONFIG)

    # --- 1 Buscar paradas cercanas ---
    origin_stops = pd.read_sql("""
        SELECT stop_id, stop_name, stop_lat, stop_lon
        FROM stops
        WHERE ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_Point(%s,%s),4326)::geography,
            %s
        )
    """, conn, params=(origin_coords[0], origin_coords[1], search_radius))

    dest_stops = pd.read_sql("""
        SELECT stop_id, stop_name, stop_lat, stop_lon
        FROM stops
        WHERE ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_Point(%s,%s),4326)::geography,
            %s
        )
    """, conn, params=(dest_coords[0], dest_coords[1], search_radius))

    if origin_stops.empty or dest_stops.empty:
        return {"status":"Not found"}

    origin_ids = tuple(origin_stops.stop_id.tolist())
    dest_ids = tuple(dest_stops.stop_id.tolist())

    # --- 2 Trips que pasan por origen ---
    origin_trips = pd.read_sql("""
        SELECT trip_id, stop_id, stop_sequence, arrival_time
        FROM stop_times
        WHERE stop_id IN %s
    """, conn, params=(origin_ids,))

    if origin_trips.empty:
        return {"status":"Not found"}

    trip_ids = tuple(origin_trips.trip_id.unique())

    # --- 3 Todas las paradas de esos trips ---
    trip_stop_times = pd.read_sql("""
        SELECT trip_id, stop_id, stop_sequence, arrival_time
        FROM stop_times
        WHERE trip_id IN %s
    """, conn, params=(trip_ids,))

    # --- 4 Encontrar posibles transfer stops ---
    transfer_candidates = origin_trips.merge(
        trip_stop_times,
        on="trip_id",
        suffixes=("_origin","_transfer")
    )

    transfer_candidates = transfer_candidates[
        transfer_candidates.stop_sequence_transfer >
        transfer_candidates.stop_sequence_origin
    ]

    if transfer_candidates.empty:
        return {"status":"Not found"}

    transfer_ids = tuple(transfer_candidates.stop_id_transfer.unique())

    # --- 5 Trips que pasan por transfer stops ---
    transfer_trips = pd.read_sql("""
        SELECT trip_id, stop_id, stop_sequence, arrival_time
        FROM stop_times
        WHERE stop_id IN %s
    """, conn, params=(transfer_ids,))

    if transfer_trips.empty:
        return {"status":"Not found"}

    transfer_trip_ids = tuple(transfer_trips.trip_id.unique())

    # --- 6 Traer paradas de esos trips ---
    transfer_trip_stop_times = pd.read_sql("""
        SELECT trip_id, stop_id, stop_sequence, arrival_time
        FROM stop_times
        WHERE trip_id IN %s
    """, conn, params=(transfer_trip_ids,))

    # --- 7 Buscar si llegan al destino ---
    dest_matches = transfer_trip_stop_times[
        transfer_trip_stop_times.stop_id.isin(dest_ids)
    ]

    if dest_matches.empty:
        return {"status":"Not found"}

    # --- 8 Combinar segmentos ---
    segment1 = transfer_candidates.rename(columns={
        "trip_id":"trip1",
        "stop_id_origin":"origin_stop",
        "stop_id_transfer":"transfer_stop",
        "arrival_time_transfer":"transfer_arrival"
    })

    segment2 = transfer_trips.rename(columns={
        "trip_id":"trip2",
        "stop_id":"transfer_stop"
    })

    routes = segment1.merge(segment2, on="transfer_stop")

    routes = routes.merge(
        dest_matches,
        left_on="trip2",
        right_on="trip_id",
        suffixes=("","_dest")
    )

    routes = routes[
        routes.stop_sequence_dest > routes.stop_sequence
    ]

    if routes.empty:
        return {"status":"Not found"}

    return {
        "status":"Found",
        "results": routes.to_dict("records")
    }


def run():
    result = find_trip_with_transfer((-122.4120372,37.7803603),(-122.4785598,37.8199109))
    print(result)