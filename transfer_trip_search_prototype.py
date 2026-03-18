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

def time_to_seconds(t):
    """Convert GTFS HH:MM:SS to seconds"""
    if pd.isna(t):
        return None
    h, m, s = map(int, t.split(":"))
    return h*3600 + m*60 + s


def find_trip_with_transfer(origin_coords, dest_coords, search_radius=800, nearest_stops=50):

    print("\n========== START TRANSFER SEARCH ==========")
    print("Origin:", origin_coords)
    print("Destination:", dest_coords)

    conn = psycopg2.connect(**DB_CONFIG)

    # ------------------------------------
    # PARAMETERS
    # ------------------------------------

    MAX_TRANSFER_WAIT = 1800   # 30 minutes
    MIN_TRANSFER_WAIT = 60     # 1 minute
    MAX_TRAVEL_TIME = 7200     # 2 hours

    # ------------------------------------
    # STEP 1 — ORIGIN STOPS
    # ------------------------------------

    print("\n[STEP 1] Searching stops near origin")

    origin_stops = pd.read_sql("""
        SELECT stop_id, stop_name, stop_lat, stop_lon
        FROM stops
        WHERE ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_Point(%s,%s),4326)::geography,
            %s
        )
        LIMIT %s
    """, conn, params=(origin_coords[0], origin_coords[1], search_radius, nearest_stops))

    print("Origin stops:", len(origin_stops))

    if origin_stops.empty:
        return {"status": "Not found", "reason": "no_origin_stops"}

    # ------------------------------------
    # STEP 2 — DESTINATION STOPS
    # ------------------------------------

    print("\n[STEP 2] Searching stops near destination")

    dest_stops = pd.read_sql("""
        SELECT stop_id, stop_name, stop_lat, stop_lon
        FROM stops
        WHERE ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_Point(%s,%s),4326)::geography,
            %s
        )
        LIMIT %s
    """, conn, params=(dest_coords[0], dest_coords[1], search_radius, nearest_stops))

    print("Destination stops:", len(dest_stops))

    if dest_stops.empty:
        return {"status": "Not found", "reason": "no_destination_stops"}

    origin_ids = tuple(origin_stops.stop_id.tolist())
    dest_ids = tuple(dest_stops.stop_id.tolist())

    # ------------------------------------
    # STEP 3 — TRIPS FROM ORIGIN
    # ------------------------------------

    print("\n[STEP 3] Trips passing origin stops")

    origin_trips = pd.read_sql("""
        SELECT trip_id, stop_id, stop_sequence, arrival_time
        FROM stop_times
        WHERE stop_id IN %s
    """, conn, params=(origin_ids,))

    print("Trips found:", len(origin_trips))

    if origin_trips.empty:
        return {"status": "Not found", "reason": "no_origin_trips"}

    trip_ids = tuple(origin_trips.trip_id.unique())

    # ------------------------------------
    # STEP 4 — STOPS OF THOSE TRIPS
    # ------------------------------------

    print("\n[STEP 4] Fetching stops for those trips")

    trip_stop_times = pd.read_sql("""
        SELECT trip_id, stop_id, stop_sequence, arrival_time
        FROM stop_times
        WHERE trip_id IN %s
    """, conn, params=(trip_ids,))

    print("Stop times fetched:", len(trip_stop_times))

    # ------------------------------------
    # STEP 5 — POSSIBLE TRANSFER STOPS
    # ------------------------------------

    print("\n[STEP 5] Finding transfer candidates")

    transfer_candidates = origin_trips.merge(
        trip_stop_times,
        on="trip_id",
        suffixes=("_origin", "_transfer")
    )

    transfer_candidates = transfer_candidates[
        transfer_candidates.stop_sequence_transfer >
        transfer_candidates.stop_sequence_origin
    ]

    print("Transfer candidates:", len(transfer_candidates))

    if transfer_candidates.empty:
        return {"status": "Not found", "reason": "no_transfer_candidates"}

    transfer_ids = tuple(transfer_candidates.stop_id_transfer.unique())

    # ------------------------------------
    # STEP 6 — TRIPS FROM TRANSFER STOPS
    # ------------------------------------

    print("\n[STEP 6] Trips from transfer stops")

    transfer_trips = pd.read_sql("""
        SELECT trip_id, stop_id, stop_sequence, arrival_time
        FROM stop_times
        WHERE stop_id IN %s
    """, conn, params=(transfer_ids,))

    transfer_trips = transfer_trips.rename(columns={
        "arrival_time": "transfer_departure"
    })

    print("Second-leg trips:", len(transfer_trips))

    if transfer_trips.empty:
        return {"status": "Not found", "reason": "no_transfer_trips"}

    transfer_trip_ids = tuple(transfer_trips.trip_id.unique())

    # ------------------------------------
    # STEP 7 — STOPS OF SECOND TRIPS
    # ------------------------------------

    print("\n[STEP 7] Fetching stop_times of second trips")

    transfer_trip_stop_times = pd.read_sql("""
        SELECT trip_id, stop_id, stop_sequence, arrival_time
        FROM stop_times
        WHERE trip_id IN %s
    """, conn, params=(transfer_trip_ids,))

    # ------------------------------------
    # STEP 8 — CHECK DESTINATION
    # ------------------------------------

    print("\n[STEP 8] Checking destination matches")

    dest_matches = transfer_trip_stop_times[
        transfer_trip_stop_times.stop_id.isin(dest_ids)
    ].rename(columns={
        "arrival_time": "arrival_time_dest",
        "stop_sequence": "stop_sequence_dest"
    })

    print("Destination matches:", len(dest_matches))

    if dest_matches.empty:
        return {"status": "Not found", "reason": "no_dest_matches"}

    # ------------------------------------
    # STEP 9 — BUILD ROUTES
    # ------------------------------------

    print("\n[STEP 9] Building route combinations")

    segment1 = transfer_candidates.rename(columns={
        "trip_id": "trip1",
        "stop_id_origin": "origin_stop",
        "stop_id_transfer": "transfer_stop",
        "arrival_time_transfer": "transfer_arrival"
    })

    segment2 = transfer_trips.rename(columns={
    "trip_id": "trip2",
    "stop_id": "transfer_stop",
    "arrival_time": "transfer_departure"
    })

    routes = segment1.merge(segment2, on="transfer_stop")

    routes = routes.merge(
        dest_matches,
        left_on="trip2",
        right_on="trip_id",
        suffixes=("", "_dest")
    )

    routes = routes[
        routes.stop_sequence_dest > routes.stop_sequence
    ]

    print("Routes built:", len(routes))

    if routes.empty:
        return {"status": "Not found", "reason": "no_valid_routes"}

    # ------------------------------------
    # STEP 10 — TIME CALCULATIONS
    # ------------------------------------

    print("\n[STEP 10] Calculating travel times")

    routes["origin_time_sec"] = routes["arrival_time_origin"].apply(time_to_seconds)

    routes["transfer_arrival_sec"] = routes["transfer_arrival"].apply(time_to_seconds)

    routes["second_trip_time_sec"] = routes["transfer_departure"].apply(time_to_seconds)

    routes["dest_time_sec"] = routes["arrival_time_dest"].apply(time_to_seconds)

    routes["transfer_wait"] = (
        routes["second_trip_time_sec"] - routes["transfer_arrival_sec"]
    )

    routes["total_travel_time"] = (
        routes["dest_time_sec"] - routes["origin_time_sec"]
    )

    print("Debug antes del filtro")
    print(routes[[
    "arrival_time_origin",
    "transfer_arrival",
    "arrival_time_dest"
    ]].head(10))

    # ------------------------------------
    # STEP 11 — FILTER BAD TRANSFERS
    # ------------------------------------

    print("\n[STEP 11] Filtering unrealistic transfers")

    routes = routes[
        (routes["transfer_wait"] >= MIN_TRANSFER_WAIT) &
        (routes["transfer_wait"] <= MAX_TRANSFER_WAIT)
    ]

    routes = routes[
        routes["total_travel_time"] <= MAX_TRAVEL_TIME
    ]

    print("Routes after filtering:", len(routes))

    if routes.empty:
        return {"status": "Not found", "reason": "no_realistic_routes"}

    # ------------------------------------
    # STEP 12 — REMOVE DUPLICATES
    # ------------------------------------

    routes = routes.drop_duplicates(
        subset=["trip1", "trip2", "transfer_stop"]
    )

    # ------------------------------------
    # STEP 13 — SORT BY BEST TIME
    # ------------------------------------

    routes = routes.sort_values("total_travel_time")

    best_route = routes.iloc[0]

    print("\n🏆 BEST ROUTE")
    print("Trip1:", best_route["trip1"])
    print("Trip2:", best_route["trip2"])
    print("Transfer stop:", best_route["transfer_stop"])
    print("Travel time minutes:", round(best_route["total_travel_time"]/60, 2))

    best_routes = routes.head(5)

    return {
        "status": "Found",
        "best_route": best_route.to_dict(),
        "alternatives": best_routes.to_dict("records"),
        "routes_found": len(routes)
    }


def run():
    result = find_trip_with_transfer((-122.4120372,37.7803603),(-122.4785598,37.8199109))
    print(result)