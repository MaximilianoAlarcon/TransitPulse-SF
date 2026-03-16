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

def find_trip_with_transfer(origin_coords, dest_coords, search_radius=2000,nearest_stops=5):

    print("\n========== START TRANSFER SEARCH ==========")

    print("origin_coords:",origin_coords)
    print("dest_coords:",dest_coords)

    conn = psycopg2.connect(**DB_CONFIG)

    # --- 1 Buscar paradas cercanas ---
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

    print("Origin stops found:", len(origin_stops))
    print(origin_stops.head())

    print("\n[STEP 1B] Searching stops near destination")

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

    print("Destination stops found:", len(dest_stops))
    print(dest_stops.head())

    if origin_stops.empty:
        print("❌ FAILURE: No stops near origin")
        return {"status":"Not found","reason":"no_origin_stops"}

    if dest_stops.empty:
        print("❌ FAILURE: No stops near destination")
        return {"status":"Not found","reason":"no_destination_stops"}

    origin_ids = tuple(origin_stops.stop_id.tolist())
    dest_ids = tuple(dest_stops.stop_id.tolist())

    print("Origin stop_ids:", origin_ids[:10])
    print("Destination stop_ids:", dest_ids[:10])

    # --- 2 Trips que pasan por origen ---
    print("\n[STEP 2] Searching trips that pass origin stops")

    origin_trips = pd.read_sql("""
        SELECT trip_id, stop_id, stop_sequence, arrival_time
        FROM stop_times
        WHERE stop_id IN %s
    """, conn, params=(origin_ids,))

    print("Trips found at origin stops:", len(origin_trips))
    print(origin_trips.head())

    if origin_trips.empty:
        print("❌ FAILURE: No trips pass origin stops")
        return {"status":"Not found","reason":"no_origin_trips"}

    trip_ids = tuple(origin_trips.trip_id.unique())

    print("Unique trips from origin:", len(trip_ids))

    # --- 3 Todas las paradas de esos trips ---
    print("\n[STEP 3] Fetching all stops of those trips")

    trip_stop_times = pd.read_sql("""
        SELECT trip_id, stop_id, stop_sequence, arrival_time
        FROM stop_times
        WHERE trip_id IN %s
    """, conn, params=(trip_ids,))

    print("Total stop_times fetched:", len(trip_stop_times))

    # --- 4 Encontrar posibles transfer stops ---
    print("\n[STEP 4] Finding possible transfer stops")

    transfer_candidates = origin_trips.merge(
        trip_stop_times,
        on="trip_id",
        suffixes=("_origin","_transfer")
    )

    print("Candidate pairs before sequence filter:", len(transfer_candidates))

    transfer_candidates = transfer_candidates[
        transfer_candidates.stop_sequence_transfer >
        transfer_candidates.stop_sequence_origin
    ]

    print("Transfer candidates after sequence filter:", len(transfer_candidates))
    print(transfer_candidates.head())

    if transfer_candidates.empty:
        print("❌ FAILURE: No downstream stops after origin")
        return {"status":"Not found","reason":"no_transfer_candidates"}

    transfer_ids = tuple(transfer_candidates.stop_id_transfer.unique())

    print("Unique transfer stops:", len(transfer_ids))
    print("Example transfer stops:", transfer_ids[:10])

    # --- 5 Trips que pasan por transfer stops ---
    print("\n[STEP 5] Searching trips that pass transfer stops")

    transfer_trips = pd.read_sql("""
        SELECT trip_id, stop_id, stop_sequence, arrival_time
        FROM stop_times
        WHERE stop_id IN %s
    """, conn, params=(transfer_ids,))

    print("Trips found at transfer stops:", len(transfer_trips))
    print(transfer_trips.head())

    if transfer_trips.empty:
        print("❌ FAILURE: No trips pass transfer stops")
        return {"status":"Not found","reason":"no_transfer_trips"}

    transfer_trip_ids = tuple(transfer_trips.trip_id.unique())

    print("Unique second-leg trips:", len(transfer_trip_ids))

    # --- 6 Traer paradas de esos trips ---
    print("\n[STEP 6] Fetching stop_times for second-leg trips")

    transfer_trip_stop_times = pd.read_sql("""
        SELECT trip_id, stop_id, stop_sequence, arrival_time
        FROM stop_times
        WHERE trip_id IN %s
    """, conn, params=(transfer_trip_ids,))

    print("Total stop_times for second-leg trips:", len(transfer_trip_stop_times))

    # --- 7 Buscar si llegan al destino ---
    print("\n[STEP 7] Checking if second trips reach destination stops")

    dest_matches = transfer_trip_stop_times[
        transfer_trip_stop_times.stop_id.isin(dest_ids)
    ]

    print("Destination matches found:", len(dest_matches))
    print(dest_matches.head())

    if dest_matches.empty:
        print("❌ FAILURE: No second-leg trips reach destination stops")
        return {"status":"Not found","reason":"no_dest_matches"}

    # --- 8 Combinar segmentos ---
    print("\n[STEP 8] Building route combinations")

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

    print("Routes after first merge:", len(routes))

    routes = routes.merge(
        dest_matches,
        left_on="trip2",
        right_on="trip_id",
        suffixes=("","_dest")
    )

    print("Routes after destination merge:", len(routes))

    routes = routes[
        routes.stop_sequence_dest > routes.stop_sequence
    ]

    print("Routes after sequence validation:", len(routes))

    if routes.empty:
        print("❌ FAILURE: No valid transfer routes after sequence filtering")
        return {"status":"Not found","reason":"sequence_filter_removed_all"}

    print("\n✅ SUCCESS: Transfer routes found:", len(routes))

    print("\n[STEP 9] Calculating best route by total travel time")

    def time_to_seconds(t):
        if pd.isna(t):
            return None
        h, m, s = map(int, t.split(":"))
        return h*3600 + m*60 + s

    # convertir tiempos
    routes["origin_time_sec"] = routes["arrival_time_origin"].apply(time_to_seconds)
    routes["transfer_time_sec"] = routes["transfer_arrival"].apply(time_to_seconds)
    routes["dest_time_sec"] = routes["arrival_time_dest"].apply(time_to_seconds)

    # calcular duración total
    routes["total_travel_time"] = routes["dest_time_sec"] - routes["origin_time_sec"]

    # eliminar valores inválidos
    routes = routes[routes["total_travel_time"] > 0]

    print("Routes after time validation:", len(routes))

    if routes.empty:
        print("❌ FAILURE: No routes with valid travel time")
        return {"status":"Not found","reason":"invalid_times"}

    # ordenar por duración
    routes = routes.sort_values("total_travel_time")

    best_route = routes.iloc[0]

    print("\n🏆 BEST ROUTE FOUND")
    print("Trip 1:", best_route["trip1"])
    print("Trip 2:", best_route["trip2"])
    print("Transfer stop:", best_route["transfer_stop"])
    print("Total travel time (minutes):", round(best_route["total_travel_time"]/60,2))

    # devolver solo las mejores 5 rutas
    best_routes = routes.head(5)

    return {
        "status":"Found",
        "best_route": best_route.to_dict(),
        "alternatives": best_routes.to_dict("records"),
        "total_options_found": len(routes)
    }


def run():
    result = find_trip_with_transfer((-122.4120372,37.7803603),(-122.4785598,37.8199109))
    print(result)