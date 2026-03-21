import requests
import zipfile
import io
import pandas as pd
import psycopg2
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from utils import get_direct_trip_geometry,estimate_radius,should_use_transit,time_to_seconds
import math

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

pd.set_option('display.max_columns', None)  # mostrar todas las columnas
pd.set_option('display.width', 200)         # ancho de la tabla en consola
pd.set_option('display.max_rows', 50)      # mostrar hasta 50 filas

WAIT_TRANSPORT_LIMIT = 3600

def find_direct_trip(origin_coords, dest_coords, search_radius_origin=800, search_radius_dest=800, auto_estimate_radius=False):

    if should_use_transit(origin_coords,dest_coords):

        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        if auto_estimate_radius:
            search_radius_origin =  estimate_radius(conn,origin_coords)
            search_radius_dest =  estimate_radius(conn,dest_coords)

        # activar PostGIS

        # --- 1. Buscar paradas cercanas al origen ---
        origin_stops = pd.read_sql(f"""
        SELECT s.stop_lat, s.stop_lon, s.stop_id, s.stop_name
        FROM stops s
        WHERE ST_DWithin(
            s.geom::geography,
            ST_SetSRID(ST_Point({origin_coords[0]}, {origin_coords[1]}), 4326)::geography,
            {search_radius_origin}
        )
        """, conn)

        if origin_stops.empty:
            return {
                "status":"Not found",
                "reason":"There is no stop near the origin"
            }

        # --- 2. Buscar paradas cercanas al destino ---
        dest_stops = pd.read_sql(f"""
        SELECT s.stop_lat, s.stop_lon, s.stop_id, s.stop_name
        FROM stops s
        WHERE ST_DWithin(
            s.geom::geography,
            ST_SetSRID(ST_Point({dest_coords[0]}, {dest_coords[1]}), 4326)::geography,
            {search_radius_dest}
        )
        """, conn)

        if dest_stops.empty:
            return {
                "status":"Not found",
                "reason":"There is no stop near the destination."
            }

        # --- 3. Traer trips que pasan por paradas de origen ---
        origin_ids = tuple(origin_stops['stop_id'].tolist())
        dest_ids = tuple(dest_stops['stop_id'].tolist())

        if len(origin_ids) > 0 and len(dest_ids) > 0:

            now_sf = datetime.now(ZoneInfo("America/Los_Angeles")) 
            current_sec = now_sf.hour*3600 + now_sf.minute*60 + now_sf.second
            arrival_end = current_sec + WAIT_TRANSPORT_LIMIT

            origin_placeholders = ','.join(['%s'] * len(origin_ids))
            dest_placeholders = ','.join(['%s'] * len(dest_ids))

            origin_trips = pd.read_sql(
                f"""
                SELECT st.operator_id, st.trip_id, st.stop_sequence, st.stop_id, st.arrival_time, st.arrival_sec 
                FROM stop_times st 
                WHERE st.stop_id IN ({origin_placeholders}) AND st.arrival_sec IS NOT NULL
                """,
                conn,
                params=(origin_ids)
            )

            # --- 4. Traer trips que pasan por paradas de destino ---

            dest_trips = pd.read_sql(
                f"""
                SELECT st.operator_id, st.trip_id, st.stop_sequence, st.stop_id, st.arrival_time, st.arrival_sec 
                FROM stop_times st 
                WHERE st.stop_id IN ({dest_placeholders}) AND st.arrival_sec IS NOT NULL
                """,
                conn,
                params=(dest_ids)
            )

            # --- Bloque completo para combinar trips y agregar nombres de paradas ---
            # 1. Merge de trips por trip_id
            df = origin_trips.merge(dest_trips, on='trip_id', suffixes=('_origin', '_dest'))

            # 2. Filtrar secuencias válidas (destino después del origen)
            df = df[df['stop_sequence_dest'] > df['stop_sequence_origin']]

            # 3. Renombrar columnas de stops para evitar conflictos al merge
            origin_stops_renamed = origin_stops.rename(columns={
                'operator_id':'operator_id_origin',
                'stop_id': 'stop_id_origin',
                'stop_name': 'stop_name_origin',
                'arrival_time': 'arrival_time_origin',
                'stop_lat': 'stop_lat_origin',
                'stop_lon': 'stop_lon_origin'
            })
            dest_stops_renamed = dest_stops.rename(columns={
                'operator_id':'operator_id_dest',
                'stop_id': 'stop_id_dest',
                'stop_name': 'stop_name_dest',
                'arrival_time': 'arrival_time_dest',
                'stop_lat': 'stop_lat_dest',
                'stop_lon': 'stop_lon_dest'
            })

            # 4. Merge para agregar nombres de paradas
            df = df.merge(origin_stops_renamed, on='stop_id_origin')
            df = df.merge(dest_stops_renamed, on='stop_id_dest')

            # 5. Selección de columnas finales
            df_final = df[['trip_id', 'stop_name_origin', 'arrival_time_origin', 'stop_name_dest', 
            'arrival_time_dest', 'stop_sequence_origin', 'stop_sequence_dest', 'operator_id_origin', 
            'operator_id_dest','stop_lat_origin','stop_lon_origin','stop_lat_dest','stop_lon_dest']]
            df_final = df_final.drop_duplicates(subset=['trip_id', 'stop_sequence_origin', 'stop_sequence_dest'])

            if df_final.shape[0] > 0:
                # Hora actual como timedelta
                
                #current_time = datetime.now().strftime("%H:%M:%S")
                #now = pd.to_timedelta(current_time)
                
                now_sf = datetime.now(ZoneInfo("America/Los_Angeles"))
                now = pd.to_timedelta(now_sf.hour, unit='h') + pd.to_timedelta(now_sf.minute, unit='m') + pd.to_timedelta(now_sf.second, unit='s')
            
                # Convertir arrival_time a timedelta
                df_final['arrival_time_origin'] = pd.to_timedelta(df_final['arrival_time_origin'])
                df_final['arrival_time_dest'] = pd.to_timedelta(df_final['arrival_time_dest'])
                # Calcular travel_time
                df_final['travel_time'] = df_final['arrival_time_dest'] - df_final['arrival_time_origin']
                # Calcular wait_time y tiempo totalW
                df_final['wait_time'] = df_final['arrival_time_origin'] - now
                df_final = df_final[df_final['wait_time'] >= pd.Timedelta(0)]  # descartar buses que ya pasaron
                df_final['total_time'] = df_final['wait_time'] + df_final['travel_time']
                # Elegir bus que llega primero considerando espera
                df_fastest = df_final.sort_values('total_time').head(1)
                if df_fastest.shape[0] > 0:
                    print("✅ Transporte que lleva al destino más rápido desde ahora:")
                    print(df_fastest.head())
                    trip_details = df_fastest.iloc[0]
                    transport_details = pd.read_sql("SELECT * FROM routes WHERE route_id IN (SELECT route_id FROM trips WHERE trip_id = %s AND operator_id = %s);",conn,params=(df_fastest['trip_id'].iloc[0], df_fastest['operator_id_origin'].iloc[0]))
                    print("Detalles del transporte")
                    print(transport_details.head())
                    print(transport_details.shape)
                    transport_details = transport_details.iloc[0]
                    t1 = trip_details["arrival_time_origin"]
                    t2 = trip_details["arrival_time_dest"]
                    trip_geometry = get_direct_trip_geometry(cur, trip_details, transport_details,search_shapes=True)
                    return {
                        "status":"Found",
                        "details":{
                            "stop_name_origin":trip_details["stop_name_origin"],
                            "arrival_time_origin":f"{t1.components.hours:02}:{t1.components.minutes:02}",
                            "stop_name_dest":trip_details["stop_name_dest"],
                            "arrival_time_dest":f"{t2.components.hours:02}:{t2.components.minutes:02}",
                            "stop_lat_origin":float(trip_details["stop_lat_origin"]),
                            "stop_lon_origin":float(trip_details["stop_lon_origin"]),
                            "stop_lat_dest":float(trip_details["stop_lat_dest"]),
                            "stop_lon_dest":float(trip_details["stop_lon_dest"]),
                            "travel_time":int(trip_details["travel_time"].total_seconds()),
                            "wait_time":int(trip_details["wait_time"].total_seconds()),
                            "total_time":int(trip_details["total_time"].total_seconds()),
                            "route_short_name":transport_details["route_short_name"],
                            "route_long_name":transport_details["route_long_name"],
                            "route_desc":transport_details["route_desc"],
                            "route_type":int(transport_details["route_type"]),
                            "route_url":transport_details["route_url"],
                            "route_color":transport_details["route_color"],
                            "route_type":transport_details["route_type"],
                            "trip_geometry":trip_geometry
                        }
                    }
                else:
                    return {
                        "status":"Not found",
                        "reason":"No direct trips were found between the origin and destination"
                    }
            else:
                return {
                    "status":"Not found",
                    "reason":"No direct trips were found between the origin and destination"
                }
        else:
            return {
                "status":"Not found",
                "reason":"No direct trips were found between the origin and destination"
            }
    else:
        return {
                "status":"Canceled",
                "reason":"You should go walking"
            }



MAX_WAIT_FOR_FIRST_BUS = 7200   # ampliado de 3600 a 7200
MAX_WAIT_FOR_SECOND_BUS = 10800  # ampliado de 7200 a 10800
MAX_TOTAL_TRIP_TIME = 14400      # nuevo: descarta viajes de más de 4 horas


def find_trip_with_transfer(origin_coords,dest_coords,search_radius_origin=800,search_radius_dest=1200,transfer_radius=350,auto_estimate_radius=False):
    """
    Busca rutas con un solo transbordo entre dos coordenadas.
    - origin_coords: (lon, lat)
    - dest_coords: (lon, lat)
    - search_radius_origin: radio para stops de origen (en metros)
    - search_radius_dest: radio para stops de destino (en metros)
    - transfer_radius: radio para considerar transbordo cercano (en metros)
    """

    conn = psycopg2.connect(**DB_CONFIG)

    if auto_estimate_radius:
        search_radius_origin = estimate_radius(conn, origin_coords)
        search_radius_dest = estimate_radius(conn, dest_coords)

    # hora actual en SF
    now_sf = datetime.now(ZoneInfo("America/Los_Angeles"))
    now_text = now_sf.strftime("%d/%m/%Y %H:%M:%S")
    current_sec = now_sf.hour * 3600 + now_sf.minute * 60 + now_sf.second

    query = """
    WITH origin AS (
        SELECT stop_id, geom
        FROM stops
        WHERE ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_Point(%s, %s), 4326)::geography,
            %s
        )
    ),
    dest AS (
        SELECT stop_id, geom
        FROM stops
        WHERE ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_Point(%s, %s), 4326)::geography,
            %s
        )
    ),
    first_leg AS (
        -- OPT 1: se expone departure_sec para calcular wait_for_first_bus correctamente
        -- OPT 2: ventana temporal reactivada con MAX_WAIT_FOR_FIRST_BUS ampliado
        SELECT st.trip_id, st.stop_id, st.stop_sequence,
               st.arrival_sec, st.departure_sec
        FROM stop_times st
        JOIN stops s ON st.stop_id = s.stop_id
        WHERE
            st.stop_id IN (SELECT stop_id FROM origin)
            AND st.arrival_sec IS NOT NULL
            AND st.departure_sec >= %s
            AND st.departure_sec <= %s + """ + str(MAX_WAIT_FOR_FIRST_BUS) + """
            AND ST_Distance(
                s.geom::geography,
                ST_SetSRID(ST_Point(%s, %s), 4326)::geography
            )
            <
            ST_Distance(
                ST_SetSRID(ST_Point(%s, %s), 4326)::geography,
                ST_SetSRID(ST_Point(%s, %s), 4326)::geography
            ) * 1.2
        ORDER BY st.departure_sec
        LIMIT 150
    ),
    transfers AS (
        SELECT
            st1.trip_id       AS trip1,
            st2.trip_id       AS trip2,
            st1.stop_id       AS leg1_stop,
            st2.stop_id       AS leg2_stop,
            -- OPT 3: departure_origin viene de first_leg para wait_for_first_bus correcto
            fl.departure_sec  AS departure_origin,
            st1.arrival_sec   AS t1,
            st2.departure_sec AS t2,
            st1.stop_sequence AS seq1,
            st2.stop_sequence AS seq2,
            st2.arrival_time  AS arrival_time_second_trip
        FROM stop_times st1
        -- OPT 3: join con first_leg para traer departure_sec del stop de origen
        JOIN first_leg fl ON fl.trip_id = st1.trip_id
        JOIN stops s1 ON st1.stop_id = s1.stop_id
        -- OPT 4: primero filtrás stops geográficamente cercanos, luego buscás trips
        -- evita el producto cartesiano de stop_times completo
        JOIN stops s2 ON ST_DWithin(s1.geom::geography, s2.geom::geography, 200)
        JOIN stop_times st2 ON st2.stop_id = s2.stop_id
        WHERE
            st1.trip_id IN (SELECT trip_id FROM first_leg)
            AND st2.departure_sec > st1.arrival_sec
            AND st2.departure_sec < st1.arrival_sec + """ + str(MAX_WAIT_FOR_SECOND_BUS) + """
    ),
    final_routes AS (
        SELECT
            t.trip1,
            t.trip2,
            t.leg1_stop,
            t.leg2_stop,
            t.departure_origin,
            t.t1,
            t.t2,
            t.seq1,
            t.seq2,
            t.arrival_time_second_trip,
            st3.arrival_sec   AS dest_time,
            st3.arrival_time  AS dest_arrival_time,
            st3.stop_id       AS dest_stop,
            st3.stop_sequence AS seq3
        FROM transfers t
        JOIN stop_times st3 ON t.trip2 = st3.trip_id
        WHERE
            st3.stop_id IN (SELECT stop_id FROM dest)
            AND st3.stop_sequence > t.seq2
    )
    SELECT *,
        -- OPT 5: total_travel_time con %% 86400 para manejar viajes que pasan medianoche
        (dest_time - %s) %% 86400        AS total_travel_time,
        -- OPT 3: wait_for_first_bus desde departure del stop de origen (no arrival del transbordo)
        (departure_origin - %s)           AS wait_for_first_bus
    FROM final_routes
    -- OPT 6: filtro de tiempo total razonable centralizado en un solo lugar
    WHERE (dest_time - %s) %% 86400 BETWEEN 0 AND """ + str(MAX_TOTAL_TRIP_TIME) + """
    ORDER BY total_travel_time
    LIMIT 20;
    """

    # Params en orden exacto de los %s (15 total)
    params = (
        origin_coords[0], origin_coords[1], search_radius_origin,   # 3 → origin CTE
        dest_coords[0],   dest_coords[1],   search_radius_dest,     # 3 → dest CTE
        current_sec,      current_sec,                              # 2 → departure_sec >= y <=
        dest_coords[0],   dest_coords[1],                           # 2 → dist(stop → dest)
        origin_coords[0], origin_coords[1],                         # 2 → dist(origin → dest) punto A
        dest_coords[0],   dest_coords[1],                           # 2 → dist(origin → dest) punto B
        current_sec,                                                # 1 → total_travel_time
        current_sec,                                                # 1 → wait_for_first_bus
        current_sec,                                                # 1 → WHERE BETWEEN
    )

    df = pd.read_sql(query, conn, params=params)

    if df.empty:
        conn.close()
        return {
            "status": "Not found",
            "reason": "We found no trips with transfers within the search window"
        }

    # Mantener solo rutas válidas
    df = df[df["total_travel_time"] > 0]
    df = df[df["wait_for_first_bus"] >= 0]
    df = df.sort_values("total_travel_time")
    df = df.drop_duplicates(subset=["leg2_stop", "dest_stop"], keep="first")
    df = df.head(1)

    if df.empty:
        conn.close()
        return {
            "status": "Not found",
            "reason": "All found trips have already departed"
        }

    cur = conn.cursor()

    # OPT 7: origin_stop filtrado por trip1 para evitar stops de otros operadores
    trip1_sample = df.iloc[0]["trip1"]
    cur.execute("""
        SELECT s.stop_id, s.stop_name, s.stop_lat, s.stop_lon
        FROM stops s
        JOIN stop_times st ON st.stop_id = s.stop_id
        WHERE st.trip_id = %s
        ORDER BY ST_Distance(
            s.geom::geography,
            ST_SetSRID(ST_Point(%s, %s), 4326)::geography
        )
        LIMIT 1;
    """, (trip1_sample, origin_coords[0], origin_coords[1]))
    origin_stop = cur.fetchone()

    if origin_stop is None:
        cur.close()
        conn.close()
        return {"status": "Not found", "reason": "No stops found near origin"}

    # Obtener stops necesarios en lote
    all_stop_ids = list(
        set(df["leg1_stop"]) | set(df["leg2_stop"]) | set(df["dest_stop"])
    )
    cur.execute("""
        SELECT stop_id, stop_name, stop_lat, stop_lon
        FROM stops
        WHERE stop_id = ANY(%s);
    """, (all_stop_ids,))
    stops_map = {row[0]: row for row in cur.fetchall()}

    # Obtener info de trips y rutas
    trip_ids = list(set(df["trip1"]) | set(df["trip2"]))
    cur.execute("""
        SELECT
            t.trip_id,
            t.operator_id,
            r.route_id,
            r.route_type,
            r.route_color,
            r.route_short_name,
            r.route_long_name
        FROM trips t
        JOIN routes r
            ON t.route_id = r.route_id
            AND t.operator_id = r.operator_id
        WHERE t.trip_id = ANY(%s)
    """, (trip_ids,))

    transport_map = {
        row[0]: {
            "operator_id":      row[1],
            "route_id":         row[2],
            "route_type":       row[3],
            "route_color":      row[4],
            "route_short_name": row[5],
            "route_long_name":  row[6],
        }
        for row in cur.fetchall()
    }

    # Guard: validar que todos los IDs necesarios están en los mapas
    missing_stops = set(all_stop_ids) - set(stops_map.keys())
    missing_trips = set(trip_ids) - set(transport_map.keys())
    if missing_stops or missing_trips:
        cur.close()
        conn.close()
        return {
            "status": "Not found",
            "reason": f"Missing data: stops={missing_stops}, trips={missing_trips}"
        }

    routes = []

    for _, row in df.iterrows():

        trip1 = row["trip1"]
        trip2 = row["trip2"]

        transfer_stop = stops_map[row["leg2_stop"]]
        dest_stop     = stops_map[row["dest_stop"]]

        # Obtener la secuencia real del stop de origen en trip1
        cur.execute("""
            SELECT stop_sequence
            FROM stop_times
            WHERE trip_id = %s AND stop_id = %s
            LIMIT 1;
        """, (trip1, origin_stop[0]))
        origin_seq_row = cur.fetchone()
        origin_seq = origin_seq_row[0] if origin_seq_row else 0

        # LEG 1
        leg1_trip_details = {
            "trip_id":              trip1,
            "operator_id_origin":   transport_map[trip1]["operator_id"],
            "route_type":           transport_map[trip1]["route_type"],
            "route_long_name":      transport_map[trip1]["route_long_name"],
            "route_short_name":     transport_map[trip1]["route_short_name"],
            "route_color":          transport_map[trip1]["route_color"],
            "stop_name_origin":     origin_stop[1],
            "stop_lat_origin":      origin_stop[2],
            "stop_lon_origin":      origin_stop[3],
            "stop_lat_dest":        transfer_stop[2],
            "stop_lon_dest":        transfer_stop[3],
            "stop_sequence_origin": origin_seq,
            "stop_sequence_dest":   row["seq1"],
            # OPT 3: wait correcto — tiempo hasta que sale el bus desde el stop de origen
            "wait_for_first_bus":   row["wait_for_first_bus"],
        }

        leg1_transport = transport_map[trip1]

        # LEG 2
        leg2_trip_details = {
            "trip_id":              trip2,
            "operator_id_origin":   transport_map[trip2]["operator_id"],
            "route_type":           transport_map[trip2]["route_type"],
            "route_long_name":      transport_map[trip2]["route_long_name"],
            "route_short_name":     transport_map[trip2]["route_short_name"],
            "route_color":          transport_map[trip2]["route_color"],
            "stop_name_origin":     transfer_stop[1],
            "stop_lat_origin":      transfer_stop[2],
            "stop_lon_origin":      transfer_stop[3],
            "stop_lat_dest":        dest_stop[2],
            "stop_lon_dest":        dest_stop[3],
            "stop_sequence_origin": row["seq2"],
            "stop_sequence_dest":   row["seq3"],
            # OPT 8: nomenclatura clara — transfer vs destino final
            "arrival_time_transfer": row["arrival_time_second_trip"],
            "arrival_time_dest":     row["dest_arrival_time"],
        }

        leg2_transport = transport_map[trip2]

        leg1_trip_geometry = get_direct_trip_geometry(cur, leg1_trip_details, leg1_transport)
        leg2_trip_geometry = get_direct_trip_geometry(cur, leg2_trip_details, leg2_transport)

        route = {
            "origin": {
                "id":   origin_stop[0],
                "name": origin_stop[1],
                "lat":  origin_stop[2],
                "lon":  origin_stop[3],
            },
            "transfer": {
                "id":   transfer_stop[0],
                "name": transfer_stop[1],
                "lat":  transfer_stop[2],
                "lon":  transfer_stop[3],
            },
            "destination": {
                "id":   dest_stop[0],
                "name": dest_stop[1],
                "lat":  dest_stop[2],
                "lon":  dest_stop[3],
            },
            "leg1": {
                "trip_details":      leg1_trip_details,
                "transport_details": leg1_transport,
                "trip_geometry":     leg1_trip_geometry,
            },
            "leg2": {
                "trip_details":      leg2_trip_details,
                "transport_details": leg2_transport,
                "trip_geometry":     leg2_trip_geometry,
            },
            "total_time": row["total_travel_time"],
            "wait_time":  row["t2"] - row["t1"],
            "now_time":   now_text,
        }

        routes.append(route)

    cur.close()
    conn.close()
    return {"status": "Found", "details": routes}