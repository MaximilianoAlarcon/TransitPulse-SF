import math
import requests
from flask import jsonify
import os

MAPBOX_API_KEY = os.environ.get("MAPBOX_API_KEY")

# Cache simple en memoria
SHAPE_CACHE = {}

def get_direct_trip_geometry(cur, trip_details, transport_details, search_shapes=False):
    trip_id = trip_details["trip_id"]
    operator_id = trip_details["operator_id_origin"]

    seq_origin = trip_details["stop_sequence_origin"]
    seq_dest = trip_details["stop_sequence_dest"]

    seq_origin = int(seq_origin) if seq_origin is not None else None
    seq_dest = int(seq_dest) if seq_dest is not None else None

    origin_coords = (
        float(trip_details["stop_lat_origin"]),
        float(trip_details["stop_lon_origin"])
    )

    dest_coords = (
        float(trip_details["stop_lat_dest"]),
        float(trip_details["stop_lon_dest"])
    )

    coords = []

    if search_shapes:
        geometry_type = "stops"

        # ------------------------------
        # 1. Obtener shape_id
        # ------------------------------
        cur.execute("""
            SELECT shape_id
            FROM trips
            WHERE trip_id = %s
            AND operator_id = %s
        """, (trip_id, operator_id))
        row = cur.fetchone()
        shape_id = row[0] if row else None

        # ------------------------------
        # 2. Intentar usar SHAPES reales
        # ------------------------------
        if shape_id and seq_origin is not None and seq_dest is not None:
            cur.execute("""
                SELECT stop_sequence, shape_dist_traveled
                FROM stop_times
                WHERE trip_id = %s
                AND operator_id = %s
                AND stop_sequence BETWEEN %s AND %s
            """, (trip_id, operator_id, min(seq_origin, seq_dest), max(seq_origin, seq_dest)))

            rows = cur.fetchall()
            dist_map = {r[0]: r[1] for r in rows if r[1] is not None}
            dist_start = dist_map.get(seq_origin)
            dist_end = dist_map.get(seq_dest)

            if dist_start is not None and dist_end is not None:
                cur.execute("""
                    SELECT shape_pt_lat, shape_pt_lon
                    FROM shapes
                    WHERE operator_id = %s
                    AND shape_id = %s
                    AND shape_dist_traveled BETWEEN %s AND %s
                    ORDER BY shape_pt_sequence
                """, (operator_id, shape_id, min(dist_start, dist_end), max(dist_start, dist_end)))

                shape_rows = cur.fetchall()
                coords = [(float(lat), float(lon)) for lat, lon in shape_rows]

                # Recorte fino
                if coords:
                    def closest_point_index(shape, target):
                        return min(range(len(shape)),
                                key=lambda i: (shape[i][0]-target[0])**2 + (shape[i][1]-target[1])**2)

                    start_idx = closest_point_index(coords, origin_coords)
                    end_idx = closest_point_index(coords, dest_coords)
                    if start_idx > end_idx:
                        start_idx, end_idx = end_idx, start_idx
                    coords = coords[start_idx:end_idx+1]
                    if coords:
                        geometry_type = "shape"

    # ------------------------------
    # 3. Fallback → línea recta entre origen y destino
    # ------------------------------
    if not coords:
        coords = [origin_coords, dest_coords]
        geometry_type = "line"

    # ------------------------------
    # 4. Resultado
    # ------------------------------
    return {
        "geometry_type": geometry_type,
        "coordinates": coords,
        "origin": origin_coords,
        "destination": dest_coords,
        "route_info": {
            "route_id": transport_details["route_id"],
            "route_type": transport_details["route_type"],
            "route_color": transport_details["route_color"],
            "route_name": transport_details["route_short_name"]
        }
    }

def time_to_seconds(t):
    """Convert GTFS HH:MM:SS to seconds"""
    if pd.isna(t):
        return None
    h, m, s = map(int, t.split(":"))
    return h*3600 + m*60 + s

def estimate_radius(conn, coords):
    lon, lat = coords

    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*)
        FROM stops
        WHERE ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_Point(%s,%s),4326)::geography,
            500
        );
    """, (lon, lat))

    count = cur.fetchone()[0]
    cur.close()

    if count > 30:
        return 600
    elif count > 10:
        return 1000
    else:
        return 1500


def haversine_distance(lat1, lon1, lat2, lon2):
    """Distancia en metros entre dos coordenadas"""
    R = 6371000  # radio de la Tierra en metros

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

    return R * c


def should_use_transit(origin_coords, dest_coords):
    """
    Decide si conviene usar transporte o caminar
    origin_coords = (lon, lat)
    dest_coords = (lon, lat)
    """

    lon1, lat1 = origin_coords
    lon2, lat2 = dest_coords

    distance = haversine_distance(lat1, lon1, lat2, lon2)

    # 👴 velocidad conservadora (todas las edades)
    walking_speed = 1.0  # m/s

    walking_time = distance / walking_speed  # en segundos

    # 🔥 reglas
    if distance < 350:
        return False  # caminar

    if walking_time < 420:  # 7 minutos
        return False

    return True  # usar transporte



def geocode(place):

    if not place:
        return jsonify({"error": "Missing input"}), 400

    url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{place}.json"

    # 🔹 intento 1: búsqueda restringida (SF)
    params = {
        "access_token": MAPBOX_API_KEY,
        "limit": 1,
        "proximity": "-122.4194,37.7749",
        "bbox": "-122.55,37.68,-122.35,37.83",
        "country": "US"
    }

    response = requests.get(url, params=params)
    data = response.json()

    features = data.get("features", [])

    # 🔥 fallback: si no hay resultados, ampliar búsqueda
    if not features:
        params.pop("bbox", None)  # quitar restricción fuerte

        response = requests.get(url, params=params)
        data = response.json()
        features = data.get("features", [])

    # 🔥 fallback 2: sin restricciones
    if not features:
        params = {
            "access_token": MAPBOX_API_KEY,
            "limit": 1
        }

        response = requests.get(url, params=params)
        data = response.json()
        features = data.get("features", [])

    # ❌ si aun así no hay nada
    if not features:
        return jsonify({"error": "Place not found"}), 404

    feature = features[0]

    result = {
        "name": feature["place_name"],
        "lat": feature["center"][1],
        "lon": feature["center"][0],
        "type": feature["place_type"][0]
    }

    return jsonify(result)