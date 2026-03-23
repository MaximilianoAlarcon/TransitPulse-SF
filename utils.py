import math
import requests
import os

MAPBOX_API_KEY = os.environ.get("MAPBOX_API_KEY")
API_GEO_KEY = os.environ.get("API_GEO_KEY")

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
        return 600*2
    elif count > 10:
        return 1000*2
    else:
        return 1500*2


def estimate_radius_and_limit(conn, coords):
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
        return 600*10,40*10000
    elif count > 10:
        return 1000*10,25*10000
    else:
        return 1500*10,15*10000


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
    if distance < 700:
        return False  # caminar

    if walking_time < 600:  # 10 minutos
        return False

    return True  # usar transporte




def geocode(place):
    """
    Recibe un texto (dirección o lugar) y devuelve:
    name, lat, lon, type
    Usando Google Places API
    """
    if not place:
        return {"error": "Missing input"}, 400

    # 1️⃣ Autocomplete para obtener place_id
    autocomplete_url = "https://maps.googleapis.com/maps/api/place/autocomplete/json"
    autocomplete_params = {
        "input": place,
        "key": API_GEO_KEY,
        "location": "37.7749,-122.4194",  # centro SF
        "radius": 80000  # 80 km, cubre SF + Bay Area
    }

    auto_resp = requests.get(autocomplete_url, params=autocomplete_params)
    auto_data = auto_resp.json()
    predictions = auto_data.get("predictions", [])

    if not predictions:
        return {"error": "Place not found"}, 404

    # Tomamos la primera predicción
    place_id = predictions[0]["place_id"]

    # 2️⃣ Place Details para obtener lat/lon y tipo
    details_url = "https://maps.googleapis.com/maps/api/place/details/json"
    details_params = {
        "place_id": place_id,
        "key": API_GEO_KEY,
        "fields": "geometry,name,types"
    }

    details_resp = requests.get(details_url, params=details_params)
    details_data = details_resp.json()
    result = details_data.get("result")

    if not result or "geometry" not in result:
        return {"error": "Place not found"}, 404

    return {
        "name": result.get("name"),
        "lat": result["geometry"]["location"]["lat"],
        "lon": result["geometry"]["location"]["lng"],
        "type": result.get("types", ["unknown"])[0]
    }