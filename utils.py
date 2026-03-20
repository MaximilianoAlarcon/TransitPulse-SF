import math

def get_direct_trip_geometry(cur, trip_details, transport_details):

    trip_id = trip_details["trip_id"]
    operator_id = trip_details["operator_id_origin"]

    origin_coords = (
        trip_details["stop_lat_origin"],
        trip_details["stop_lon_origin"]
    )

    dest_coords = (
        trip_details["stop_lat_dest"],
        trip_details["stop_lon_dest"]
    )

    # ---------------------------------------
    # 1. Obtener shape_id
    # ---------------------------------------
    cur.execute("""
        SELECT shape_id
        FROM trips
        WHERE trip_id = %s
        AND operator_id = %s
    """, (trip_id, operator_id))

    row = cur.fetchone()
    shape_id = row[0] if row else None

    # ---------------------------------------
    # 2. Validar existencia de shape
    # ---------------------------------------
    has_shape = False

    if shape_id:
        cur.execute("""
            SELECT 1
            FROM shapes
            WHERE operator_id = %s
            AND shape_id = %s
            LIMIT 1
        """, (operator_id, shape_id))

        has_shape = cur.fetchone() is not None

    # ---------------------------------------
    # 3. Si hay shape → usarlo
    # ---------------------------------------
    if has_shape:

        cur.execute("""
            SELECT shape_pt_lat, shape_pt_lon
            FROM shapes
            WHERE operator_id = %s
            AND shape_id = %s
            ORDER BY shape_pt_sequence
        """, (operator_id, shape_id))

        coords = [(float(lat), float(lon)) for lat, lon in cur.fetchall()]

        geometry_type = "shape"

    # ---------------------------------------
    # 4. Fallback → usar stops
    # ---------------------------------------
    else:

        cur.execute("""
            SELECT s.stop_lat, s.stop_lon
            FROM stop_times st
            JOIN stops s
              ON st.stop_id = s.stop_id
             AND st.operator_id = s.operator_id
            WHERE st.trip_id = %s
            AND st.operator_id = %s
            ORDER BY st.stop_sequence
        """, (trip_id, operator_id))

        coords = [(float(lat), float(lon)) for lat, lon in cur.fetchall()]

        geometry_type = "stops"

    # ---------------------------------------
    # 5. Retornar resultado listo para frontend
    # ---------------------------------------
    return {
        "geometry_type": geometry_type,  # "shape" o "stops"
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