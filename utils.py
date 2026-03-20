import math

import requests

def get_direct_trip_geometry(cur, trip_details, transport_details, use_routing_api=True, osrm_url="http://router.project-osrm.org/route/v1/driving/"):

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
    # 2. Intentar usar SHAPES
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

        if rows:
            dist_values = [r[1] for r in rows if r[1] is not None]

            if dist_values:
                dist_start = min(dist_values)
                dist_end = max(dist_values)

                cur.execute("""
                    SELECT shape_pt_lat, shape_pt_lon
                    FROM shapes
                    WHERE operator_id = %s
                    AND shape_id = %s
                    AND shape_dist_traveled BETWEEN %s AND %s
                    ORDER BY shape_pt_sequence
                """, (operator_id, shape_id, dist_start, dist_end))

                shape_rows = cur.fetchall()
                coords = [(float(lat), float(lon)) for lat, lon in shape_rows]

                # RECORTE FINO
                if coords:

                    def closest_point_index(shape, target):
                        return min(
                            range(len(shape)),
                            key=lambda i: (shape[i][0]-target[0])**2 + (shape[i][1]-target[1])**2
                        )

                    start_idx = closest_point_index(coords, origin_coords)
                    end_idx = closest_point_index(coords, dest_coords)
                    if start_idx > end_idx:
                        start_idx, end_idx = end_idx, start_idx
                    coords = coords[start_idx:end_idx+1]
                    if coords:
                        geometry_type = "shape"

    # ------------------------------
    # 3. Fallback → reconstruir shape a partir de stops
    # ------------------------------
    if not coords:

        if seq_origin is not None and seq_dest is not None:
            cur.execute("""
                SELECT s.stop_lat, s.stop_lon
                FROM stop_times st
                JOIN stops s
                  ON st.stop_id = s.stop_id
                 AND st.operator_id = s.operator_id
                WHERE st.trip_id = %s
                AND st.operator_id = %s
                AND st.stop_sequence BETWEEN %s AND %s
                ORDER BY st.stop_sequence
            """, (trip_id, operator_id, min(seq_origin, seq_dest), max(seq_origin, seq_dest)))
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

        stops_coords = [(float(lat), float(lon)) for lat, lon in cur.fetchall()]

        # ------------------------------
        # 3a. Usar API de routing si está activo
        # ------------------------------
        if stops_coords and use_routing_api and len(stops_coords) >= 2:

            try:
                # coords como lon,lat para OSRM
                coords_str = ";".join([f"{lon},{lat}" for lat, lon in stops_coords])
                resp = requests.get(f"{osrm_url}{coords_str}?overview=full&geometries=geojson")
                data = resp.json()
                if "routes" in data and len(data["routes"]) > 0:
                    coords = [(lat, lon) for lon, lat in data["routes"][0]["geometry"]["coordinates"]]
                    geometry_type = "shape"
            except Exception as e:
                # si falla, fallback a stops
                coords = stops_coords
                geometry_type = "stops"
        else:
            coords = stops_coords
            geometry_type = "stops"

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