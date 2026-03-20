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