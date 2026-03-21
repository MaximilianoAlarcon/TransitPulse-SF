from flask import Flask, render_template, jsonify, request
import random, os, requests, json
import threading,load_gtfs_stops,execute_query_postgis,load_gtfs_routes
import load_gtfs_trips,load_gtfs_stop_times,load_gtfs_shapes
import direct_trip_search_prototype,transfer_trip_search_prototype,claude_test
from claude import transform_input_address
from transit_engine import find_direct_trip,find_trip_with_transfer
import psycopg2
import numpy as np
from utils import geocode

app = Flask(__name__)

API_KEY = os.environ.get("API_511_KEY")
API_GEO_KEY = os.environ.get("API_GEO_KEY")
MAPBOX_API_KEY = os.environ.get("MAPBOX_API_KEY")

# Configuración DB
DB_CONFIG = {
    "host": os.environ.get("DB_HOST"),
    "database": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "port": os.environ.get("DB_PORT")
}


def sanitize(obj):
    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize(v) for v in obj]
    elif isinstance(obj, tuple):
        return tuple(sanitize(v) for v in obj)
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    else:
        return obj


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


# Función para geocoding
def geocode_address(address):
    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={address}&key={API_GEO_KEY}"
    resp = requests.get(url).json()
    status = resp.get("status")
    if status == "OK":
        location = resp["results"][0]["geometry"]["location"]
        return location["lat"], location["lng"], None
    elif status == "ZERO_RESULTS":
        return None, None, "Address not found"
    else:
        return None, None, f"Geocoding error: {status}"

# Función para obtener la parada más cercana
def get_closest_stop(lat, lon):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT stop_name, stop_lat, stop_lon
        FROM stops
        ORDER BY geom <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326)
        LIMIT 1;
    """, (lon, lat))
    stop = cur.fetchone()
    cur.close()
    conn.close()
    return stop


@app.route("/stops")
def stops():
    lat_min = float(request.args.get("lat_min"))
    lon_min = float(request.args.get("lon_min"))
    lat_max = float(request.args.get("lat_max"))
    lon_max = float(request.args.get("lon_max"))

    conn = get_connection()
    cur = conn.cursor()

    # ST_MakeEnvelope crea un rectángulo con los límites, SRID 4326 = WGS84
    cur.execute("""
        SELECT stop_name, stop_lat, stop_lon
        FROM stops
        WHERE geom && ST_MakeEnvelope(%s, %s, %s, %s, 4326)
    """, (lon_min, lat_min, lon_max, lat_max))

    rows = cur.fetchall()

    result = [
        {"stop_name": r[0], "stop_lat": r[1], "stop_lon": r[2]}
        for r in rows
    ]

    return jsonify(result)


@app.route("/direct-trip")
def direct_trip():
    my_location = (-122.4120372,37.7803603)

    address = request.args.get("address")
    lat = request.args.get("lat")
    lon = request.args.get("lon")
    try:
        lat = float(lat) if lat is not None else None
        lon = float(lon) if lon is not None else None
    except ValueError:
        lat, lon = None, None

    if lat is None or lon is None:
        if not address:
            return jsonify({"error": "No address received"}), 400
        search_coords = geocode(address)
        if "error" in search_coords:
            return jsonify({"error": search_coords["error"]}), 404
        lat = search_coords["lat"]
        lon = search_coords["lon"]

    origin_coords = my_location
    dest_coords = (lon,lat)

    search = find_direct_trip(origin_coords,dest_coords)
    if search["status"] == "Found":
        return jsonify(sanitize({
            "status": "Found",
            "details": search["details"],
            "origin_coords":origin_coords,
            "dest_coords":dest_coords
        }))
    else:
        return jsonify(sanitize({
            "status": search["status"],
            "reason": search["reason"],
            "origin_coords":origin_coords,
            "dest_coords":dest_coords
        }))


@app.route("/transfer-trip")
def transfer_trip():
    my_location = (-122.4120372,37.7803603)

    address = request.args.get("address")
    lat = request.args.get("lat")
    lon = request.args.get("lon")
    try:
        lat = float(lat) if lat is not None else None
        lon = float(lon) if lon is not None else None
    except ValueError:
        lat, lon = None, None

    if lat is None or lon is None:
        if not address:
            return jsonify({"error": "No address received"}), 400
        search_coords = geocode(address)
        if "error" in search_coords:
            return jsonify({"error": search_coords["error"]}), 404
        lat = search_coords["lat"]
        lon = search_coords["lon"]

    origin_coords = my_location
    dest_coords = (lon,lat)

    search = find_trip_with_transfer(origin_coords,dest_coords,auto_estimate_radius=True)
    if search["status"] == "Found":
        return jsonify(sanitize({
            "status": "Found",
            "details": search["details"],
            "origin_coords":origin_coords,
            "dest_coords":dest_coords
        }))
    else:
        return jsonify(sanitize({
            "status": search["status"],
            "reason": search["reason"],
            "origin_coords":origin_coords,
            "dest_coords":dest_coords
        }))


@app.route("/autocomplete")
def autocomplete():
    query = request.args.get("q")

    if not query:
        return jsonify({"error": "Missing input"}), 400

    url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{query}.json"

    params = {
        "access_token": MAPBOX_API_KEY,
        "autocomplete": "true",
        "limit": 5,
        "proximity": "-122.4194,37.7749",
        "country": "US"
    }

    response = requests.get(url, params=params)
    data = response.json()

    suggestions = []

    for feature in data.get("features", []):
        suggestions.append({
            "name": feature["place_name"],
            "lat": feature["center"][1],
            "lon": feature["center"][0],
            "type": feature["place_type"][0],
            "relevance": feature["relevance"]
        })

    suggestions.sort(key=lambda x: x["relevance"], reverse=True)

    return jsonify(suggestions)






@app.route("/api/operators")
def operators():

    url = " http://api.511.org/transit/gtfsoperators?api_key="+API_KEY

    response = requests.get(url)

    if response.status_code != 200:
        return {"error": "API failed"}

    data = json.loads(response.content.decode("utf-8-sig"))
    return jsonify(data)




def run_load_stops():
    load_gtfs_stops.run()

@app.route("/load-stops")
def load_stops():

    thread = threading.Thread(target=run_load_stops)
    thread.start()

    return {"status": "GTFS import started"}


def run_load_routes():
    load_gtfs_routes.run()

@app.route("/load-routes")
def load_routes():

    thread = threading.Thread(target=run_load_routes)
    thread.start()

    return {"status": "GTFS import started"}



def run_load_trips():
    load_gtfs_trips.run()

@app.route("/load-trips")
def load_trips():

    thread = threading.Thread(target=run_load_trips)
    thread.start()

    return {"status": "GTFS import started"}



def run_load_stop_times():
    load_gtfs_stop_times.run()

@app.route("/load-stop-times")
def load_stop_times():

    thread = threading.Thread(target=run_load_stop_times)
    thread.start()

    return {"status": "GTFS import started"}



def run_load_shapes():
    load_gtfs_shapes.run()

@app.route("/load-shapes")
def load_shapes():

    thread = threading.Thread(target=run_load_shapes)
    thread.start()

    return {"status": "GTFS import started"}




def run_query_postgis():
    execute_query_postgis.run()

@app.route("/query-postgis")
def query_postgis():

    thread = threading.Thread(target=run_query_postgis)
    thread.start()

    return {"message": "Holitoo"}






def run_transfer_trip_search_prototype():
    transfer_trip_search_prototype.run()

@app.route("/transfer-trip-search-test")
def endpoint_transfer_trip_search_prototype():

    thread = threading.Thread(target=run_transfer_trip_search_prototype)
    thread.start()

    return {"message": "Holitoo"}



def run_direct_trip_search_prototype():
    direct_trip_search_prototype.run()

@app.route("/direct-trip-search-test")
def endpoint_direct_trip_search_prototype():

    thread = threading.Thread(target=run_direct_trip_search_prototype)
    thread.start()

    return {"message": "Holitoo"}



def run_claude_test():
    claude_test.run()

@app.route("/claude-test")
def endpoint_claude_test():

    thread = threading.Thread(target=run_claude_test)
    thread.start()

    return {"message": "Holitoo"}




@app.route("/")
def dashboard():
    return render_template("index.html")

@app.route("/api/vehicles")
def vehicles():
    data = [
        {
            "id": i,
            "lat": 37.77 + random.uniform(-0.02,0.02),
            "lon": -122.41 + random.uniform(-0.02,0.02),
            "status": random.choice(["moving","stopped","delivery"])
        }
        for i in range(10)
    ]
    return jsonify(data)

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)