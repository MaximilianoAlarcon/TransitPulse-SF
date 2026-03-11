from flask import Flask, render_template, jsonify
import random, os, requests, json
import threading,load_gtfs_stops,execute_query_postgis

app = Flask(__name__)

API_KEY = os.environ.get("API_511_KEY")
API_GEO_KEY = os.environ.get("API_GEO_KEY")

# Configuración DB
DB_CONFIG = {
    "host": os.environ.get("DB_HOST"),
    "database": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "port": os.environ.get("DB_PORT")
}


# Función para geocoding
def geocode_address(address):
    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={address}&key={API_GEO_KEY}"
    resp = requests.get(url).json()
    status = resp.get("status")
    if status == "OK":
        location = resp["results"][0]["geometry"]["location"]
        return location["lat"], location["lng"], None
    elif status == "ZERO_RESULTS":
        return None, None, "Dirección no encontrada"
    else:
        return None, None, f"Error de geocoding: {status}"

# Función para obtener la parada más cercana
def get_closest_stop(lat, lon):
    conn = psycopg2.connect(**DB_CONFIG)
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


@app.route("/closest-stop")
def closest_stop():
    address = request.args.get("address")
    if not address:
        return jsonify({"error": "No se recibió dirección"}), 400

    lat, lon, error = geocode_address(address)
    if error:
        return jsonify({"error": error}), 404

    stop = get_closest_stop(lat, lon)
    if not stop:
        return jsonify({"error": "No se encontró ninguna parada"}), 404

    print("Esta es la parada mas cercana")
    print(stop)

    return jsonify({
        "stop_name": stop[0],
        "stop_lat": stop[1],
        "stop_lon": stop[2]
    })



@app.route("/api/operators")
def operators():

    url = " http://api.511.org/transit/gtfsoperators?api_key="+API_KEY

    response = requests.get(url)

    if response.status_code != 200:
        return {"error": "API failed"}

    data = json.loads(response.content.decode("utf-8-sig"))
    return jsonify(data)




def run_import():
    load_gtfs_stops.run()

@app.route("/load-stops")
def load_stops():

    thread = threading.Thread(target=run_import)
    thread.start()

    return {"status": "GTFS import started"}





def run_query_postgis():
    execute_query_postgis.run()

@app.route("/query-postgis")
def query_postgis():

    thread = threading.Thread(target=run_query_postgis)
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