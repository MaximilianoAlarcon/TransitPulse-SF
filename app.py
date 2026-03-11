from flask import Flask, render_template, jsonify
import random, os, requests, json
import load_gtfs_stops

app = Flask(__name__)

API_KEY = os.environ.get("API_511_KEY")


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