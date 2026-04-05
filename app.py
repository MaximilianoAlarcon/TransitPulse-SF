from flask import Flask, render_template, jsonify, request, Response
import random, os, requests, json
import threading,execute_query_postgis
import claude_test
import load_payment_methods
import psycopg2
import numpy as np
from utils import geocode,summarize_place_reviews_with_claude
from datetime import datetime
from zoneinfo import ZoneInfo
import context_aware_recommendations


app = Flask(__name__)

API_KEY = os.environ.get("API_511_KEY")
API_GEO_KEY = os.environ.get("API_GEO_KEY")
MAPBOX_API_KEY = os.environ.get("MAPBOX_API_KEY")
OTP_URL = os.environ.get("OTP_URL")

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



def get_sf_date_time():
    now_sf = datetime.now(ZoneInfo("America/Los_Angeles"))
    
    fecha = now_sf.strftime("%Y-%m-%d")
    hora = now_sf.strftime("%H:%M")
    
    return fecha, hora



def normalize_advanced_inputs(transport_type: str, inputs: dict, default_time: str):
    result = {
        "priority": None,
        "time": default_time,
        "arrive_by": False,
        "max_walk_distance": None,
        "wheelchair": False,
    }

    if transport_type == "public-transport":
        result["priority"] = inputs.get("priority") or "fastest"

        time_data = inputs.get("time", {}) or {}
        time_type = time_data.get("type", "now")
        time_value = time_data.get("value", "")

        if time_type == "depart" and time_value:
            result["time"] = f"{time_value}:00"
        elif time_type == "arrive" and time_value:
            result["time"] = f"{time_value}:00"
            result["arrive_by"] = True

        max_walk = inputs.get("max_walking_distance")
        if max_walk not in ("", None):
            result["max_walk_distance"] = int(max_walk)

        result["wheelchair"] = bool(inputs.get("wheelchair_accessible", False))

    elif transport_type == "car":
        time_data = inputs.get("time", {}) or {}
        time_type = time_data.get("type", "now")
        time_value = time_data.get("value", "")

        if time_type == "depart" and time_value:
            result["time"] = f"{time_value}:00"
        elif time_type == "arrive" and time_value:
            result["time"] = f"{time_value}:00"
            result["arrive_by"] = True

    return result


def otp_plan(
    otp_url: str,
    from_lat: float,
    from_lon: float,
    to_lat: float,
    to_lon: float,
    date: str,
    time: str,
    arrive_by: bool = False,
    transport_modes: str = "{ mode: WALK }, { mode: TRANSIT }",
    search_window: int = 3600,
    num_itineraries: int = 5,
    max_transfers: int = 3,
    max_walk_distance: int | None = None,
    wheelchair: bool = False
):
    query = """
    query PlanTrip(
      $fromLat: Float!,
      $fromLon: Float!,
      $toLat: Float!,
      $toLon: Float!,
      $date: String!,
      $time: String!,
      $arriveBy: Boolean!,
      $searchWindow: Long!,
      $numItineraries: Int!,
      $maxTransfers: Int!,
      $wheelchair: Boolean!,
      $maxWalkDistance: Float
    ) {
      plan(
        from: { lat: $fromLat, lon: $fromLon }
        to: { lat: $toLat, lon: $toLon }
        date: $date
        time: $time
        arriveBy: $arriveBy
        searchWindow: $searchWindow
        numItineraries: $numItineraries
        maxTransfers: $maxTransfers
        wheelchair: $wheelchair
        maxWalkDistance: $maxWalkDistance
        transportModes: [__MODES__]
      ) {
        nextPageCursor
        previousPageCursor
        itineraries {
          duration
          startTime
          endTime
          generalizedCost
          walkDistance
          legs {
            duration
            mode
            headsign
            startTime
            endTime
            from { name lat lon }
            to { name lat lon }
            route { gtfsId shortName longName textColor }
            legGeometry { points }
            agency { gtfsId name }
          }
        }
      }
    }
    """

    variables = {
        "fromLat": from_lat,
        "fromLon": from_lon,
        "toLat": to_lat,
        "toLon": to_lon,
        "date": date,
        "time": time,
        "arriveBy": arrive_by,
        "searchWindow": search_window,
        "numItineraries": num_itineraries,
        "maxTransfers": max_transfers,
        "wheelchair": wheelchair,
        "maxWalkDistance": max_walk_distance
    }

    query = query.replace("__MODES__", transport_modes)

    response = requests.post(
        otp_url,
        json={"query": query, "variables": variables},
        timeout=30
    )

    response.raise_for_status()
    return response.json(), response.status_code


@app.route("/search-trip", methods=["POST"])
def search_trip():
    payload = request.get_json(silent=True) or {}

    address = payload.get("address")
    address_origin = payload.get("address_origin")
    lat = payload.get("lat")
    lon = payload.get("lon")
    lat_origin = payload.get("lat_origin")
    lon_origin = payload.get("lon_origin")
    transport_type = (payload.get("transport_type") or "public-transport").lower()
    advanced_filters = payload.get("advanced_filters") or {}

    try:
        lat = float(lat) if lat is not None else None
        lon = float(lon) if lon is not None else None
    except (TypeError, ValueError):
        lat, lon = None, None

    dest_name = None
    if lat is None or lon is None:
        if not address:
            return jsonify({"error": "No destination received"}), 400

        search_coords = geocode(address)
        if isinstance(search_coords, tuple):
            error_dict, status_code = search_coords
            return jsonify({"error": error_dict["error"]}), status_code

        if "error" in search_coords:
            return jsonify({"error": search_coords["error"]}), 404

        lat = search_coords["lat"]
        lon = search_coords["lon"]
        dest_name = search_coords["name"]

    try:
        lat_origin = float(lat_origin) if lat_origin is not None else None
        lon_origin = float(lon_origin) if lon_origin is not None else None
    except (TypeError, ValueError):
        lat_origin, lon_origin = None, None

    if lat_origin is None or lon_origin is None:
        search_coords = geocode(address_origin, is_origin=True)

        if isinstance(search_coords, tuple):
            error_dict, status_code = search_coords
            return jsonify({"error": error_dict["error"]}), status_code

        if "error" in search_coords:
            return jsonify({"error": search_coords["error"]}), 404

        lat_origin = search_coords["lat"]
        lon_origin = search_coords["lon"]

    origin_coords = (lon_origin, lat_origin)
    dest_coords = (lon, lat)

    date_now, hour_now = get_sf_date_time()

    TRANSPORT_MAP = {
        "public-transport": "{ mode: WALK }, { mode: TRANSIT }",
        "car": "{ mode: CAR }",
        "walk": "{ mode: WALK }"
    }

    transport_modes = TRANSPORT_MAP.get(
        transport_type,
        "{ mode: WALK }, { mode: TRANSIT }"
    )

    inputs = advanced_filters.get("inputs", {})

    normalized = normalize_advanced_inputs(
        transport_type=transport_type,
        inputs=inputs,
        default_time=f"{hour_now}:00"
    )

    date = date_now
    priority = normalized["priority"]
    time = normalized["time"]
    arrive_by = normalized["arrive_by"]
    max_walk_distance = normalized["max_walk_distance"]
    wheelchair = normalized["wheelchair"]
    num_itineraries = 5

    # Filtros por modo
    if transport_type == "public-transport":

        time_data = inputs.get("time", {})
        time_type = time_data.get("type", "now")
        time_value = time_data.get("value", "")

        if time_type == "depart" and time_value:
            time = f"{time_value}:00"
        elif time_type == "arrive" and time_value:
            time = f"{time_value}:00"
            arrive_by = True

    elif transport_type == "car":
        priority = None

        time_data = inputs.get("time", {})
        time_type = time_data.get("type", "now")
        time_value = time_data.get("value", "")

        if time_type == "depart" and time_value:
            time = f"{time_value}:00"
        elif time_type == "arrive" and time_value:
            time = f"{time_value}:00"
            arrive_by = True

    else:  # walk
        priority = None

    search, search_status = otp_plan(
        OTP_URL,
        origin_coords[1],
        origin_coords[0],
        dest_coords[1],
        dest_coords[0],
        date,
        time,
        arrive_by=arrive_by,
        transport_modes=transport_modes,
        num_itineraries=num_itineraries,
        max_walk_distance=max_walk_distance,
        wheelchair=wheelchair
    )

    itineraries = (
        search.get("data", {})
        .get("plan", {})
        .get("itineraries", [])
    )

    # Ordenamiento según priority, solo para transporte público
    if transport_type == "public-transport" and itineraries:
        if priority == "fastest":
            itineraries.sort(key=lambda x: x.get("duration", float("inf")))
        elif priority == "fewest":
            itineraries.sort(key=lambda x: len([
                leg for leg in x.get("legs", [])
                if leg.get("mode") != "WALK"
            ]))
        elif priority == "walking":
            itineraries.sort(key=lambda x: x.get("walkDistance", float("inf")))

    if search_status == 200 and itineraries:
        return jsonify(sanitize({
            "status": "Found",
            "itineraries": itineraries,
            "origin_coords": origin_coords,
            "dest_coords": dest_coords,
            "dest_name": dest_name,
            "applied_filters": {
                "transport_type": transport_type,
                "date": date,
                "time": time,
                "arrive_by": arrive_by,
                "max_walk_distance": max_walk_distance,
                "wheelchair": wheelchair
            }
        }))

    if transport_type == "walk":
        return jsonify({
            "status": "Not found",
            "reason": "This route is not suitable for walking, please select another transport type"
        })

    return jsonify({
        "status": "Not found",
        "reason": "We couldn't find a route at the moment. This app only works in Northern California"
    })

PLACES_NEARBY_URL = "https://places.googleapis.com/v1/places:searchNearby"
PLACES_BASE_URL = "https://places.googleapis.com/v1"
STREET_VIEW_URL = "https://maps.googleapis.com/maps/api/streetview"


def street_view_response(lat, lon, max_width):
    try:
        img_resp = requests.get(
            STREET_VIEW_URL,
            params={
                "size": f"{max_width}x300",
                "location": f"{lat},{lon}",
                "key": API_GEO_KEY,
            },
            stream=True,
            timeout=10,
        )
        img_resp.raise_for_status()

        return Response(
            img_resp.iter_content(chunk_size=8192),
            content_type=img_resp.headers.get("Content-Type", "image/jpeg"),
            direct_passthrough=True,
        )
    except requests.RequestException as e:
        return jsonify({"error": "Street View failed", "details": str(e)}), 502


@app.route("/place-image")
def place_image():
    lat = request.args.get("lat", type=float)
    lon = request.args.get("lon", type=float)
    radius = request.args.get("radius", default=300, type=int)
    max_width = request.args.get("max_width", default=400, type=int)
    use_places = request.args.get("use_places", default="false").lower() == "true"

    if not API_GEO_KEY:
        return jsonify({"error": "Missing API_GEO_KEY"}), 500

    if lat is None or lon is None:
        return jsonify({"error": "lat and lon are required"}), 400

    max_width = max(1, min(max_width, 640))

    # Por defecto: Street View
    if not use_places:
        return street_view_response(lat, lon, max_width)

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": API_GEO_KEY,
        "X-Goog-FieldMask": "places.displayName,places.photos",
    }

    payload = {
        "locationRestriction": {
            "circle": {
                "center": {
                    "latitude": lat,
                    "longitude": lon,
                },
                "radius": radius,
            }
        },
        "maxResultCount": 5,
        "rankPreference": "DISTANCE",
        "includedPrimaryTypes": [
            "transit_station",
            "train_station",
            "subway_station",
            "tourist_attraction"
        ],
    }

    try:
        nearby_resp = requests.post(
            PLACES_NEARBY_URL,
            headers=headers,
            json=payload,
            timeout=10,
        )
        nearby_resp.raise_for_status()
        nearby_data = nearby_resp.json()
    except requests.RequestException:
        return street_view_response(lat, lon, max_width)

    places = nearby_data.get("places", [])

    photo_name = None
    for place in places:
        photos = place.get("photos", [])
        if photos:
            photo_name = photos[0].get("name")
            break

    if not photo_name:
        return street_view_response(lat, lon, max_width)

    image_url = f"{PLACES_BASE_URL}/{photo_name}/media"

    try:
        img_resp = requests.get(
            image_url,
            params={
                "maxWidthPx": max_width,
                "key": API_GEO_KEY,
            },
            stream=True,
            timeout=15,
        )
        img_resp.raise_for_status()
    except requests.RequestException:
        return street_view_response(lat, lon, max_width)

    return Response(
        img_resp.iter_content(chunk_size=8192),
        content_type=img_resp.headers.get("Content-Type", "image/jpeg"),
        direct_passthrough=True,
    )



@app.route("/autocomplete")
def autocomplete():
    query = request.args.get("q")

    if not query:
        return jsonify({"error": "Missing input"}), 400

    # Endpoint de Google Places Autocomplete
    url = "https://maps.googleapis.com/maps/api/place/autocomplete/json"
    params = {
        "input": query,
        "key": API_GEO_KEY,
        "location": "37.7749,-122.4194",  # centro de SF
        "radius": 80000,  # 80 km alrededor
    }

    response = requests.get(url, params=params)
    data = response.json()

    suggestions = []

    for prediction in data.get("predictions", []):
        # Para autocompletado rápido devolvemos el place_id y el nombre
        suggestions.append({
            "name": prediction["description"],
            "place_id": prediction["place_id"]
        })

    return jsonify(suggestions)


@app.route("/place-details")
def place_details():
    place_id = request.args.get("place_id")
    if not place_id:
        return jsonify({"error": "Missing place_id"}), 400

    if not API_GEO_KEY:
        return jsonify({"error": "Missing API_GEO_KEY"}), 500

    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "key": API_GEO_KEY,
        "fields": "geometry,name,rating,user_ratings_total,types,formatted_address,reviews"
    }

    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        return jsonify({"error": "Google Places request failed", "details": str(e)}), 502

    result = data.get("result")
    if not result or "geometry" not in result:
        return jsonify({"error": "Place not found"}), 404

    reviews = result.get("reviews", [])
    review_texts = [r.get("text", "") for r in reviews[:3] if r.get("text")]

    review_summary = summarize_place_reviews_with_claude(
        place_name=result.get("name", ""),
        rating=result.get("rating"),
        review_texts=review_texts
    )

    print("review_summary")
    print(review_summary)

    return jsonify({
        "name": result.get("name"),
        "formatted_address": result.get("formatted_address"),
        "lat": result["geometry"]["location"]["lat"],
        "lon": result["geometry"]["location"]["lng"],
        "type": result.get("types", ["unknown"])[0],
        "types": result.get("types", []),
        "rating": result.get("rating"),
        "user_ratings_total": result.get("user_ratings_total"),
        "review_summary": review_summary
    })



@app.route("/payment-methods", methods=["GET"])
def get_payment_methods():
    conn = None

    try:
        conn = get_connection()

        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    agency_id, 
                    route_type, 
                    payment_method_code, 
                    fare_media_name 
                FROM route_payment_methods
            """)

            rows = cur.fetchall()

            columns = [desc[0] for desc in cur.description]

            results = [
                dict(zip(columns, row))
                for row in rows
            ]

        return jsonify(results), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            conn.close()



def run_claude_test():
    claude_test.run()

@app.route("/claude-test")
def endpoint_claude_test():

    thread = threading.Thread(target=run_claude_test)
    thread.start()

    return {"message": "Holitoo"}


def run_load_payment_methods():
    load_payment_methods.run()

@app.route("/load-route-payment-methods")
def endpoint_load_payment_methods():

    thread = threading.Thread(target=run_load_payment_methods)
    thread.start()

    return {"message": "Holitoo"}



def run_context_aware_recommendations():
    context_aware_recommendations.run()

@app.route("/context_aware_recommendations")
def endpoint_context_aware_recommendations():

    thread = threading.Thread(target=run_context_aware_recommendations)
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