// --- Leaflet Map ---
var map = L.map('map', { 
    zoomControl: window.innerWidth > 1024,
    touchZoom: true,
    rotate: true,
    bearing: 0
}).setView([37.77,-122.41], 12);

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution:'© OpenStreetMap'
}).addTo(map);

function showAlert(message) {
    const container = document.getElementById("alert-container");

    const alert = document.createElement("div");
    alert.className = "alert alert-danger shadow text-center d-inline-block fade show mb-0";
    alert.setAttribute("role", "alert");
    alert.textContent = message;

    container.innerHTML = "";
    container.appendChild(alert);

    setTimeout(() => {
        alert.classList.remove("show");
        setTimeout(() => alert.remove(), 200);
    }, 3000);

}

// Rotacion de mapa

let followHeading = false;
let lastBearing = 0;
let watchId = null;
let lastPosition = null;

// Umbrales para evitar temblores
const MIN_SPEED_TO_ROTATE = 1.0;   // m/s aprox
const MIN_BEARING_DELTA = 5;       // grados mínimos para actualizar

// --- Marcador del usuario ---
window.userMarker = null;

// --- Helpers ---
function normalizeBearing(deg) {
  return ((deg % 360) + 360) % 360;
}

function smallestAngleDiff(a, b) {
  let diff = Math.abs(a - b) % 360;
  return diff > 180 ? 360 - diff : diff;
}

function setMapBearingSmooth(newBearing) {
  const normalized = normalizeBearing(newBearing);

  if (smallestAngleDiff(normalized, lastBearing) < MIN_BEARING_DELTA) {
    return;
  }

  map.setBearing(normalized);
  lastBearing = normalized;
}

function resetRotation() {
  map.setBearing(0);
  lastBearing = 0;
}

function computeBearing(lat1, lon1, lat2, lon2) {
  const toRad = (d) => d * Math.PI / 180;
  const toDeg = (r) => r * 180 / Math.PI;

  const φ1 = toRad(lat1);
  const φ2 = toRad(lat2);
  const λ1 = toRad(lon1);
  const λ2 = toRad(lon2);

  const y = Math.sin(λ2 - λ1) * Math.cos(φ2);
  const x =
    Math.cos(φ1) * Math.sin(φ2) -
    Math.sin(φ1) * Math.cos(φ2) * Math.cos(λ2 - λ1);

  return normalizeBearing(toDeg(Math.atan2(y, x)));
}

// --- Permiso de orientación (iPhone/Safari) ---
async function requestOrientationPermissionIfNeeded() {
  if (
    typeof DeviceOrientationEvent !== "undefined" &&
    typeof DeviceOrientationEvent.requestPermission === "function"
  ) {
    const result = await DeviceOrientationEvent.requestPermission();
    return result === "granted";
  }
  return true;
}

// --- Botón de navegación ---
const btnCompass = document.getElementById("btn-compass");

btnCompass.addEventListener("click", async () => {
  const granted = await requestOrientationPermissionIfNeeded();

  if (!granted) {
    alert("Orientation permission was not granted.");
    return;
  }

  followHeading = !followHeading;
  btnCompass.classList.toggle("active", followHeading);

  if (!followHeading) {
    resetRotation();
  } else if (window.userMarker) {
    map.setView(window.userMarker.getLatLng(), Math.max(map.getZoom(), 16));
  }
});

// --- Brújula del dispositivo ---
// Se usa solo si el usuario NO se está moviendo rápido.
// Si se mueve, dejamos que mande el GPS.
window.addEventListener("deviceorientation", (event) => {
  if (!followHeading) return;
  if (currentSpeed > 1) return;

  const alpha = event.alpha;
  if (alpha == null) return;

  const heading = normalizeBearing(360 - alpha);
  setMapBearingSmooth(heading);
});

// --- Seguimiento en tiempo real ---
function startUserTracking() {
  if (!navigator.geolocation) {
    console.warn("Geolocation is not supported in this browser.");
    showAlert("Geolocation is not supported in this browser.")
    return;
  }

  geoWatchId = navigator.geolocation.watchPosition(
    (position) => {
      // Para test podés descomentar estos valores fijos:
      // const lat = 37.7803603;
      // const lon = -122.4120372;

      const lat = position.coords.latitude;
      const lon = position.coords.longitude;
      currentSpeed = position.coords.speed || 0;

      const latlng = [lat, lon];

      // Crear marcador una sola vez
      if (!window.userMarker) {
        window.userMarker = L.circleMarker(latlng, {
          radius: 8,
          color: "#136aec",
          fillColor: "#2a93ee",
          fillOpacity: 0.9
        }).addTo(map).bindPopup("You");

        map.setView(latlng, 15);
      } else {
        // Mover marcador existente
        window.userMarker.setLatLng(latlng);
      }

      // Si el modo navegación está activo, seguir al usuario
      if (followHeading) {
        map.setView(latlng, Math.max(map.getZoom(), 16), {
          animate: true
        });
      }

      // Rotación usando rumbo GPS cuando hay movimiento real
      if (followHeading && lastPosition && currentSpeed > MIN_SPEED_TO_ROTATE) {
        const gpsBearing = computeBearing(
          lastPosition.lat,
          lastPosition.lon,
          lat,
          lon
        );

        setMapBearingSmooth(gpsBearing);
      }

      lastPosition = { lat, lon };

      // Si necesitás recargar paradas visibles:
      // loadStopsInView();
    },
    (error) => {
      console.error("Geolocation error:", error);
    },
    {
      enableHighAccuracy: true,
      maximumAge: 1000,
      timeout: 10000
    }
  );
}










// Ocultar zoom en mobile
if (window.innerWidth <= 1024) {
    document.querySelectorAll(".leaflet-control-zoom").forEach(ctrl => ctrl.style.display = "none");
}

// Marker cluster
const stopsLayer = L.markerClusterGroup();
map.addLayer(stopsLayer);

let routesLayer = L.featureGroup().addTo(map);

//L.polyline([[37.78, -122.41], [37.76, -122.50]], {color:'red', weight:5}).addTo(map);

//L.polyline([[37.7845, -122.4145], [37.7645, -122.5045]], {color:'violet', weight:5}).addTo(routesLayer);

let originMarker = null
let destMarker = null
let selectedPlace = null
const suggestionsBox = document.getElementById("suggestions")
let globalItineraries = {}

let timeout = null

function formatDuration(seconds) {
    seconds = Math.floor(seconds)

    const hours = Math.floor(seconds / 3600)
    const minutes = Math.floor((seconds % 3600) / 60)

    if (hours > 0) {
        return `${hours} h ${minutes} min`
    }

    return `${minutes} min`
}



function clearRoutes() {
    routesLayer.clearLayers();
}

function getRouteInfo(routeType) {
    const map = {
        WALK: {
            key: "walk",
            color: "#136aec",
            icon: "🚶"
        },
        BICYCLE: {
            key: "bicycle",
            color: "#27ae60",
            icon: "🚴"
        },
        CAR: {
            key: "car",
            color: "#136aec",
            icon: "🚗"
        },
        BUS: {
            key: "bus",
            color: "#2980b9",
            icon: "🚌"
        },
        TRAM: {
            key: "tram",
            color: "#f39c12",
            icon: "🚋"
        },
        SUBWAY: {
            key: "subway",
            color: "#8e44ad",
            icon: "🚇"
        },
        RAIL: {
            key: "rail",
            color: "#c0392b",
            icon: "🚆"
        },
        FERRY: {
            key: "ferry",
            color: "#16a085",
            icon: "⛴️"
        },
        CABLE_CAR: {
            key: "cable_car",
            color: "#d35400",
            icon: "🚠"
        },
        GONDOLA: {
            key: "gondola",
            color: "#9b59b6",
            icon: "🚡"
        },
        FUNICULAR: {
            key: "funicular",
            color: "#34495e",
            icon: "🚞"
        },
        SCOOTER: {
            key: "scooter",
            color: "#1abc9c",
            icon: "🛴"
        }
    };

    return map[routeType] || {
        label: "Transporte",
        key: "transport",
        color: "#7f8c8d",
        icon: "❓"
    };
}

function createAccordionItem(index, title, body) {
  return `
    <div class="accordion-item">
      <h2 class="accordion-header">
        <button
          class="accordion-button collapsed"
          type="button"
          data-bs-toggle="collapse"
          data-bs-target="#collapse${index}"
        >
          ${title}
        </button>
      </h2>
      <div
        id="collapse${index}"
        class="accordion-collapse collapse"
        data-bs-parent="#tripAccordion"
      >
        <div class="accordion-body">
          ${body}
        </div>
      </div>
    </div>
  `;
}


async function drawWalkingRoute(leg,defaultColor) {

    if (!leg?.legGeometry?.points) {
        return null;
    }

    const latLngs = decodePolyline(leg.legGeometry.points);

    L.polyline(latLngs, {
        color: defaultColor,
        weight: 4,
        dashArray: "5,10"
    }).addTo(routesLayer);
}

function markRouteStops(map, originLat, originLon, destLat, destLon, originColor = "#000000", destColor = "#000000",labelorigin="",labeldest="") {

    originMarker = L.circleMarker([originLat, originLon], {
        radius: 8,
        color: originColor,
        weight: 2,
        fillColor: originColor,
        fillOpacity: 0.8
    }).addTo(routesLayer).bindPopup(labelorigin);

    destMarker = L.circleMarker([destLat, destLon], {
        radius: 8,
        color: destColor,
        weight: 2,
        fillColor: destColor,
        fillOpacity: 0.8
    }).addTo(routesLayer).bindPopup(labeldest);
}






function drawLine(map, coordinates, defaultColor = "#3388ff") {
    if (!coordinates || coordinates.length < 2) return;

    console.log("Coordenadas para dibujar la linea")
    console.log(coordinates)
    // Usar arrays [lat, lon] directamente
    const polyline = L.polyline(coordinates, { color: defaultColor, weight: 5, opacity: 1 });
    
    // Agregar al layer
    polyline.addTo(routesLayer);

    // Ajustar la vista al polyline
    map.fitBounds(polyline.getBounds());
}




function markDest(destLat, destLon) {

    const redIcon = new L.Icon({
        iconUrl: "https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-red.png",
        shadowUrl: "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-shadow.png",
        iconSize: [25,41],
        iconAnchor: [12,41]
    })

    destMarker = L.marker([destLat, destLon], {icon: redIcon}).addTo(routesLayer)

}


if (navigator.geolocation) {

    navigator.geolocation.getCurrentPosition(function(position) {

        //const lat = position.coords.latitude;
        //const lon = position.coords.longitude;
        const lat = 37.7803603;
        const lon = -122.4120372;

        map.setView([lat, lon], 15);

        window.userMarker = L.circleMarker([lat, lon], {
            radius: 8,
            color: "#136aec",
            fillColor: "#2a93ee",
            fillOpacity: 0.9
        }).addTo(map).bindPopup("You");

        //loadStopsInView();

    });

}


// --- Sidebar drag handle ---
const sidebar = document.getElementById("sidebar");
const mapContainer = document.getElementById("map");
const handle = document.getElementById("drag-handle");

let isDragging = false;
handle.addEventListener("mousedown", () => isDragging = true);
handle.addEventListener("touchstart", () => isDragging = true);

document.addEventListener("mouseup", () => isDragging = false);
document.addEventListener("touchend", () => isDragging = false);

document.addEventListener("mousemove", dragHandler);
document.addEventListener("touchmove", dragHandler);

function dragHandler(e) {
    if (!isDragging) return;
    e.preventDefault();

    const isMobile = window.innerWidth <= 1024;
    const clientY = e.touches ? e.touches[0].clientY : e.clientY;
    const clientX = e.touches ? e.touches[0].clientX : e.clientX;

    if (isMobile) {
        // Vertical split
        const newSidebarHeight = window.innerHeight - clientY;
        sidebar.style.height = Math.max(100, newSidebarHeight) + "px";
        mapContainer.style.height = window.innerHeight - sidebar.offsetHeight + "px";
    } else {
        // Horizontal split
        const newSidebarWidth = clientX;
        sidebar.style.width = Math.max(200, Math.min(400, newSidebarWidth)) + "px";
    }

    map.invalidateSize();
}

function otpMsToSfHour(ms) {
    const date = new Date(ms);

    return date.toLocaleTimeString("en-US", {
        timeZone: "America/Los_Angeles",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false
    });
}

function decodePolyline(encoded) {
    let index = 0;
    const coordinates = [];
    let lat = 0;
    let lng = 0;

    while (index < encoded.length) {
        let result = 0;
        let shift = 0;
        let byte;

        do {
            byte = encoded.charCodeAt(index++) - 63;
            result |= (byte & 0x1f) << shift;
            shift += 5;
        } while (byte >= 0x20);

        const deltaLat = (result & 1) ? ~(result >> 1) : (result >> 1);
        lat += deltaLat;

        result = 0;
        shift = 0;

        do {
            byte = encoded.charCodeAt(index++) - 63;
            result |= (byte & 0x1f) << shift;
            shift += 5;
        } while (byte >= 0x20);

        const deltaLng = (result & 1) ? ~(result >> 1) : (result >> 1);
        lng += deltaLng;

        coordinates.push([lat / 1e5, lng / 1e5]);
    }

    return coordinates;
}


function drawLegGeometry(map, leg, options = {}) {
    if (!leg?.legGeometry?.points) {
        return null;
    }

    const latLngs = decodePolyline(leg.legGeometry.points);

    const polyline = L.polyline(latLngs, {
        weight: options.weight ?? 5,
        opacity: options.opacity ?? 0.9,
        color: options.color ?? "#2563eb"
    }).addTo(routesLayer);

    return polyline;
}

// --- Chat ---
const chatSend = document.getElementById("chat-send");
const chatInput = document.getElementById("chat-input");
const chatResult = document.getElementById("chat-result");
const transportOptions = document.getElementById("transport-type");
const clearBtn = document.getElementById("clear-input");


// limpiar al hacer click
clearBtn.addEventListener("click", () => {
  chatInput.value = "";
  chatInput.focus();
  clearBtn.style.display = "none";
});




chatSend.addEventListener("click", async () => {

    let address = document.getElementById("chat-input").value.trim();
    if (!address) return showAlert("Enter your destination");
    let transport_type = document.getElementById("transport-type").value
    if (!transport_type) {
        transportOptions.value = "public-transport";
        transport_type = "public-transport"
    }

    chatSend.disabled = true;
    chatInput.disabled = true;
    transportOptions.disabled = true;
    suggestionsBox.style.display = "none";
    suggestionsBox.innerHTML = ""

    clearRoutes()
    lat = null
    lon = null
    if (selectedPlace){
        lat = selectedPlace.lat
        lon = selectedPlace.lon
    }
    document.getElementById("chat-result").innerHTML = `
    <p>Searching paths...</p>
    <div class="spinner"></div>`;
    try {
        //Search trip
        let response = await fetch(`/search-trip?address=${encodeURIComponent(address)}&lat=${lat}&lon=${lon}&transport_type=${transport_type}`);
        if (!response.ok) {
            let errData = await response.json();
            document.getElementById("chat-result").innerText = errData.error || "Unknown error";
            return;
        }
        let data = await response.json();

        if ("error" in data){
            document.getElementById("chat-result").innerHTML = `
            <p>Error: <b>${data.error}</b></p>
            `;
        } else if (data["status"] == "Found") {
            lat = data["dest_coords"][1]
            lon = data["dest_coords"][0]
            option = 1
            trip_options = `<center><p>${data["dest_name"]}</p></center>`
            trip_options += '<div class="accordion" id="tripAccordion">'
            trip_description = ''
            globalItineraries = {}
            globalItineraries["dest_lat"] = lat
            globalItineraries["dest_lon"] = lon
            data["itineraries"].sort((a, b) => a.duration - b.duration);
            data["itineraries"].forEach(itinerary => {
                globalItineraries["collapse"+String(option)] = itinerary
                trip_description += `
                    <p>Duration: ${formatDuration(itinerary.duration)}</p>
                    <p>Start time: ${otpMsToSfHour(itinerary.startTime)}</p>
                    <p>End time: ${otpMsToSfHour(itinerary.endTime)}</p>
                    <p>Path</p>
                `
                itinerary.legs.forEach(leg => {
                    styles = getRouteInfo(leg.mode)
                    trip_description += `<p>${otpMsToSfHour(leg.startTime)} - ${otpMsToSfHour(leg.endTime)}</p><hr>`
                    if (leg.mode == "WALK"){
                        trip_description += `
                            <p>Walk from ${leg.from.name} to ${leg.to.name} for ${formatDuration(leg.duration)}</p>
                        `
                    } else if (leg.mode == "CAR"){
                        trip_description += `
                            <p>Drive from ${leg.from.name} to ${leg.to.name} for ${formatDuration(leg.duration)}</p>
                        ` 
                    } else {
                        trip_description += `
                            <p>Take <b>${leg.route.longName} : ${leg.route.shortName}</b> from ${leg.from.name} to ${leg.to.name} for ${formatDuration(leg.duration)}</p>
                        `
                    }
                });

                trip_options += createAccordionItem(option,`${option}: ${formatDuration(itinerary.duration)}`,trip_description)
                trip_description = ''
                option += 1
            });
            trip_options += '</div>'
            markDest(lat,lon)
            document.getElementById("chat-result").innerHTML = trip_options
            document.querySelectorAll(".accordion-collapse").forEach((el) => {
                el.addEventListener("shown.bs.collapse", (event) => {
                    clearRoutes()
                    const id = event.target.id; // collapse0, collapse1, etc
                    const itinerary = globalItineraries[id]
                    itinerary.legs.forEach(leg => {
                        styles = getRouteInfo(leg.mode)
                        markRouteStops(map, 
                            originLat=leg.from.lat, 
                            originLon=leg.from.lon, 
                            destLat=leg.to.lat, 
                            destLon=leg.to.lon, 
                            originColor = styles["color"], 
                            destColor = styles["color"],
                            labelorigin=leg.from.name,
                            labeldest=leg.to.name
                        )
                        if (leg.mode == "WALK"){
                            drawWalkingRoute(leg,styles["color"])
                        } else {
                            drawLegGeometry(map, leg, options={"color":styles["color"]});
                        }
                    });
                    markDest(globalItineraries["dest_lat"],globalItineraries["dest_lon"])
                });
            });

            const first = document.querySelector(".accordion-collapse");

            if (first) {
                new bootstrap.Collapse(first, { toggle: true });
            }

        } else if (data["status"] == "Not found"){
            document.getElementById("chat-result").innerHTML = `<p>${data["reason"]}</p>`
        }
        
    } catch (error) {
        document.getElementById("chat-result").innerText = "Internal Error";
        console.error(error);
    }
    selectedPlace = null;
    document.getElementById("chat-input").value = "";
    chatSend.disabled = false;
    chatInput.disabled = false;
    transportOptions.disabled = false;
    suggestionsBox.style.display = "block";
});

// Enter key
/*
chatInput.addEventListener("keypress", e => { if (e.key === "Enter") chatSend.click(); });
*/


async function onPlaceSelected(map, place) {

    response = await fetch(`/place-details?place_id=${place.place_id}`);
    if (response.ok) {
        place.lat = response["lat"]
        place.lon = response["lon"]
        console.log("Destino:", place.lat, place.lon)
        // 👉 Ejemplo: centrar mapa (Leaflet)
        map.setView([place.lat, place.lon], 14)
        selectedPlace = place
    }
}

chatInput.addEventListener("input", () => {
    if (!chatInput.disabled && !chatSend.disabled) {
        clearBtn.style.display = chatInput.value ? "block" : "none";
        const query = chatInput.value

        if (query.length < 3) {
            suggestionsBox.innerHTML = ""
            return
        }

        clearTimeout(timeout)

        timeout = setTimeout(() => {
            fetch(`/autocomplete?q=${encodeURIComponent(query)}`)
                .then(res => res.json())
                .then(data => {
                    suggestionsBox.innerHTML = ""

                    // 🔥 NUEVO: manejar sin resultados
                    if (!data || data.length === 0) {
                        const div = document.createElement("div")
                        div.classList.add("suggestion-item")
                        div.classList.add("no-results") 
                        div.innerText = "No suggestions to show"
                        
                        suggestionsBox.appendChild(div)
                        return
                    }

                    suggestionsBox.classList.add("active");

                    data.forEach(place => {
                        const div = document.createElement("div")
                        div.classList.add("suggestion-item")

                        div.innerText = place.name

                        div.addEventListener("click", () => {
                            chatInput.value = place.name
                            suggestionsBox.innerHTML = ""

                            console.log("Seleccionado:", place)

                            // 🔥 ACÁ conectás tu GPS
                            onPlaceSelected(map,place)
                        })

                        suggestionsBox.appendChild(div)
                    })
                })
        }, 300)
    } else {
        suggestionsBox.innerHTML = ""
        return
    }
})


document.addEventListener("click", (e) => {
    if (!chatInput.contains(e.target) && !suggestionsBox.contains(e.target)) {
        suggestionsBox.innerHTML = ""
    }
})


