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


async function checkLocationPermission() {
  if (!navigator.permissions) {
    return "unsupported";
  }

  try {
    const result = await navigator.permissions.query({ name: "geolocation" });
    return result.state; // "granted" | "prompt" | "denied"
  } catch (error) {
    console.error("Error checking permissions:", error);
    return "error";
  }
}

let paymentMethodsCache = [];

async function getPaymentMethods() {
  const res = await fetch("/payment-methods");
  const data = await res.json();
  return data;
}

async function queryPaymentMethods() {
  paymentMethodsCache = await getPaymentMethods();
}

queryPaymentMethods();


mobile_app_url = {
    "clipper": {
        "android": "https://play.google.com/store/apps/details?id=com.clippercard.mobile.clipper",
        "ios": "https://apps.apple.com/us/app/clipper-card/id1534042451"
    },
    "munimobile": {
        "android": "https://play.google.com/store/apps/details?id=de.hafas.android.sfmta",
        "ios": "https://apps.apple.com/us/app/munimobile/id6466818495"
    }
}

function hide_element(element_id){
  document.getElementById(element_id).style.display = "none";
}

function show_element(element_id){
  document.getElementById(element_id).style.display = "block";
}

const modeToRouteType = {
  // 🚋 Tram / Light rail
  TRAM: 0,
  LIGHT_RAIL: 0,
  STREETCAR: 0,
  // 🚇 Subway / Metro
  SUBWAY: 1,
  METRO: 1,
  // 🚆 Rail
  RAIL: 2,
  TRAIN: 2,
  // 🚌 Bus
  BUS: 3,
  COACH: 3,
  TROLLEYBUS: 3,
  SHUTTLE: 3,
  // ⛴️ Ferry
  FERRY: 4,
  // 🚠 Cable / special
  CABLE_CAR: 5,
  GONDOLA: 6,
  FUNICULAR: 7
};


function showAlert(message, type = "danger") {
    const container = document.getElementById("alert-container");

    const alert = document.createElement("div");
    alert.className = `alert alert-${type} shadow text-center d-inline-block fade show mb-0`;
    alert.setAttribute("role", "alert");
    alert.textContent = message;

    container.innerHTML = "";
    container.appendChild(alert);

    setTimeout(() => {
        alert.classList.remove("show");
        setTimeout(() => alert.remove(), 200);
    }, 3000);
}

function focusMap(lat, lon, zoom = 16) {
  map.setView([lat, lon], zoom, {
    animate: true
  });
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
let selectedPlaceOrigin = null
const suggestionsBox = document.getElementById("suggestions")
const suggestionsBoxOrigin = document.getElementById("suggestions-origin")
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




function markDest(destLat, destLon) {

    const redIcon = new L.Icon({
        iconUrl: "https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-red.png",
        shadowUrl: "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-shadow.png",
        iconSize: [25,41],
        iconAnchor: [12,41]
    })

    destMarker = L.marker([destLat, destLon], {icon: redIcon}).addTo(routesLayer)

}




// Mostrar ubicacion del usuario en tiempo real

let watchId = null;
let compassEnabled = false;
let deviceHeading = null;
let movementHeading = null;
let lastPosition = null;

window.userMarker = null;
window.userAccuracyCircle = null;
window.userDirectionCone = null;

/**
 * Convierte grados a radianes
 */
function toRad(deg) {
  return (deg * Math.PI) / 180;
}

/**
 * Normaliza un ángulo al rango 0-360
 */
function normalizeHeading(deg) {
  return ((deg % 360) + 360) % 360;
}

/**
 * Obtiene heading a partir del movimiento entre dos puntos
 */
function getHeadingFromMovement(prev, current) {
  if (!prev || !current) return null;

  const lat1 = toRad(prev.lat);
  const lon1 = toRad(prev.lng);
  const lat2 = toRad(current.lat);
  const lon2 = toRad(current.lng);

  const dLon = lon2 - lon1;

  const y = Math.sin(dLon) * Math.cos(lat2);
  const x =
    Math.cos(lat1) * Math.sin(lat2) -
    Math.sin(lat1) * Math.cos(lat2) * Math.cos(dLon);

  const bearing = Math.atan2(y, x) * (180 / Math.PI);
  return normalizeHeading(bearing);
}

/**
 * Devuelve el heading más confiable disponible
 * prioridad:
 * 1) brújula
 * 2) dirección por movimiento
 */
function getBestHeading() {
  if (deviceHeading !== null) return deviceHeading;
  if (movementHeading !== null) return movementHeading;
  return null;
}

/**
 * Genera puntos para un cono de dirección
 * angle: apertura del cono en grados
 * radiusMeters: largo del cono en metros
 */
function createDirectionCone(lat, lng, heading, angle = 40, radiusMeters = 35) {
  if (heading === null) return null;

  const points = [];
  const steps = 18;
  const startAngle = heading - angle / 2;
  const endAngle = heading + angle / 2;

  points.push([lat, lng]);

  for (let i = 0; i <= steps; i++) {
    const currentAngle = startAngle + ((endAngle - startAngle) * i) / steps;
    const rad = toRad(currentAngle);

    // Aproximación local para metros -> grados
    const dLat = (radiusMeters * Math.cos(rad)) / 111320;
    const dLng =
      (radiusMeters * Math.sin(rad)) /
      (111320 * Math.cos(toRad(lat)));

    points.push([lat + dLat, lng + dLng]);
  }

  points.push([lat, lng]);

  return points;
}

/**
 * Inicia la lectura de brújula si está disponible
 * En iPhone/iPad requiere gesto del usuario para pedir permiso
 */
async function startCompass() {
  if (
    typeof DeviceOrientationEvent === "undefined" ||
    compassEnabled
  ) {
    return;
  }

  try {
    // iOS Safari
    if (typeof DeviceOrientationEvent.requestPermission === "function") {
      const permission = await DeviceOrientationEvent.requestPermission();
      if (permission !== "granted") {
        console.warn("Permiso de brújula denegado");
        return;
      }
    }

    window.addEventListener(
      "deviceorientation",
      (event) => {
        let heading = null;

        // iOS suele exponer webkitCompassHeading
        if (typeof event.webkitCompassHeading === "number") {
          heading = event.webkitCompassHeading;
        }
        // Android / otros navegadores
        else if (typeof event.alpha === "number") {
          // Ajuste común para que 0 sea norte
          heading = 360 - event.alpha;
        }

        if (heading !== null && !Number.isNaN(heading)) {
          deviceHeading = normalizeHeading(heading);
          compassEnabled = true;

          // Si ya existe marcador, actualizar cono aunque el usuario no se mueva
          if (lastPosition && window.userDirectionCone) {
            const conePoints = createDirectionCone(
              lastPosition.lat,
              lastPosition.lng,
              getBestHeading()
            );
            if (conePoints) {
              window.userDirectionCone.setLatLngs(conePoints);
            }
          }
        }
      },
      true
    );
  } catch (error) {
    console.warn("No se pudo iniciar la brújula:", error);
  }
}

/**
 * Crea o actualiza el marcador, círculo de precisión y cono
 */
function updateUserLayers(map, lat, lng, accuracy) {
  const latlng = [lat, lng];

  // marcador
  if (!window.userMarker) {
    window.userMarker = L.circleMarker(latlng, {
      radius: 8,
      color: "#136aec",
      fillColor: "#2a93ee",
      fillOpacity: 0.9,
      weight: 2
    })
      .addTo(map)
      .bindPopup("You");
  } else {
    window.userMarker.setLatLng(latlng);
  }

  // accuracy circle
  if (!window.userAccuracyCircle) {
    window.userAccuracyCircle = L.circle(latlng, {
      radius: accuracy,
      color: "#136aec",
      fillColor: "#136aec",
      fillOpacity: 0.12,
      weight: 1
    }).addTo(map);
  } else {
    window.userAccuracyCircle.setLatLng(latlng);
    window.userAccuracyCircle.setRadius(accuracy);
  }

  // cono de dirección
  const heading = getBestHeading();
  const conePoints = createDirectionCone(lat, lng, heading);

  if (conePoints) {
    if (!window.userDirectionCone) {
      window.userDirectionCone = L.polygon(conePoints, {
        color: "#136aec",
        fillColor: "#136aec",
        fillOpacity: 0.18,
        weight: 0
      }).addTo(map);
    } else {
      window.userDirectionCone.setLatLngs(conePoints);
    }
  } else if (window.userDirectionCone) {
    // si todavía no hay heading, ocultamos el cono
    window.userDirectionCone.setLatLngs([]);
  }
}

/**
 * Inicia el tracking en tiempo real
 *
 * options:
 * - centerOnFirstFix: centra el mapa la primera vez
 * - followUser: centra el mapa en cada actualización
 * - zoom: zoom inicial si se centra el mapa
 * - enableCompass: intenta usar brújula
 */
async function startUserTracking(map, options = {}) {
  const {
    centerOnFirstFix = true,
    followUser = false,
    zoom = 16,
    enableCompass = true
  } = options;

  if (!navigator.geolocation) {
    console.error("Geolocation no soportado por este navegador");
    return;
  }

  if (watchId !== null) {
    console.warn("El tracking ya está activo");
    return;
  }

  if (enableCompass) {
    await startCompass();
  }

  let firstFix = true;

  watchId = navigator.geolocation.watchPosition(
    (position) => {
      const lat = position.coords.latitude;
      const lng = position.coords.longitude;
      const accuracy = position.coords.accuracy ?? 0;

      const currentPosition = { lat, lng };

      // fallback por movimiento si no hay brújula
      if (lastPosition) {
        const computedHeading = getHeadingFromMovement(lastPosition, currentPosition);

        // solo actualizar si hubo desplazamiento real
        const movedEnough =
          map.distance(
            [lastPosition.lat, lastPosition.lng],
            [currentPosition.lat, currentPosition.lng]
          ) > 3;

        if (movedEnough && computedHeading !== null) {
          movementHeading = computedHeading;
        }
      }

      lastPosition = currentPosition;

      updateUserLayers(map, lat, lng, accuracy);

      if (firstFix && centerOnFirstFix) {
        map.setView([lat, lng], zoom);
        firstFix = false;
      } else if (followUser) {
        map.setView([lat, lng], map.getZoom());
      }

      console.log("Updated location:", {
        lat,
        lng,
        accuracy,
        heading: getBestHeading()
      });
    },
    (error) => {
      switch (error.code) {
        case error.PERMISSION_DENIED:
          console.error("El usuario denegó el permiso de ubicación");
          break;
        case error.POSITION_UNAVAILABLE:
          console.error("La ubicación no está disponible");
          break;
        case error.TIMEOUT:
          console.error("La solicitud de ubicación expiró");
          break;
        default:
          console.error("Error de geolocalización:", error.message);
      }
    },
    {
      enableHighAccuracy: true,
      maximumAge: 0,
      timeout: 10000
    }
  );
}

/**
 * Detiene el tracking
 */
function stopUserTracking() {
  if (watchId !== null) {
    navigator.geolocation.clearWatch(watchId);
    watchId = null;
    console.log("Stopped tracking");
  }
}

/**
 * Elimina las capas del usuario del mapa
 */
function removeUserTrackingLayers(map) {
  if (window.userMarker) {
    map.removeLayer(window.userMarker);
    window.userMarker = null;
  }

  if (window.userAccuracyCircle) {
    map.removeLayer(window.userAccuracyCircle);
    window.userAccuracyCircle = null;
  }

  if (window.userDirectionCone) {
    map.removeLayer(window.userDirectionCone);
    window.userDirectionCone = null;
  }

  lastPosition = null;
  movementHeading = null;
}

/**
 * Detiene tracking y borra las capas
 */
function destroyUserTracking(map) {
  stopUserTracking();
  removeUserTrackingLayers(map);
}

// Check location permission

async function checkLocationPermission() {
  if (!navigator.permissions) {
    return "unsupported";
  }

  try {
    const result = await navigator.permissions.query({ name: "geolocation" });
    return result.state; // "granted" | "prompt" | "denied"
  } catch (error) {
    console.error("Error checking location permission:", error);
    return "error";
  }
}

function requestLocationOnce() {
  return new Promise((resolve, reject) => {
    navigator.geolocation.getCurrentPosition(resolve, reject, {
      enableHighAccuracy: true,
      maximumAge: 0,
      timeout: 10000
    });
  });
}

async function initLocation() {
  const permission = await checkLocationPermission();

  if (permission === "granted") {
    startUserTracking(map, {
    centerOnFirstFix: true,
    followUser: false,
    zoom: 16,
    enableCompass: true
    });
    return true;
  }

  if (permission === "prompt" || permission === "unsupported" || permission === "error") {
    try {
        await requestLocationOnce(); // esto dispara el popup
        startUserTracking(map, {
        centerOnFirstFix: true,
        followUser: false,
        zoom: 16,
        enableCompass: true
        });
        return true;
    } catch (error) {
      if (error.code === error.PERMISSION_DENIED) {
        showAlert("Share your location to get more accurate routes.", "info");
      } else {
        showAlert("Could not get your location.", "info");
      }
      return false;
    }
  }

  if (permission === "denied") {
    showAlert("Please enable location in your browser settings and reload the page", "info");
    return false;
  }

  return false;
}


initLocation();









// --- Sidebar drag handle ---
const sidebar = document.getElementById("sidebar");
const mapContainer = document.getElementById("map-container");
const handle = document.getElementById("drag-handle");

let isDragging = false;
let dragOffsetY = 0;
let dragOffsetX = 0;

handle.addEventListener("mousedown", (e) => {
    isDragging = true;
    const sidebarRect = sidebar.getBoundingClientRect();
    dragOffsetX = e.clientX - sidebarRect.left;
    e.preventDefault();
});

handle.addEventListener("touchstart", (e) => {
    isDragging = true;
    const touch = e.touches[0];
    const handleRect = handle.getBoundingClientRect();
    // Offset del dedo DENTRO del handle (normalmente ~0-25px)
    dragOffsetY = touch.clientY - handleRect.top;
    e.preventDefault();
    document.body.style.overflow = "hidden";
}, { passive: false });

document.addEventListener("mouseup", () => {
    isDragging = false;
    document.body.style.overflow = "";
});

document.addEventListener("touchend", () => {
    isDragging = false;
    document.body.style.overflow = "";
});

document.addEventListener("touchcancel", () => {
    isDragging = false;
    document.body.style.overflow = "";
});

document.addEventListener("mousemove", dragHandler);
document.addEventListener("touchmove", dragHandler, { passive: false });

function dragHandler(e) {
    if (!isDragging) return;
    e.preventDefault();

    const isMobile = window.innerWidth <= 1024;
    const clientY = e.touches ? e.touches[0].clientY : e.clientY;
    const clientX = e.touches ? e.touches[0].clientX : e.clientX;

    if (isMobile) {
        // El top del handle es donde está el dedo menos el offset interno
        const handleTop = clientY - dragOffsetY;
        const handleH = handle.offsetHeight;
        // La altura del sidebar empieza DESPUÉS del handle
        const newSidebarHeight = window.innerHeight - handleTop - handleH;
        const clampedHeight = Math.max(80, Math.min(window.innerHeight - 80, newSidebarHeight));

        sidebar.style.height = clampedHeight + "px";
        mapContainer.style.height = (window.innerHeight - clampedHeight - handleH) + "px";
    } else {
        const sidebarRect = sidebar.getBoundingClientRect();
        const leftOfSidebar = clientX - dragOffsetX + sidebarRect.left;
        const newSidebarWidth = leftOfSidebar;

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
const chatOrigin = document.getElementById("chat-origin");
const chatResult = document.getElementById("chat-result");
const transportOptions = document.getElementById("transport-type");
const clearBtn = document.getElementById("clear-input");
const clearBtnOrigin = document.getElementById("clear-origin");



// limpiar al hacer click
clearBtn.addEventListener("click", () => {
  chatInput.value = "";
  chatInput.focus();
  clearBtn.style.display = "none";
});

clearBtnOrigin.addEventListener("click", () => {
  chatOrigin.value = "";
  chatOrigin.focus();
  clearBtnOrigin.style.display = "none";
});





transportOptions.addEventListener("change", () => {
  if (transportOptions.value == "public-transport"){
    show_element("block-priority")
    show_element("block-time")
    show_element("block-walking-distance")
    show_element("block-wheelchair")
    document.getElementById("message-for-user").innerHTML = ""
  } else if (transportOptions.value == "car"){
    hide_element("block-priority")
    show_element("block-time")
    hide_element("block-walking-distance")
    hide_element("block-wheelchair")
    document.getElementById("message-for-user").innerHTML = ""
  } else if (transportOptions.value == "walk"){
    hide_element("block-priority")
    hide_element("block-time")
    hide_element("block-walking-distance")
    hide_element("block-wheelchair")
    document.getElementById("message-for-user").innerHTML = "No more filters for Walk 🚶"
  }
});



const btnAdvancedOptions = document.getElementById("toggle-advanced-options");
const advancedOptions = document.getElementById("advanced-options");
btnAdvancedOptions.addEventListener("click", () => {
  const isHidden = advancedOptions.style.display === "none";

  advancedOptions.style.display = isHidden ? "block" : "none";

  btnAdvancedOptions.textContent = isHidden
    ? "⚙️ Less filters"
    : "⚙️ More filters";
});

const timeType = document.getElementById("time-type");
const timeInput = document.getElementById("time-input");

timeType.addEventListener("change", () => {
  if (timeType.value === "now") {
    //timeInput.value = "";
    timeInput.style.display = "none";
  } else {
    timeInput.style.display = "block";
  }
});


function getAdvancedTransportFilters() {
  const transportType = document.getElementById("transport-type").value;
  const advancedOptions = document.getElementById("advanced-options");

  const isAdvancedVisible = advancedOptions.style.display !== "none";

  const result = {
    transport_type: transportType,
    inputs: {}
  };

  // Si el panel avanzado no está visible, devolvemos inputs vacío
  if (!isAdvancedVisible) {
    return result;
  }

  // PUBLIC TRANSPORT
  if (transportType === "public-transport") {
    const pref = document.querySelector('input[name="pref"]:checked')?.value || null;
    const timeType = document.getElementById("time-type").value;
    const timeInput = document.getElementById("time-input").value;
    const maxWalk = document.getElementById("max-walk").value;
    const wheelchair = document.getElementById("wheelchair").checked;

    result.inputs = {
      priority: pref,
      time: {
        type: timeType,
        value: timeType === "now" ? "" : timeInput
      },
      max_walking_distance: maxWalk === "" ? "" : Number(maxWalk),
      wheelchair_accessible: wheelchair
    };

    return result;
  }

  // CAR
  if (transportType === "car") {
    const timeType = document.getElementById("time-type").value;
    const timeInput = document.getElementById("time-input").value;

    result.inputs = {
      time: {
        type: timeType,
        value: timeType === "now" ? "" : timeInput
      }
    };

    return result;
  }

  // WALK
  if (transportType === "walk") {
    return result;
  }

  return result;
}


async function getPlaceRatingReviews(placeId) {
  try {
    console.log(placeId);

    const res = await fetch(`/place-rating-reviews?place_id=${placeId}`);

    if (!res.ok) {
      throw new Error("Request failed");
    }

    const data = await res.json();

    if (data["rating"] || data["review_summary"]) {
      const review_text =
        (data["rating"] ? data["rating"] + "⭐ " : "") +
        (data["review_summary"] || "");

      showAlert(review_text, "info");

      const reviewBox = document.createElement("div");
      reviewBox.className = "alert alert-info shadow text-center d-inline-block fade show mb-0";
      reviewBox.setAttribute("role", "alert");
      reviewBox.style.width = "auto";
      reviewBox.style.maxWidth = "100%";
      reviewBox.textContent = review_text;

      document.getElementById("chat-result").appendChild(reviewBox);
    }

  } catch (err) {
    console.error("Error fetching place rating:", err);
    console.log(placeId);
  }
}


function toggle_inputs(state){
  chatSend.disabled = !state;
  chatInput.disabled = !state;
  chatOrigin.disabled = !state;
  transportOptions.disabled = !state;
  suggestionsBox.style.display = state? "block" : "none";
  suggestionsBox.innerHTML = ""
  suggestionsBoxOrigin.style.display = state? "block" : "none";
  suggestionsBoxOrigin.innerHTML = ""
}

chatSend.addEventListener("click", async () => {

    toggle_inputs(false);
    let address = document.getElementById("chat-input").value.trim();
    if (!address) {
      toggle_inputs(true); 
      return showAlert("Enter your destination");
    }
    let transport_type = document.getElementById("transport-type").value
    if (!transport_type) {
        transportOptions.value = "public-transport";
        transport_type = "public-transport"
    }
    let address_origin = document.getElementById("chat-origin").value.trim();

    clearRoutes()
    lat = null
    lon = null
    lat_origin = null
    lon_origin = null
    if (selectedPlace){
        lat = selectedPlace.lat
        lon = selectedPlace.lon
    }
    if (selectedPlaceOrigin){
        lat_origin = selectedPlaceOrigin.lat
        lon_origin = selectedPlaceOrigin.lon
    }
    if (selectedPlaceOrigin == null && address_origin == "") {
        const state = await checkLocationPermission();
        if (state != "granted") {
            showAlert("It requires permission to track your location","info")
            navigator.geolocation.getCurrentPosition(
                () => startUserTracking(map),
                () => showAlert("It requires permission to track your location","info")
            );
            toggle_inputs(true);
            return;
        }
        if (lastPosition){
            const state = await checkLocationPermission();
            if (state == "granted") {
                lat_origin = lastPosition.lat
                lon_origin = lastPosition.lng
            } else  {
                showAlert("It requires permission to track your location","info")
                navigator.geolocation.getCurrentPosition(
                    () => startUserTracking(map),
                    () => showAlert("It requires permission to track your location","info")
                );
                toggle_inputs(true);
                return;
            }
        }
    }   


    document.getElementById("chat-result").innerHTML = `
    <p>Searching paths...</p>
    <div class="spinner"></div>`;
    try {
        //Search trip

        const advancedFilters = getAdvancedTransportFilters();

        const place_id = null
        if (selectedPlace){
          place_id = selectedPlace.place_id
        }

        const payload = {
          address,
          lat,
          lon,
          transport_type,
          address_origin,
          lat_origin,
          lon_origin,
          advanced_filters: advancedFilters,
          place_id
        };

        console.log("advancedFilters")
        console.log(advancedFilters)

        const response = await fetch("/search-trip", {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify(payload)
        });
        if (!response.ok) {
            let errData = await response.json();
            document.getElementById("chat-result").innerText = errData.error || "Unknown error";
            toggle_inputs(true);
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
                    trip_description += `<hr><p>${otpMsToSfHour(leg.startTime)} - ${otpMsToSfHour(leg.endTime)} ${styles["icon"]}</p>`
                    if (leg.mode == "WALK"){
                        trip_description += `
                            <p>Walk from ${leg.from.name} to ${leg.to.name} for ${formatDuration(leg.duration)}</p><hr>
                        `
                    } else if (leg.mode == "CAR"){
                        trip_description += `
                            <p>Drive from ${leg.from.name} to ${leg.to.name} for ${formatDuration(leg.duration)}</p><hr>
                        ` 
                    } else {
                        const match = paymentMethodsCache.find(p =>
                        p.agency_id === leg.agency.gtfsId.split(":")[1] &&
                        p.route_type === modeToRouteType[leg.mode]
                        );

                        payment_methods = []
                        if (match?.fare_media_name?.includes("cash")) {
                            payment_methods.push("Cash")
                        }
                        if (match?.fare_media_name?.includes("contactless")) {
                            payment_methods.push("Credit/Debit Card")
                        }
                        if (match?.fare_media_name?.includes("clipper")) {
                            payment_methods.push(`
                                <span class="payment-method">
                                Clipper 
                                <a href="${mobile_app_url["clipper"]["android"]}" target="_blank" rel="noopener noreferrer" aria-label="Clipper Android"> <i class="fa-brands fa-google-play"></i></a>
                                <a href="${mobile_app_url["clipper"]["ios"]}" target="_blank" rel="noopener noreferrer" aria-label="Clipper iOS"><i class="fa-brands fa-apple"></i></a>
                                </span>
                                `)
                        }
                        if (match?.fare_media_name?.includes("munimobile")) {
                            payment_methods.push(`
                                <span class="payment-method">
                                MuniMobile 
                                <a href="${mobile_app_url["munimobile"]["android"]}" target="_blank" rel="noopener noreferrer" aria-label="MuniMobile Android"> <i class="fa-brands fa-google-play"></i></a>
                                <a href="${mobile_app_url["munimobile"]["ios"]}" target="_blank" rel="noopener noreferrer" aria-label="MuniMobile iOS"><i class="fa-brands fa-apple"></i></a>
                                </span>
                                `)
                        }
                        if (match?.fare_media_name?.includes("online")) {
                            payment_methods.push("Online Payment (Transport website)")
                        }

                        trip_description += `
                            <p>Take the ${leg.mode.toLowerCase()} <b>${leg.route?.longName || ""} ${leg.route?.shortName || ""}</b> ${leg.headsign ? `towards <b>${leg.headsign}</b>` : ""} at "${leg.from.name}"</p>
                            <img class="place-img" src="/place-image?lat=${leg.from.lat}&lon=${leg.from.lon}&name=${leg.from.name}&is_stop=true" />
                            Then, get off at "${leg.to.name}", it will take around ${formatDuration(leg.duration)}</p>
                            <p>${match?.payment_method_code == "1" ? "The ticket is paid <b>before</b> boarding the transport." : "The ticket is paid <b>on</b> boarding the transport."}</p>
                            <p>Payment method:</p>
                            <p>${payment_methods.length > 0 ? payment_methods.join(" / ") : "No payment methods available"}</p><hr>
                        `
                    }
                });

                trip_description += `<img class="place-img" src="/place-image?lat=${globalItineraries["dest_lat"]}&lon=${globalItineraries["dest_lon"]}&name=${data["dest_name"]}" />`

                trip_options += createAccordionItem(option,`${option}# ${formatDuration(itinerary.duration)}`,trip_description)
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
                    console.log("Cambiando ruta")
                    console.log("id")
                    console.log(id)
                    console.log("itinerary")
                    console.log(itinerary)
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

            focusMap(data["origin_coords"][1],data["origin_coords"][0])
            hide_element("advanced-options")
            btnAdvancedOptions.textContent = "⚙️ More filters";

            getPlaceRatingReviews(data["place_id"]).catch((err) => {console.error("Unexpected reviews error:", err);});

        } else if (data["status"] == "Not found"){
            document.getElementById("chat-result").innerHTML = `<p>${data["reason"]}</p>`
        }
        toggle_inputs(true);
        
    } catch (error) {
        document.getElementById("chat-result").innerText = "Internal Error";
        console.error(error);
        toggle_inputs(true);
    }
    selectedPlace = null;
    selectedPlaceOrigin = null;
    toggle_inputs(true);
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
        place.rating = response["rating"]
        place.review_summary = response["review_summary"]
        // 👉 Ejemplo: centrar mapa (Leaflet)
        map.setView([place.lat, place.lon], 14)
        selectedPlace = place
    }
}

async function onPlaceSelectedOrigin(map, place) {

    response = await fetch(`/place-details?place_id=${place.place_id}`);
    if (response.ok) {
        place.lat = response["lat"]
        place.lon = response["lon"]
        // 👉 Ejemplo: centrar mapa (Leaflet)
        map.setView([place.lat, place.lon], 14)
        selectedPlaceOrigin = place
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

chatOrigin.addEventListener("input", () => {
    if (!chatOrigin.disabled && !chatSend.disabled) {
        clearBtnOrigin.style.display = chatOrigin.value ? "block" : "none";
        const query = chatOrigin.value

        if (query.length < 3) {
            suggestionsBoxOrigin.innerHTML = ""
            return
        }

        clearTimeout(timeout)

        timeout = setTimeout(() => {
            fetch(`/autocomplete?q=${encodeURIComponent(query)}`)
                .then(res => res.json())
                .then(data => {
                    suggestionsBoxOrigin.innerHTML = ""

                    // 🔥 NUEVO: manejar sin resultados
                    if (!data || data.length === 0) {
                        const div = document.createElement("div")
                        div.classList.add("suggestion-item")
                        div.classList.add("no-results") 
                        div.innerText = "No suggestions to show"
                        
                        suggestionsBoxOrigin.appendChild(div)
                        return
                    }

                    suggestionsBoxOrigin.classList.add("active");

                    data.forEach(place => {
                        const div = document.createElement("div")
                        div.classList.add("suggestion-item")

                        div.innerText = place.name

                        div.addEventListener("click", () => {
                            chatOrigin.value = place.name
                            suggestionsBoxOrigin.innerHTML = ""
                            onPlaceSelectedOrigin(map,place)
                        })

                        suggestionsBoxOrigin.appendChild(div)
                    })
                })
        }, 300)
    } else {
        suggestionsBoxOrigin.innerHTML = ""
        return
    }
})


document.addEventListener("click", (e) => {
    if (!chatInput.contains(e.target) && !suggestionsBox.contains(e.target)) {
        suggestionsBox.innerHTML = ""
    }
    if (!chatOrigin.contains(e.target) && !suggestionsBoxOrigin.contains(e.target)) {
        suggestionsBoxOrigin.innerHTML = ""
    }
})