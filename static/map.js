// --- Leaflet Map ---
var map = L.map('map', {
    zoomControl: window.innerWidth > 1024
}).setView([37.77,-122.41], 12);

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution:'© OpenStreetMap'
}).addTo(map);

// Ocultar zoom en mobile
if (window.innerWidth <= 1024) {
    document.querySelectorAll(".leaflet-control-zoom")
        .forEach(ctrl => ctrl.style.display = "none");
}

// Marker cluster
const stopsLayer = L.markerClusterGroup();
map.addLayer(stopsLayer);

let originMarker = null;
let destMarker = null;

// --- Split.js y handle ---
let splitInstance = null;
const sidebar = document.getElementById("sidebar");
const handle = document.getElementById("drag-handle");
const mapContainer = document.getElementById("map");

function initSplit() {
    const isMobile = window.innerWidth <= 1024;

    if (splitInstance) splitInstance.destroy();

    if (!isMobile) {
        // Desktop: Split.js horizontal
        splitInstance = Split(['#sidebar', '#map'], {
            direction: 'horizontal',
            sizes: [25,75],
            minSize: [200,200],
            gutterSize: 12,
            cursor: 'ew-resize',
            snapOffset: 0,
            onDragEnd: () => map.invalidateSize()
        });

        // Reset estilos mobile
        sidebar.style.position = "";
        sidebar.style.width = "";
        sidebar.style.height = "";
        sidebar.style.bottom = "";
        mapContainer.style.height = "";
    } else {
        // Mobile: sidebar abajo, mapa arriba, arrastre manual
        sidebar.style.position = "absolute";
        sidebar.style.bottom = "0";
        sidebar.style.left = "0";
        sidebar.style.width = "100%";
        sidebar.style.height = "40vh"; // altura inicial
        mapContainer.style.height = (window.innerHeight - sidebar.offsetHeight) + "px";
    }
}

window.addEventListener("resize", () => {
    initSplit();
    map.invalidateSize();
});

initSplit();

// --- Drag handle manual para mobile ---
let isDragging = false;

handle.addEventListener("touchstart", () => isDragging = true);
document.addEventListener("touchend", () => isDragging = false);

document.addEventListener("touchmove", (e) => {
    if (!isDragging || window.innerWidth > 1024) return;

    const screenHeight = window.innerHeight;
    let pointerY = e.touches[0].clientY;

    // Limitar pointer
    if (pointerY < 0) pointerY = 0;
    if (pointerY > screenHeight) pointerY = screenHeight;

    // Altura del sidebar desde abajo
    sidebar.style.height = (screenHeight - pointerY) + "px";

    // Altura del mapa
    mapContainer.style.height = pointerY + "px";

    map.invalidateSize();
});

// --- Funciones de ruta y markers ---
function clearRouteMarkers(map) {
    if (originMarker) { map.removeLayer(originMarker); originMarker = null; }
    if (destMarker) { map.removeLayer(destMarker); destMarker = null; }
}

function formatDuration(seconds) {
    seconds = Math.floor(seconds);
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    return hours > 0 ? `${hours} h ${minutes} min` : `${minutes} min`;
}

function markRouteStops(map, originLat, originLon, destLat, destLon) {
    const blueIcon = new L.Icon({
        iconUrl: "https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-blue.png",
        shadowUrl: "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-shadow.png",
        iconSize: [25,41],
        iconAnchor: [12,41]
    });
    const redIcon = new L.Icon({
        iconUrl: "https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-red.png",
        shadowUrl: "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-shadow.png",
        iconSize: [25,41],
        iconAnchor: [12,41]
    });

    originMarker = L.marker([originLat, originLon], {icon: blueIcon}).addTo(map);
    destMarker = L.marker([destLat, destLon], {icon: redIcon}).addTo(map);
}

// --- Geolocalización ---
if (navigator.geolocation) {
    navigator.geolocation.getCurrentPosition(function(position) {
        const lat = 37.7803603;
        const lon = -122.4120372;
        map.setView([lat, lon], 15);

        window.userMarker = L.circleMarker([lat, lon], {
            radius: 8,
            color: "#136aec",
            fillColor: "#2a93ee",
            fillOpacity: 0.9
        }).addTo(map).bindPopup("You");
    });
}

// --- Chat ---
document.getElementById("chat-send").addEventListener("click", async () => {
    clearRouteMarkers(map);
    const address = document.getElementById("chat-input").value.trim();
    if (!address) return alert("Enter your destination");

    try {
        const response = await fetch(`/direct-trip?address=${encodeURIComponent(address)}`);
        if (!response.ok) {
            const errData = await response.json();
            document.getElementById("chat-result").innerText = errData.error || "Unknown error";
            return;
        }

        const data = await response.json();
        if ("error" in data){
            document.getElementById("chat-result").innerHTML = `<p>Error: <b>${data.error}</b></p>`;
        } else {
            if (data["status"] == "Found"){
                const trip_details = data["details"];
                document.getElementById("chat-result").innerHTML = `
                    <p>You should take the transport : <b>${trip_details.route_long_name}</b></p>
                    <p>The next transport will arrive at "${trip_details.stop_name_origin}" stop in ${formatDuration(trip_details.wait_time)}</p>
                    <p>Your trip will last approximately ${formatDuration(trip_details.total_time)}</p>
                `;
                map.setView([trip_details.stop_lat_origin, trip_details.stop_lon_origin], 15);
                document.getElementById("chat-input").value = "";
                markRouteStops(map, trip_details.stop_lat_origin, trip_details.stop_lon_origin, trip_details.stop_lat_dest, trip_details.stop_lon_dest);
            } else {
                document.getElementById("chat-result").innerHTML = `
                    <p>We couldn't find a direct trip</p>
                    <p>${data.reason}</p>
                `;
            }
        }
    } catch (error) {
        document.getElementById("chat-result").innerText = "Internal Error";
        console.error(error);
    }
});

// Enter = click
document.getElementById("chat-input").addEventListener("keypress", (e) => {
    if (e.key === "Enter") {
        document.getElementById("chat-send").click();
    }
});