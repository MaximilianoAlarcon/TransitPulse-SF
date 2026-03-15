// --- Inicializar Split.js ---
Split(['#map', '#sidebar'], {
    direction: 'vertical',
    sizes: [60, 40],      // porcentaje inicial
    minSize: [100, 100],  // altura mínima de cada panel
    gutterSize: 8,
    cursor: 'ns-resize'
});

// --- Inicializar mapa ---
var map = L.map('map', { zoomControl: window.innerWidth > 1024 }).setView([37.77,-122.41], 12);

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution:'© OpenStreetMap'
}).addTo(map);

// Ocultar zoom en mobile
if (window.innerWidth <= 1024) {
    const zoomControls = document.querySelectorAll(".leaflet-control-zoom");
    zoomControls.forEach(ctrl => ctrl.style.display = "none");
}



// --- Split.js adaptable desktop/mobile ---
let splitInstance;

function initSplit() {
    const isMobile = window.innerWidth <= 1024;

    // Si ya hay un split, lo destruimos
    if(splitInstance) splitInstance.destroy();

    splitInstance = Split(['#sidebar','#map'], {
        direction: isMobile ? 'vertical' : 'horizontal',
        sizes: isMobile ? [40,60] : [25,75],  // % iniciales
        minSize: [100,100],
        gutterSize: 8,
        cursor: isMobile ? 'ns-resize' : 'ew-resize'
    });
}

// Inicializamos
initSplit();

// Reiniciar al cambiar tamaño de ventana
window.addEventListener('resize', () => {
    initSplit();
    map.invalidateSize();
});




// Marker Cluster
const stopsLayer = L.markerClusterGroup();
map.addLayer(stopsLayer);

let originMarker = null;
let destMarker = null;

function clearRouteMarkers(map) {
    if(originMarker){ map.removeLayer(originMarker); originMarker=null; }
    if(destMarker){ map.removeLayer(destMarker); destMarker=null; }
}

function formatDuration(seconds) {
    seconds = Math.floor(seconds);
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    return hours>0 ? `${hours} h ${minutes} min` : `${minutes} min`;
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

async function loadStopsInView() {
    const bounds = map.getBounds();
    const response = await fetch(`/stops?lat_min=${bounds.getSouthWest().lat}&lon_min=${bounds.getSouthWest().lng}&lat_max=${bounds.getNorthEast().lat}&lon_max=${bounds.getNorthEast().lng}`);
    const stops = await response.json();

    stopsLayer.clearLayers();
    stops.forEach(stop => {
        const marker = L.marker([stop.stop_lat, stop.stop_lon])
            .bindPopup(`<b>${stop.stop_name}</b>`);
        stopsLayer.addLayer(marker);
    });
}

// Geolocalización usuario
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

// --- Chat flotante ---
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
                    <p>You should take the transport: <b>${trip_details.route_long_name}</b></p>
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

// Enter para enviar
document.getElementById("chat-input").addEventListener("keypress", (e)=>{
    if(e.key==="Enter") document.getElementById("chat-send").click();
});

// --- Ajuste al resize ventana ---
window.addEventListener("resize", ()=>{
    map.invalidateSize();
});