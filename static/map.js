// --- Leaflet Map ---
var map = L.map('map', { zoomControl: window.innerWidth > 1024 }).setView([37.77,-122.41], 12);

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution:'© OpenStreetMap'
}).addTo(map);

// Ocultar zoom en mobile
if (window.innerWidth <= 1024) {
    document.querySelectorAll(".leaflet-control-zoom").forEach(ctrl => ctrl.style.display = "none");
}

// Marker cluster
const stopsLayer = L.markerClusterGroup();
map.addLayer(stopsLayer);

let originMarker = null
let destMarker = null

function clearRouteMarkers(map) {

    if (originMarker) {
        map.removeLayer(originMarker)
        originMarker = null
    }

    if (destMarker) {
        map.removeLayer(destMarker)
        destMarker = null
    }

}

function formatDuration(seconds) {
    seconds = Math.floor(seconds)

    const hours = Math.floor(seconds / 3600)
    const minutes = Math.floor((seconds % 3600) / 60)

    if (hours > 0) {
        return `${hours} h ${minutes} min`
    }

    return `${minutes} min`
}


function markRouteStops(map, originLat, originLon, destLat, destLon) {

    const blueIcon = new L.Icon({
        iconUrl: "https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-blue.png",
        shadowUrl: "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-shadow.png",
        iconSize: [25,41],
        iconAnchor: [12,41]
    })

    const redIcon = new L.Icon({
        iconUrl: "https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-red.png",
        shadowUrl: "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-shadow.png",
        iconSize: [25,41],
        iconAnchor: [12,41]
    })

    originMarker = L.marker([originLat, originLon], {icon: blueIcon}).addTo(map)
    destMarker = L.marker([destLat, destLon], {icon: redIcon}).addTo(map)

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

// --- Chat ---
const chatSend = document.getElementById("chat-send");
const chatInput = document.getElementById("chat-input");
const chatResult = document.getElementById("chat-result");

chatSend.addEventListener("click", async () => {
    clearRouteMarkers(map)
    document.getElementById("chat-result").innerHTML = `
    <p>Searching direct trip...</p>
    <div class="spinner"></div>`;
    let address = document.getElementById("chat-input").value.trim();
    if (!address) return alert("Enter your destination");
    try {
        //Search direct trip
        let response = await fetch(`/direct-trip?address=${encodeURIComponent(address)}`);
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
        } else {
            if (data["status"] == "Found"){
                trip_details = data["details"]
                document.getElementById("chat-result").innerHTML = `
                <p>If you want to go to: ${address}</p>
                <p>You should take the transport : <b>${trip_details.route_long_name}</b></p>
                <p>The next transport will arrive at "${trip_details.stop_name_origin}" stop in ${formatDuration(trip_details.wait_time)}</p>
                <p>Your trip will last approximately ${formatDuration(trip_details.total_time)}</p>
                `;
                // Centrar mapa en la parada más cercana
                map.setView([trip_details.stop_lat_origin, trip_details.stop_lon_origin], 15);
                document.getElementById("chat-input").value = "";
                /*
                markClosestStop({
                    "stop_name":trip_details.stop_name_origin,
                    "stop_lat":trip_details.stop_lat_origin,
                    "stop_lon":trip_details.stop_lon_origin
                });
                */

                markRouteStops(map, trip_details.stop_lat_origin, trip_details.stop_lon_origin, trip_details.stop_lat_dest, trip_details.stop_lon_dest)
            } else if(data["status"] == "Canceled") {
                document.getElementById("chat-result").innerHTML = `
                <p>${data.reason}</p>
                `;
                markRouteStops(map, data["origin_coords"][1], data["origin_coords"][0], data["dest_coords"][1], data["dest_coords"][0])
            } else {
                //Search transfer trip
                document.getElementById("chat-result").innerHTML = `
                <p>Searching transfer trip...</p>
                <div class="spinner"></div>`;
                address = document.getElementById("chat-input").value.trim();
                response = await fetch(`/transfer-trip?address=${encodeURIComponent(address)}`);
                if (!response.ok) {
                    errData = await response.json();
                    document.getElementById("chat-result").innerText = errData.error || "Unknown error";
                    return;
                }
                data = await response.json();
                if ("error" in data){
                    document.getElementById("chat-result").innerHTML = `
                    <p>Error: <b>${data.error}</b></p>
                    `;
                } else {
                    if (data["status"] == "Found"){
                        trip_details = data["details"]
                        console.log(trip_details)
                        document.getElementById("chat-result").innerHTML = `
                        <p>We found it! Yaaay</p>
                        `;
                    } else {
                        document.getElementById("chat-result").innerHTML = `
                        <p>${data.reason}</p>
                        `;
                        markRouteStops(map, data["origin_coords"][1], data["origin_coords"][0], data["dest_coords"][1], data["dest_coords"][0])                        
                    }
                }
            }
        }
        
    } catch (error) {
        document.getElementById("chat-result").innerText = "Internal Error";
        console.error(error);
    }
});

// Enter key
chatInput.addEventListener("keypress", e => { if (e.key === "Enter") chatSend.click(); });