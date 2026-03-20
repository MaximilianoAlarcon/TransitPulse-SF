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

function getRouteInfo(routeType) {
    const map = {
        0: {
            label: "Tranvía",
            key: "tram",
            color: "#f39c12",
            icon: "🚋"
        },
        1: {
            label: "Metro",
            key: "metro",
            color: "#e74c3c",
            icon: "🚇"
        },
        2: {
            label: "Tren",
            key: "train",
            color: "#3498db",
            icon: "🚆"
        },
        3: {
            label: "Bus",
            key: "bus",
            color: "#27ae60",
            icon: "🚌"
        },
        4: {
            label: "Ferry",
            key: "ferry",
            color: "#00BFFF",
            icon: "⛴️"
        },
        5: {
            label: "Cable Car",
            key: "cable",
            color: "#8e44ad",
            icon: "🚠"
        },
        6: {
            label: "Góndola",
            key: "gondola",
            color: "#16a085",
            icon: "🚡"
        },
        7: {
            label: "Funicular",
            key: "funicular",
            color: "#2c3e50",
            icon: "🚞"
        }
    };

    return map[routeType] || {
        label: "Transporte",
        key: "other",
        color: "#7f8c8d",
        icon: "❓"
    };
}

async function getWalkingRoute(lat1, lon1, lat2, lon2) {
    const url = `https://router.project-osrm.org/route/v1/foot/${lon1},${lat1};${lon2},${lat2}?overview=full&geometries=geojson`;

    const response = await fetch(url);
    const data = await response.json();

    // convertir [lon, lat] → [lat, lon] (Leaflet lo necesita así)
    const coords = data.routes[0].geometry.coordinates.map(coord => [coord[1], coord[0]]);

    return coords;
}

async function drawWalkingRoute(map, lat1, lon1, lat2, lon2) {

    const coords = await getWalkingRoute(lat1, lon1, lat2, lon2);

    L.polyline(coords, {
        color: "#00BFFF",
        weight: 4,
        dashArray: "5,10"
    }).addTo(map);
}

function markRouteStops(map, originLat, originLon, destLat, destLon, originColor = "#000000", destColor = "#000000") {

    originMarker = L.circleMarker([originLat, originLon], {
        radius: 8,
        color: originColor,
        weight: 2,
        fillColor: originColor,
        fillOpacity: 0.8
    }).addTo(map);

    destMarker = L.circleMarker([destLat, destLon], {
        radius: 8,
        color: destColor,
        weight: 2,
        fillColor: destColor,
        fillOpacity: 0.8
    }).addTo(map);
}


function drawShapeRoute(map, coords, options = {}, defaultColor = "#000000") {

    const {
        color = defaultColor,
        weight = 5,
        opacity = 0.9
    } = options;

    return L.polyline(coords, {
        color: color,
        weight: weight,
        opacity: opacity,
        lineCap: "round",
        lineJoin: "round"
    }).addTo(map);
}


function drawStopsRoute(map, coords, options = {}, defaultColor = "#000000") {

    const {
        color = defaultColor,
        weight = 4,
        dashArray = "6, 10",
        opacity = 0.9,
        showMarkers = true
    } = options;

    const polyline = L.polyline(coords, {
        color: color,
        weight: weight,
        dashArray: dashArray,
        opacity: opacity,
        lineCap: "round"
    }).addTo(map);

    // Opcional: dibujar puntos de parada
    let markers = [];

    if (showMarkers) {
        markers = coords.map(([lat, lon]) => 
            L.circleMarker([lat, lon], {
                radius: 4,
                color: color,
                fillColor: color,
                fillOpacity: 0.8
            }).addTo(map)
        );
    }

    return {
        polyline,
        markers
    };
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
                transport_desc = getRouteInfo(trip_details.route_type)
                document.getElementById("chat-result").innerHTML = `
                <p>If you want to go to: ${address}</p>
                <p>You should take the ${transport_desc["key"]} ${transport_desc["icon"]}: <b>${trip_details.route_long_name}</b></p>
                <p>The next transport will arrive at "${trip_details.stop_name_origin}" stop in ${formatDuration(trip_details.wait_time)}</p>
                <p>Your trip will last approximately ${formatDuration(trip_details.total_time)}</p>
                `;
                map.setView([trip_details.stop_lat_origin, trip_details.stop_lon_origin], 15);
                document.getElementById("chat-input").value = "";
                markRouteStops(
                    map, 
                    trip_details.stop_lat_origin, 
                    trip_details.stop_lon_origin, 
                    trip_details.stop_lat_dest, 
                    trip_details.stop_lon_dest,
                    transport_desc["color"],
                    transport_desc["color"]
                )
                if (trip_details["trip_geometry"]["geometry_type"] == "shape"){
                    drawShapeRoute(map, trip_details["trip_geometry"]["coordinates"], defaultColor = transport_desc["color"])
                } else if (trip_details["trip_geometry"]["geometry_type"] == "stops"){
                    drawStopsRoute(map, trip_details["trip_geometry"]["coordinates"], defaultColor = transport_desc["color"])
                }
            } else if(data["status"] == "Canceled") {
                document.getElementById("chat-result").innerHTML = `
                <p>${data.reason}</p>
                `;
                markRouteStops(
                    map, 
                    data["origin_coords"][1], 
                    data["origin_coords"][0], 
                    data["dest_coords"][1], 
                    data["dest_coords"][0],
                    "#2a93ee",
                    "#2a93ee"
                )
                drawWalkingRoute(map,data["origin_coords"][1],data["origin_coords"][0],data["dest_coords"][1],data["dest_coords"][0])
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