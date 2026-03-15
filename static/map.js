var map = L.map('map').setView([37.77,-122.41], 12);

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution:'© OpenStreetMap'
}).addTo(map);


// capa de clusters
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



async function loadStopsInView() {
    const bounds = map.getBounds();
    const response = await fetch(`/stops?lat_min=${bounds.getSouthWest().lat}&lon_min=${bounds.getSouthWest().lng}&lat_max=${bounds.getNorthEast().lat}&lon_max=${bounds.getNorthEast().lng}`);
    const stops = await response.json();

    stopsLayer.clearLayers(); // limpiamos los markers antiguos
    stops.forEach(stop => {
        const marker = L.marker([stop.stop_lat, stop.stop_lon])
            .bindPopup(`<b>${stop.stop_name}</b>`);
        stopsLayer.addLayer(marker);
    });
}

// cargar la vista inicial
//loadStopsInView();

// recargar cuando el usuario mueve o hace zoom
//map.on('moveend', loadStopsInView);



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


window.addEventListener("resize", () => {

    const isMobileLayout = window.innerWidth <= 768

    if (!isMobileLayout) {

        // reset estilos que modificamos al arrastrar
        mapContainer.style.height = ""
        sidebarPanel.style.height = ""

        // importante para Leaflet
        map.invalidateSize()

    }

})

const dragMargin = 1
const handleHeight = 25

function startResize() {
    isResizingLayout = true
    document.body.classList.add("dragging")
}

function stopResize() {
    isResizingLayout = false
    document.body.classList.remove("dragging")
}

let isResizingLayout = false

const resizeHandle = document.getElementById("drag-handle")
const sidebarPanel = document.getElementById("sidebar")
const mapContainer = document.getElementById("map")

resizeHandle.addEventListener("mousedown", startResize)
resizeHandle.addEventListener("touchstart", startResize)

document.addEventListener("mouseup", stopResize)
document.addEventListener("mouseleave", stopResize)
document.addEventListener("touchend", stopResize)

document.addEventListener("mousemove", (event) => {

    if (!isResizingLayout) return

    const screenHeight = document.documentElement.clientHeight
    let pointerY = event.clientY

    const collapseThreshold = screenHeight - 60

    if (pointerY > collapseThreshold) {

        // Colapsar sidebar
        mapContainer.style.height = screenHeight - handleHeight + "px"
        sidebarPanel.style.height = "0px"

        map.invalidateSize()
        return
    }

    const topLimit = dragMargin
    const bottomLimit = screenHeight - (handleHeight + dragMargin)

    if (pointerY < topLimit) pointerY = topLimit
    if (pointerY > bottomLimit) pointerY = bottomLimit

    const newMapHeight = pointerY
    const newSidebarHeight = screenHeight - pointerY - handleHeight

    mapContainer.style.height = newMapHeight + "px"
    sidebarPanel.style.height = newSidebarHeight + "px"

    map.invalidateSize()

})

document.addEventListener("touchmove", (event) => {
    if (!isResizingLayout) return;

    event.preventDefault();

    const screenHeight = document.documentElement.clientHeight;
    let pointerY = Math.round(event.touches[0].clientY)

    const topLimit = dragMargin; // no subir más arriba del margen
    const bottomLimit = screenHeight - handleHeight - dragMargin; // no bajar más abajo

    // limitar el pointer dentro de los límites
    //if (pointerY < topLimit) pointerY = topLimit;
    //if (pointerY > bottomLimit) pointerY = bottomLimit;

    // umbral de colapso del sidebar
    const collapseThresholdTop = topLimit + 1
    const collapseThresholdBottom = screenHeight - handleHeight - dragMargin

    if (pointerY <= collapseThresholdTop) {
        //alert("Se alcanzó el límite inferior: " + pointerY + " <= " + collapseThresholdTop);
        pointerY = collapseThresholdTop
        // Colapsar sidebar hacia arriba
        sidebarPanel.style.height = screenHeight - handleHeight + "px"
        mapContainer.style.height = handleHeight + "px"
        resizeHandle.style.position = "absolute"
        resizeHandle.style.top = "0px"
        resizeHandle.style.bottom = "" // limpiar bottom
        map.invalidateSize()
        return
    }

    if (pointerY >= collapseThresholdBottom) {
        //alert("Se alcanzó el límite inferior: " + pointerY + " >= " + collapseThresholdBottom);
        pointerY = collapseThresholdBottom
        // Colapsar sidebar hacia abajo
        sidebarPanel.style.height = "0px"
        mapContainer.style.height = screenHeight - handleHeight + "px"
        resizeHandle.style.position = "absolute"
        resizeHandle.style.bottom = "0px"
        resizeHandle.style.top = "" // limpiar top
        map.invalidateSize()
        return
    }

    // Ajuste normal dentro de límites
    if (pointerY < topLimit) pointerY = topLimit
    if (pointerY > bottomLimit) pointerY = bottomLimit

    sidebarPanel.style.height = screenHeight - pointerY + "px"
    mapContainer.style.height = pointerY + "px"

    map.invalidateSize()
});


const dragHandle = document.getElementById("drag-handle")
dragHandle.addEventListener("click", () => {

    const screenHeight = document.documentElement.clientHeight

    mapContainer.style.height = screenHeight * 0.55 + "px"
    sidebarPanel.style.height = screenHeight * 0.45 + "px"

    map.invalidateSize()

})


const closestIcon = L.icon({
    iconUrl: '/static/blue-pin.png', // ruta a tu imagen de pin azul
    iconSize: [25, 41], // tamaño del pin
    iconAnchor: [12, 41], // punto del pin que indica la ubicación
    popupAnchor: [0, -41] // posición del popup respecto al pin
});


async function markClosestStop(data) {
    const lat = data.stop_lat;
    const lon = data.stop_lon;
    const stopName = data.stop_name;

    // Si el marcador ya existe, lo movemos
    if (window.closestMarker) {
        window.closestMarker.setLatLng([lat, lon])
            .setPopupContent(`<b>${stopName}</b>`)
            .openPopup();
    } else {
        // Crear un nuevo marcador
        window.closestMarker = L.marker([lat, lon], { icon: closestIcon })
            .addTo(map)
            .bindPopup(`<b>${stopName}</b>`)
            .openPopup();
    }

    // Centrar mapa en la parada
    map.setView([lat, lon], 15);
}


// --- Código del chat flotante ---
document.getElementById("chat-send").addEventListener("click", async () => {
    clearRouteMarkers(map)
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
            document.getElementById("chat-result").innerHTML = `
            <p>Error: <b>${data.error}</b></p>
            `;
        } else {
            if (data["status"] == "Found"){
                trip_details = data["details"]
                document.getElementById("chat-result").innerHTML = `
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

// Permitir enviar con Enter
document.getElementById("chat-input").addEventListener("keypress", (e) => {
    if (e.key === "Enter") {
        document.getElementById("chat-send").click();
    }
});