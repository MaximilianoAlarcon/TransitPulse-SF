var map = L.map('map').setView([37.77,-122.41], 12);

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution:'© OpenStreetMap'
}).addTo(map);


// capa de clusters
const stopsLayer = L.markerClusterGroup();

map.addLayer(stopsLayer);






async function loadVehicles(){

let response = await fetch("/api/vehicles");
let vehicles = await response.json();

vehicles.forEach(v => {

    L.marker([v.lat, v.lon])
    .addTo(map)
    .bindPopup("Vehicle " + v.id + "<br>Status: " + v.status)

});

}

//loadVehicles();


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
loadStopsInView();

// recargar cuando el usuario mueve o hace zoom
map.on('moveend', loadStopsInView);



async function loadOperators(){

let response = await fetch("/api/operators");
let data = await response.json();

let list = document.getElementById("operators-list");

data.forEach(op => {

let li = document.createElement("li");

li.textContent = op.Name;

list.appendChild(li);

});

}

loadOperators();


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
    const address = document.getElementById("chat-input").value.trim();
    if (!address) return alert("Ingresa un lugar");

    try {
        const response = await fetch(`/closest-stop?address=${encodeURIComponent(address)}`);
        if (!response.ok) {
            const errData = await response.json();
            document.getElementById("chat-result").innerText = errData.error || "Error desconocido";
            return;
        }

        const data = await response.json();
        document.getElementById("chat-result").innerHTML = `
            <p>Parada más cercana: <b>${data.stop_name}</b></p>
            <p>Lat: ${data.stop_lat}, Lon: ${data.stop_lon}</p>
        `;

        // Centrar mapa en la parada más cercana
        map.setView([data.stop_lat, data.stop_lon], 15);

        document.getElementById("chat-input").value = "";

        markClosestStop(data);
    } catch (error) {
        document.getElementById("chat-result").innerText = "Error al consultar la API";
        console.error(error);
    }
});

// Permitir enviar con Enter
document.getElementById("chat-input").addEventListener("keypress", (e) => {
    if (e.key === "Enter") {
        document.getElementById("chat-send").click();
    }
});