var map = L.map('map').setView([37.77,-122.41], 12);

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution:'© OpenStreetMap'
}).addTo(map);

async function loadVehicles(){

let response = await fetch("/api/vehicles");
let vehicles = await response.json();

vehicles.forEach(v => {

    L.marker([v.lat, v.lon])
    .addTo(map)
    .bindPopup("Vehicle " + v.id + "<br>Status: " + v.status)

});

}

loadVehicles();

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