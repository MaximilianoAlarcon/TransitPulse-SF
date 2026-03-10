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