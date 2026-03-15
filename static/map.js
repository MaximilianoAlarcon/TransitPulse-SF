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
    const address = chatInput.value.trim();
    if (!address) return alert("Enter your destination");

    try {
        const resp = await fetch(`/direct-trip?address=${encodeURIComponent(address)}`);
        const data = await resp.json();

        if (!resp.ok || data.error) {
            chatResult.innerHTML = `<p>Error: <b>${data.error || 'Unknown'}</b></p>`;
            return;
        }

        if (data.status === "Found") {
            const details = data.details;
            chatResult.innerHTML = `
                <p>You should take the transport: <b>${details.route_long_name}</b></p>
                <p>Next transport at "${details.stop_name_origin}" in ${Math.floor(details.wait_time/60)} min</p>
                <p>Total trip: ${Math.floor(details.total_time/60)} min</p>
            `;
            chatInput.value = "";
            map.setView([details.stop_lat_origin, details.stop_lon_origin], 15);
        } else {
            chatResult.innerHTML = `<p>Could not find a direct trip</p><p>${data.reason || ""}</p>`;
        }
    } catch (err) {
        console.error(err);
        chatResult.innerText = "Internal Error";
    }
});

// Enter key
chatInput.addEventListener("keypress", e => { if (e.key === "Enter") chatSend.click(); });