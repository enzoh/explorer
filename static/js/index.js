let currentEvents = [];
let currentIndex = 0;

/* Utility */
function clearActive(selector) {
    document.querySelectorAll(selector).forEach(el => el.classList.remove("active"));
}

/* Load Dates */
async function loadDates() {
    const res = await fetch('/api/list');
    const dates = await res.json();

    // Sort newest first
    dates.sort((a, b) => b.localeCompare(a));

    const container = document.getElementById('dates');
    container.innerHTML = '';

    dates.forEach(date => {
        const div = document.createElement('div');
        div.textContent = date;
        div.className = 'date-item';
        div.onclick = () => selectDate(date, div);
        container.appendChild(div);
    });

    if (dates.length > 0) {
        container.firstChild.click(); // auto select newest date
    }
}

/* Select Date */
async function selectDate(date, element) {
    clearActive('.date-item');
    element.classList.add('active');

    const res = await fetch(`/api/data?date=${date}`);
    const events = await res.json();

    // Sort newest first
    events.sort((a, b) => b.timestamp.localeCompare(a.timestamp));

    currentEvents = events;
    renderEvents();

    if (events.length > 0) {
        selectEvent(0); // auto select newest event
    } else {
        document.getElementById('viewer').innerHTML = '';
    }
}

/* Render Event Thumbnails */
function renderEvents() {
    const row = document.getElementById('events');
    row.innerHTML = '';

    currentEvents.forEach((event, index) => {
        const img = document.createElement('img');
        img.className = 'event-thumb';

        if (event.file.endsWith('.mp4')) {
            img.src = `/thumbnail/${event.file}`;
        } else {
            img.src = `/data/${event.file}`;
        }

        img.onclick = () => selectEvent(index);
        row.appendChild(img);
    });
}

/* Select Event */
function selectEvent(index) {
    currentIndex = index;

    clearActive('.event-thumb');
    document.querySelectorAll('.event-thumb')[index].classList.add('active');

    const viewer = document.getElementById('viewer');
    viewer.innerHTML = '';

    const event = currentEvents[index];

    if (event.file.endsWith('.mp4')) {
        const video = document.createElement('video');
        video.src = `/data/${event.file}`;
        video.controls = true;
        video.autoplay = true;
        viewer.appendChild(video);
    } else {
        const img = document.createElement('img');
        img.src = `/data/${event.file}`;
        viewer.appendChild(img);
    }

    // Auto-scroll selected thumbnail into view
    document.querySelectorAll('.event-thumb')[index]
        .scrollIntoView({ behavior: "smooth", inline: "center" });
}

/* Keyboard Navigation */
document.addEventListener('keydown', (e) => {
    if (!currentEvents.length) return;

    if (e.key === 'ArrowRight' && currentIndex < currentEvents.length - 1) {
        selectEvent(currentIndex + 1);
    }

    if (e.key === 'ArrowLeft' && currentIndex > 0) {
        selectEvent(currentIndex - 1);
    }
});

/* Initialize */
loadDates();
