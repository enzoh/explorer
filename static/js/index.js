;(() => {
    'use strict'

    // Define application state.
    const state = {
        index: -1,
        events: [],
    }

    // Define DOM element cache.
    const dom = {
        dates: document.getElementById('dates'),
        events: document.getElementById('events'),
        viewer: document.getElementById('viewer'),
    }

    // Check if a file is a image.
    function isImage(file) {
        return file.toLowerCase().endsWith('.jpg')
    }

    // Check if a file is a video.
    function isVideo(file) {
        return file.toLowerCase().endsWith('.mp4')
    }

    // Fetch a JSON object.
    function safeFetchJSON(url) {
        return fetch(url).then((response) => {
            if (!response.ok) {
                throw new Error(`Request failed: ${response.status}`)
            }
            return response.json()
        })
    }

    // Select a date.
    async function selectDate(date) {
        try {
            const dateElems = [...dom.dates.querySelectorAll('.date-item')]
            dateElems.forEach((dateElem) => {
                dateElem.classList.remove('active')
            })
            const selected = dateElems.find((candidate) => {
                return candidate.dataset.date === date
            })
            if (selected) {
                selected.classList.add('active')
            }
            const query = `?date=${encodeURIComponent(date)}`
            state.events = await safeFetchJSON(`/api/data${query}`)
            renderThumbnails()
            if (state.events.length > 0) {
                selectEvent(0)
            } else {
                dom.viewer.innerHTML = ''
                state.index = -1
            }
        } catch (err) {
            console.error('Failed to load events:', err)
        }
    }

    // Select an event.
    function selectEvent(i) {
        if (i < 0 || i >= state.events.length) {
            return
        }
        state.index = i
        const eventElems = [...dom.events.querySelectorAll('.event-item')]
        eventElems.forEach((eventElem) => {
            eventElem.classList.remove('active')
        })
        const selected = eventElems[i]
        if (selected) {
            selected.classList.add('active')
            selected.scrollIntoView({
                behavior: 'smooth',
                block: 'nearest',
                inline: 'center',
            })
        }
        renderViewer(state.events[i])
    }

    // Render thumbnails.
    function renderThumbnails() {
        dom.events.innerHTML = ''
        const frag = document.createDocumentFragment()
        state.events.forEach((event, index) => {
            const img = document.createElement('img')
            img.className = 'event-item'
            img.dataset.index = index
            if (isImage(event.file)) {
                img.src = `/data/${event.file}`
            }
            if (isVideo(event.file)) {
                img.src = `/thumbnail/${event.file}`
            }
            frag.appendChild(img)
        })
        dom.events.appendChild(frag)
    }

    // Render either an image or video into the viewer pane.
    function renderViewer(event) {
        // Render an image.
        if (isImage(event.file)) {
            dom.viewer.innerHTML = ''
            const img = document.createElement('img')
            img.src = `/data/${event.file}`
            dom.viewer.appendChild(img)
        }
        // Render a video.
        if (isVideo(event.file)) {
            dom.viewer.innerHTML = ''
            const video = document.createElement('video')
            video.addEventListener('focus', () => video.blur())
            video.autoplay = true
            video.className = 'controls'
            video.controls = true
            video.loop = true
            video.muted = true
            video.setAttribute('tabindex', '-1')
            video.src = `/data/${event.file}`
            dom.viewer.appendChild(video)
        }
    }

    // Handle date selection via event delegation.
    dom.dates.addEventListener('click', (event) => {
        const item = event.target.closest('.date-item')
        if (!item) {
            return
        }
        selectDate(item.dataset.date)
    })

    // Handle thumbnail selection via event delegation.
    dom.events.addEventListener('click', (event) => {
        const item = event.target.closest('.event-item')
        if (!item) {
            return
        }
        selectEvent(Number(item.dataset.index))
    })

    // Enable keyboard navigation and video playback control.
    document.addEventListener('keydown', (event) => {
        if (!state.events.length) {
            return
        }
        const video = dom.viewer.querySelector('video')
        switch (event.code) {
            // Select next event.
            case 'ArrowRight':
                event.preventDefault()
                selectEvent(state.index + 1)
                break
            // Select previous event.
            case 'ArrowLeft':
                event.preventDefault()
                selectEvent(state.index - 1)
                break
            // Play / Pause.
            case 'Space':
                event.preventDefault()
                if (!video) {
                    return
                }
                if (video.paused) {
                    video.play()
                } else {
                    video.pause()
                }
                break
            // Move the playhead a half second backwards.
            case 'KeyJ':
                event.preventDefault()
                video.currentTime = Math.max(video.currentTime - 0.5, 0.01)
                break
            // Move the playhead a half second forwards.
            case 'KeyL':
                event.preventDefault()
                video.currentTime = Math.min(
                    video.currentTime + 0.5,
                    video.duration - 0.01,
                )
                break
        }
    })

    // Entrypoint.
    async function main() {
        try {
            const dates = await safeFetchJSON('/api/list')
            dom.dates.innerHTML = ''
            const frag = document.createDocumentFragment()
            dates.forEach((date) => {
                const div = document.createElement('div')
                div.className = 'date-item'
                div.dataset.date = date
                div.textContent = date
                frag.appendChild(div)
            })
            dom.dates.appendChild(frag)
            if (dates.length > 0) {
                selectDate(dates[0])
            }
        } catch (err) {
            console.error('Failed to load dates:', err)
        }
    }

    // Begin.
    main()
})()
