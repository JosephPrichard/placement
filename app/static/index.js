const GROUP_DIM = 100;

function init() {
    const canvas = document.getElementById("canvas");
    const ctx = canvas.getContext("2d");

    canvas.width = window.innerWidth;
    canvas.height = window.innerHeight;

    for (let x = 0; x < 1000; x++) {
        for (let y = 0; y < 1000; y++) {

        }
    }
}

function drawTile(canvas, x, y, rgb) {

}

function drawGroupToCanvas(canvas, group) {
    if (group.length === 0) {
        return;
    }

    for (let x  = 0; x < GROUP_DIM; x++) {
        for (let y  = 0; y < GROUP_DIM; y++) {
            const offset = (y * 3 * GROUP_DIM) + (x * 3)
            const o1 = offset + 1;
            const o2 = offset + 2;

            const r = group[offset];
            const g = group[o1];
            const b = group[o2];

            drawTile(canvas, x, y, [r, g, b])
        }
    }
}

function drawGroup(canvas, x, y) {
    const key = `${x},${y}`;
    const group = canvas.groupMap.get(key);
    if (group === undefined) {
        canvas.groupMap.set(key, 0);
        getGroup(x, y).then((resp) => {
            if (resp instanceof Uint8Array) {
                canvas.groupMap.set(key, resp);
                drawGroupToCanvas(canvas, resp);
            }
        })
    } else if (group instanceof Uint8Array) {
        drawGroupToCanvas(canvas, group);
    } else {
        console.debug("Attempted to draw group currently in the fetch state");
    }
}

function updateGroup(canvas, draw) {
    const key = `${draw.x},${draw.y}`;
    const group = canvas.groupMap.get(key);

    const offset = (draw.y * 3 * GROUP_DIM) + (draw.x * 3)
    const o1 = offset + 1;
    const o2 = offset + 2;

    group[offset] = draw.rgb[0];
    group[o1] = draw.rgb[1];
    group[o2] = draw.rgb[2];
}

/**
 * @param {*} canvas 
 */
function startListenCanvas(canvas) {
    const sse = new EventSource("/canvas/sse");

    sse.onmessage = function (event) {
        if (event.data !== "keep-alive") {
            const draw = JSON.parse(event.data);

            updateGroup(canvas, draw);
            drawTile(canvas, draw.x, draw.y, draw.rgb);
        }
    };

    sse.onerror = function() {
        console.error("Canvas server side event was closed.");
        sse.close();
    };
}

/**
 * @param {string} msg
 */
function handleError(msg) {
    console.error(msg);
}

const URL = "http://localhost:3000";

/**
 * @typedef {object} Tile
 * @property {number} x
 * @property {number} y
 * @property {number[]} rgb
 * @property {placementTime} string
 */

/**
 * @param {number} x 
 * @param {number} y  
 * @returns -1 | Tile
 */
async function getTile(x, y) {
    try {
        const resp = await fetch(`${URL}/tile${new URLSearchParams({ x, y })}`, { method: "GET" })
        if (ok) {
            return await resp.json();
        } else {
            handleError((await resp.json()).msg);
        }
    } catch (err) {
        console.error(err);
        handleError("An unexpected error has occurred");
    }
    return -1;
}

/**
 * @param {number} x
 * @param {number} y
 * @param {number[]} rgb
 * @returns 
 */
async function postTile(x, y, rgb) {
    try {
        const resp = await fetch(`${URL}/tile`, { method: "POST", body: JSON.stringify({ x, y, rgb }) })
        if (resp.ok) {
            await resp.text();
            return 0;
        } else {
            handleError((await resp.json()).msg);
        }
    } catch (err) {
        console.error(err);
        handleError("An unexpected error has occurred");
    }
    return -1;
}

/**
 * @param {number} x 
 * @param {number} y 
 * @returns number | Uint8Array
 */
async function getGroup(x, y) {
    try {
        const resp = await fetch(`${URL}/group${new URLSearchParams({ x, y })}`, { method: "GET" })
        if (resp.ok) {
            const buffer = await resp.arrayBuffer();
            return new Uint8Array(buffer);
        } else {
            handleError((await resp.json()).msg);
        }
    } catch (err) {
        console.error(err);
        handleError("An unexpected error has occurred");
    }
    return -1;
}