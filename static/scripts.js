/**
 * @typedef {object} CanvasState
 * @property {Map<string, Uint8Array | 0>} groupMap
 * @property {CanvasRenderingContext2D} ctx
 * @property {number} centerX
 * @property {number} centerY
 */

const GROUP_DIM = 100;

/**
 * @param group {Uint8Array}
 * @param canvas {CanvasState}
 */
function drawGroupToCanvas(canvas, group) {
    for (let x  = 0; x < GROUP_DIM; x++) {
        for (let y  = 0; y < GROUP_DIM; y++) {
            const offset = (y * 3 * GROUP_DIM) + (x * 3)
            const o1 = offset + 1;
            const o2 = offset + 2;

            const r = group[offset];
            const g = group[o1];
            const b = group[o2];

            canvas.ctx.fillStyle = `rgb(${r}, ${g}, ${b})`;
            // state.ctx.fillRect();
        }
    }
}

/**
 * @param canvas {CanvasState}
 * @param x {number}
 * @param y {number}
 */
function drawGroup(canvas, x, y) {
    const key = `${x},${y}`;
    const group = canvas.groupMap.get(key);
    if (group === undefined) {
        canvas.groupMap.set(key, 0);
        getGroup(x, y)
            .then((resp) => {
                if (resp instanceof Uint8Array) {
                    canvas.groupMap.set(key, resp);
                    drawGroupToCanvas(canvas, resp);
                } else {

                }
            })
    } else if (group instanceof Uint8Array) {
        drawGroupToCanvas(canvas, group);
    } else {
        console.debug("Attempted to draw group currently in the fetch state");
    }
}

function listenCanvas() {
    const source = new EventSource("/canvas/sse");

    source.onmessage = function (event) {
        if (event.data !== "keep-alive") {
            const draw = JSON.parse(event.data);
        }
    };

    source.onerror = function() {
        console.error("Canvas server side event was closed.");
        source.close();
    };
}

const URL = "http://localhost:3000";

/**
 * @typedef {object} Tile
 * @property {number} x
 * @property {number} y
 * @property {number[]} rgb
 * @property {string} date
 */

/**
 * @param x {number}
 * @param y {number}
 * @return {Promise<Tile | string>}
 */
async function getTile(x, y) {
    try {
        const resp = await fetch(`${URL}/tile?x=${x}&y=${y}`, { method: "GET" })
        if (resp.ok) {
            return await resp.json();
        } else {
            const text = await resp.text();
            console.error(text);
            return text;
        }
    } catch (err) {
        console.error(err);
        return "Unexpected error has occurred";
    }
}

/**
 * @typedef {object} DrawEvent
 * @property {number} x
 * @property {number} y
 * @property {number[]} rgb
 */

/**
 * @param draw {DrawEvent}
 * @return {Promise<string | 0>}
 */
async function postTile(draw) {
    try {
        const resp = await fetch(`${URL}/tile`, { method: "POST", body: JSON.stringify(draw) })
        if (resp.ok) {
            const text = await resp.text();
            console.log(text);
            return 0;
        } else {
            const text = await resp.text();
            console.error(text);
            return text;
        }
    } catch (err) {
        console.error(err);
        return "Unexpected error has occurred";
    }
}

/**
 * @param x {number}
 * @param y {number}
 * @return {Promise<Uint8Array | string>}
 */
async function getGroup(x, y) {
    try {
        const resp = await fetch(`${URL}/group?x=${x}&y=${y}`, { method: "GET" })
        if (resp.ok) {
            const buffer = await resp.arrayBuffer();
            return new Uint8Array(buffer);
        } else {
            const text = await resp.text();
            console.error(text);
            return text;
        }
    } catch (err) {
        console.error(err);
        return "Unexpected error has occurred";
    }
}