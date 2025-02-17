/**
 * @typedef {object} CanvasState
 * @property {Map<string, Uint8Array | 0>} groupMap
 * @property {CanvasRenderingContext2D} ctx
 * @property {number} centerX
 * @property {number} centerY
 */

/**
 * @typedef {object} ConnState
 * @property {WebSocket} socket
 * @property {number} connectTries
 */

const GROUP_DIM = 100;

/**
 * @param group {Uint8Array}
 * @param canvas {CanvasState}
 */
function drawGroupToCanvas(group, canvas) {
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
 * @param socket {ConnState}
 * @param x {number}
 * @param y {number}
 */
function drawGroup(canvas, socket, x, y) {
    const key = `${x},${y}`;
    const group = canvas.groupMap.get(key);
    if (group === undefined) {
        // group doesn't exist? let's signal the backend to get it for us
        canvas.groupMap.set(key, 0);
        sendGetGroupMsg(socket, x, y);
    } else if (group instanceof Uint8Array) {
        // we have the group? attempt to draw it on the canvas
        drawGroupToCanvas(group, canvas);
    }
    // otherwise we've already requested a group and the backend has not responded yet, just do nothing
}

/**
 * @param conn {ConnState}
 * @param e {MessageEvent<any>}
 */
function handleMessage(conn, e) {
    if (e.data instanceof ArrayBuffer) {
        const buffer = e.data;
        const array32 = new Uint32Array(buffer);

        const type = array32[0];
        if (type === 0) {
            const x = array32[1];
            const y = array32[2];
            const group = new Uint8Array(buffer).subarray(12);
        } else {
            throw new Error("Unknown binary data type " + type);
        }
    } else if (e.data instanceof String) {
        const data = JSON.parse(e.data);
        const drawEvent = data["DrawEvent"];
        const err = data["Err"];

        if (drawEvent) {

        } else if (err) {

        } else {
            throw new Error("Text data should contain TileInfo or DrawEvent field");
        }
    } else {
        throw new Error("Message data should be a String (text) or an ArrayBuffer (binary)");
    }
}

/**
 * @param conn {ConnState}
 */
function tryConnect(conn) {
    setTimeout(function() {
        conn.socket = new WebSocket("ws://localhost/canvas");
        conn.socket.addEventListener("open", function(e) {
            conn.connectTries = 0;
        });
        conn.socket.addEventListener("message", function(e) {
            handleMessage(conn, e);
        });
        conn.socket.addEventListener("error", function(e) {
            conn.connectTries += 1;
            tryConnect(conn);
        });
        conn.socket.addEventListener("close", function(e) {
            conn.connectTries += 1;
            tryConnect(conn);
        });
    }, conn.connectTries * 2);
}

/**
 * @param conn {ConnState}
 * @param x {number}
 * @param y {number}
 */
function sendGetGroupMsg(conn, x, y) {
    const msg = JSON.stringify({ "GetGroup": [x, y] });
    conn.socket.send(msg);
}

/**
 * @param conn {ConnState}
 * @param x {number}
 * @param y {number}
 */
function sendGetTileInfoMsg(conn, x, y) {
    const msg = JSON.stringify({ "GetTileInfo": [x, y] });
    conn.socket.send(msg);
}

/**
 * @param conn {ConnState}
 * @param x {number}
 * @param y {number}
 * @param rgb {number[]}
 */
function sendDrawTileMsg(conn, x, y, rgb) {
    const msg = JSON.stringify({ "DrawTile": { x, y, rgb } });
    conn.socket.send(msg);
}