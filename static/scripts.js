const GROUP_DIM = 100;

function drawInitialCanvas() {
    const canvas = document.getElementById("canvas");
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
        getGroup(x, y)
            .then((resp) => {
                if (resp instanceof Uint8Array) {
                    canvas.groupMap.set(key, resp);
                    drawGroupToCanvas(canvas, resp);
                } else if (resp instanceof String) {

                } else {
                    console.error("Expected resp to get Uint8Array or String, got ", typeof resp);
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

function startListenCanvas(canvas) {
    const source = new EventSource("/canvas/sse");

    source.onmessage = function (event) {
        if (event.data !== "keep-alive") {
            const draw = JSON.parse(event.data);
            updateGroup(canvas, draw);
            drawTile(canvas, draw.x, draw.y, draw.rgb);
        }
    };

    source.onerror = function() {
        console.error("Canvas server side event was closed.");
        source.close();
    };
}

const URL = "http://localhost:3000";

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