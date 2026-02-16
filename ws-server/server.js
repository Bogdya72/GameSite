"use strict";

const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT || 8787);
const HOST = process.env.HOST || "0.0.0.0";
const PING_INTERVAL_MS = Number(process.env.PING_INTERVAL_MS || 20000);

const rooms = new Map();
const worlds = new Map();

const subscriptionsByKey = new Map();
const socketSubs = new WeakMap();

function isPlainObject(value) {
  return Boolean(value && typeof value === "object" && !Array.isArray(value));
}

function deepClone(value) {
  if (value === undefined) return null;
  return JSON.parse(JSON.stringify(value));
}

function normalizeKind(kind) {
  return String(kind || "").toLowerCase() === "world" ? "world" : "room";
}

function normalizeRoomId(roomId) {
  return String(roomId || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9_-]/g, "")
    .slice(0, 32);
}

function getStore(kind) {
  return kind === "world" ? worlds : rooms;
}

function getSubscriptionKey(kind, roomId) {
  return `${kind}:${roomId}`;
}

function readState(kind, roomId) {
  const store = getStore(kind);
  return deepClone(store.get(roomId) ?? null);
}

function setByPath(target, path, value) {
  const parts = String(path || "")
    .split("/")
    .map((part) => part.trim())
    .filter(Boolean);
  if (parts.length === 0) return;

  let cursor = target;
  for (let i = 0; i < parts.length - 1; i += 1) {
    const key = parts[i];
    if (!isPlainObject(cursor[key])) {
      cursor[key] = {};
    }
    cursor = cursor[key];
  }

  const last = parts[parts.length - 1];
  if (value === null) {
    delete cursor[last];
  } else {
    cursor[last] = value;
  }
}

function writeSet(kind, roomId, value) {
  const store = getStore(kind);
  if (value === null || value === undefined) {
    store.delete(roomId);
    return;
  }
  store.set(roomId, deepClone(value));
}

function writeUpdate(kind, roomId, patch) {
  if (!isPlainObject(patch)) {
    throw new Error("invalid-update-payload");
  }

  const store = getStore(kind);
  const current = isPlainObject(store.get(roomId)) ? deepClone(store.get(roomId)) : {};

  for (const [key, value] of Object.entries(patch)) {
    if (key.includes("/")) {
      setByPath(current, key, value);
      continue;
    }
    if (value === null) {
      delete current[key];
    } else {
      current[key] = value;
    }
  }

  if (Object.keys(current).length === 0) {
    store.delete(roomId);
  } else {
    store.set(roomId, current);
  }
}

function safeSend(ws, message) {
  if (!ws || ws.readyState !== ws.OPEN) return;
  try {
    ws.send(JSON.stringify(message));
  } catch (error) {}
}

function getSocketSubMap(ws) {
  let map = socketSubs.get(ws);
  if (!map) {
    map = new Map();
    socketSubs.set(ws, map);
  }
  return map;
}

function unsubscribeSocket(ws, sid) {
  const map = socketSubs.get(ws);
  if (!map) return;
  const sub = map.get(sid);
  if (!sub) return;
  map.delete(sid);

  const key = getSubscriptionKey(sub.kind, sub.roomId);
  const listeners = subscriptionsByKey.get(key);
  if (!listeners) return;
  listeners.delete(sid);
  if (listeners.size === 0) {
    subscriptionsByKey.delete(key);
  }
}

function subscribeSocket(ws, sid, kind, roomId) {
  unsubscribeSocket(ws, sid);
  const key = getSubscriptionKey(kind, roomId);
  let listeners = subscriptionsByKey.get(key);
  if (!listeners) {
    listeners = new Map();
    subscriptionsByKey.set(key, listeners);
  }
  listeners.set(sid, ws);
  getSocketSubMap(ws).set(sid, { kind, roomId });
}

function cleanupSocket(ws) {
  const map = socketSubs.get(ws);
  if (!map) return;
  for (const sid of map.keys()) {
    unsubscribeSocket(ws, sid);
  }
  socketSubs.delete(ws);
}

function broadcastSnapshot(kind, roomId) {
  const key = getSubscriptionKey(kind, roomId);
  const listeners = subscriptionsByKey.get(key);
  if (!listeners || listeners.size === 0) return;

  const data = readState(kind, roomId);
  for (const [sid, ws] of listeners.entries()) {
    safeSend(ws, {
      type: "snapshot",
      sid,
      data,
    });
  }
}

function sendResponse(ws, rid, ok, data = null, error = "") {
  if (String(rid || "").startsWith("ff-")) {
    return;
  }
  safeSend(ws, {
    type: "response",
    rid,
    ok,
    data,
    error,
  });
}

function handleRequest(ws, message) {
  const rid = String(message?.rid || "");
  if (!rid) return;

  const action = String(message?.action || "").toLowerCase();
  const kind = normalizeKind(message?.kind);
  const roomId = normalizeRoomId(message?.roomId);
  const payload = message?.payload;

  if (!roomId) {
    sendResponse(ws, rid, false, null, "invalid-room-id");
    return;
  }

  try {
    if (action === "once") {
      sendResponse(ws, rid, true, readState(kind, roomId));
      return;
    }

    if (action === "subscribe") {
      const sid = String(payload?.sid || "");
      if (!sid) throw new Error("missing-sub-id");
      subscribeSocket(ws, sid, kind, roomId);
      const data = readState(kind, roomId);
      sendResponse(ws, rid, true, data);
      safeSend(ws, {
        type: "snapshot",
        sid,
        data,
      });
      return;
    }

    if (action === "unsubscribe") {
      const sid = String(payload?.sid || "");
      if (!sid) throw new Error("missing-sub-id");
      unsubscribeSocket(ws, sid);
      sendResponse(ws, rid, true, null);
      return;
    }

    if (action === "set") {
      writeSet(kind, roomId, payload);
      broadcastSnapshot(kind, roomId);
      sendResponse(ws, rid, true, null);
      return;
    }

    if (action === "update") {
      writeUpdate(kind, roomId, payload);
      broadcastSnapshot(kind, roomId);
      sendResponse(ws, rid, true, null);
      return;
    }

    if (action === "remove") {
      writeSet(kind, roomId, null);
      broadcastSnapshot(kind, roomId);
      sendResponse(ws, rid, true, null);
      return;
    }

    sendResponse(ws, rid, false, null, "unknown-action");
  } catch (error) {
    sendResponse(ws, rid, false, null, String(error?.message || error || "server-error"));
  }
}

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    const payload = {
      ok: true,
      uptimeSec: Math.floor(process.uptime()),
      rooms: rooms.size,
      worlds: worlds.size,
    };
    res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
    res.end(JSON.stringify(payload));
    return;
  }

  res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("Zombie Surge WS server is running.");
});

const wss = new WebSocketServer({ server });

wss.on("connection", (ws) => {
  ws.isAlive = true;

  ws.on("pong", () => {
    ws.isAlive = true;
  });

  ws.on("message", (raw) => {
    let message = null;
    try {
      message = JSON.parse(String(raw || "{}"));
    } catch (error) {
      return;
    }
    if (!message || typeof message !== "object" || message.type !== "request") {
      return;
    }
    handleRequest(ws, message);
  });

  ws.on("close", () => {
    cleanupSocket(ws);
  });

  ws.on("error", () => {
    cleanupSocket(ws);
  });
});

const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      try {
        ws.terminate();
      } catch (error) {}
      continue;
    }
    ws.isAlive = false;
    try {
      ws.ping();
    } catch (error) {}
  }
}, PING_INTERVAL_MS);

wss.on("close", () => {
  clearInterval(heartbeat);
});

server.listen(PORT, HOST, () => {
  console.log(`[ws-server] listening on ${HOST}:${PORT}`);
});
