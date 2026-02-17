"use strict";

const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT || 8787);
const HOST = process.env.HOST || "0.0.0.0";
const PING_INTERVAL_MS = Number(process.env.PING_INTERVAL_MS || 20000);
const SIM_TICK_MS = Number(process.env.SIM_TICK_MS || 33);

const WORLD_WIDTH = Number(process.env.WORLD_WIDTH || 1080);
const WORLD_HEIGHT = Number(process.env.WORLD_HEIGHT || 1620);
const CORE_RADIUS = 16;
const WORLD_EDGE_PAD = 50;
const MAX_ZOMBIES = 24;
const MAX_BULLETS = 42;
const MAX_BURSTS = 26;
const MAX_SHOTS_PER_TICK = 4;

const WEAPONS = {
  blaster: { cooldown: 220, speed: 720, damage: 1, pellets: 1, spread: 0.1 },
  shotgun: { cooldown: 620, speed: 640, damage: 0.6, pellets: 5, spread: 0.65 },
  beam: { cooldown: 0, dps: 6.5, width: 14 },
  grenade: { cooldown: 900, speed: 420, damage: 2.2, pellets: 1, spread: 0.05, splash: 70, life: 0.95 },
};

const ZOMBIE_TYPE_TO_CODE = {
  normal: 0,
  fast: 1,
  tank: 2,
  dash: 3,
  boss: 4,
};

const rooms = new Map();
const worlds = new Map();
const simulations = new Map();

const subscriptionsByKey = new Map();
const socketSubs = new WeakMap();

function isPlainObject(value) {
  return Boolean(value && typeof value === "object" && !Array.isArray(value));
}

function deepClone(value) {
  if (value === undefined) return null;
  return JSON.parse(JSON.stringify(value));
}

function clamp(value, min, max) {
  const n = Number(value);
  if (!Number.isFinite(n)) return min;
  if (n < min) return min;
  if (n > max) return max;
  return n;
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

function sanitizeWeapon(value) {
  const key = String(value || "");
  return WEAPONS[key] ? key : "blaster";
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

function quantizeNorm(value) {
  return Math.round(clamp(value, 0, 1) * 10000) / 10000;
}

function nowMs() {
  return Date.now();
}

function normalizePlayerSlot(slot) {
  if (!isPlainObject(slot) || !slot.uid) return null;
  return {
    uid: String(slot.uid),
    name: String(slot.name || "Игрок").slice(0, 48),
    hp: clamp(slot.hp, 0, 500),
    maxHp: clamp(slot.maxHp, 1, 500),
    alive: slot.alive !== false,
    score: Math.max(0, Math.floor(Number(slot.score) || 0)),
    wave: Math.max(1, Math.floor(Number(slot.wave) || 1)),
    aimX: quantizeNorm(slot.aimX),
    aimY: quantizeNorm(slot.aimY),
    pointerDown: Boolean(slot.pointerDown),
    weapon: sanitizeWeapon(slot.weapon),
    shotSeq: Math.max(0, Math.floor(Number(slot.shotSeq) || 0)),
    inputAt: Math.max(0, Math.floor(Number(slot.inputAt) || nowMs())),
    updatedAt: Math.max(0, Math.floor(Number(slot.updatedAt) || nowMs())),
  };
}

function normalizeRoomForSet(roomId, value) {
  if (!isPlainObject(value)) return null;
  const host = normalizePlayerSlot(value.host);
  const guest = normalizePlayerSlot(value.guest);
  return {
    roomId,
    status: ["waiting", "running", "ended"].includes(String(value.status || ""))
      ? String(value.status)
      : "waiting",
    sharedWave: Math.max(1, Math.floor(Number(value.sharedWave) || 1)),
    createdAt: Math.max(0, Math.floor(Number(value.createdAt) || nowMs())),
    updatedAt: nowMs(),
    endedAt: Math.max(0, Math.floor(Number(value.endedAt) || 0)),
    endedReason: String(value.endedReason || ""),
    host,
    guest,
  };
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

function sanitizeRoomPatch(current, patch) {
  if (!isPlainObject(patch)) return {};
  const running = current && current.status === "running";
  if (!running) {
    return { ...patch };
  }

  const next = {};
  const blockedPathRe = /^(host|guest)\/(hp|alive|score|wave)$/;

  for (const [key, value] of Object.entries(patch)) {
    if (key === "sharedWave") continue;
    if (blockedPathRe.test(key)) continue;

    if ((key === "host" || key === "guest") && isPlainObject(value)) {
      const filtered = {};
      const allowedKeys = [
        "uid",
        "name",
        "maxHp",
        "aimX",
        "aimY",
        "pointerDown",
        "weapon",
        "shotSeq",
        "inputAt",
        "updatedAt",
      ];
      for (const allowed of allowedKeys) {
        if (!(allowed in value)) continue;
        filtered[allowed] = value[allowed];
      }
      if (Object.keys(filtered).length > 0) {
        next[key] = filtered;
      }
      continue;
    }

    next[key] = value;
  }

  return next;
}

function getCoreCoordinates(width, height) {
  const offset = Math.min(110, width * 0.11);
  return {
    host: { x: width / 2 - offset, y: height / 2 + 10 },
    guest: { x: width / 2 + offset, y: height / 2 - 10 },
  };
}

function pickZombieType(wave) {
  if (wave < 3) return "normal";
  const roll = Math.random();
  if (roll < 0.55) return "normal";
  if (roll < 0.75) return "fast";
  if (roll < 0.92) return "tank";
  return "dash";
}

function estimateZombieBaseSpeed(type = "normal", wave = 1) {
  const safeWave = Math.max(1, Number(wave) || 1);
  const base = 38 + safeWave * 6;
  if (type === "fast") return base * 1.6;
  if (type === "tank") return base * 0.65;
  if (type === "dash") return base * 1.15;
  if (type === "boss") return base * 0.55;
  return base + 7;
}

function createZombie(sim, type, x, y) {
  const baseSpeed = 38 + sim.wave * 6;
  let speed = baseSpeed + Math.random() * 14;
  let radius = 16 + Math.random() * 8;
  let hp = 2;

  const zombie = {
    id: sim.nextZombieId++,
    x,
    y,
    r: radius,
    wobble: Math.random() * Math.PI,
    type,
    dashCooldown: 0,
    dashTimer: 0,
    targetCore: Math.random() < 0.4 ? "guest" : "host",
    baseSpeed: speed,
    hp,
    maxHp: hp,
  };

  if (type === "fast") {
    zombie.baseSpeed = baseSpeed * 1.6;
    zombie.r = 12 + Math.random() * 6;
    zombie.hp = 1;
    zombie.maxHp = 1;
  } else if (type === "tank") {
    zombie.baseSpeed = baseSpeed * 0.65;
    zombie.r = 24 + Math.random() * 10;
    zombie.hp = 5;
    zombie.maxHp = 5;
  } else if (type === "dash") {
    zombie.baseSpeed = baseSpeed * 1.15;
    zombie.r = 16 + Math.random() * 6;
    zombie.hp = 2;
    zombie.maxHp = 2;
    zombie.dashCooldown = 1.4 + Math.random() * 1.6;
  } else if (type === "boss") {
    zombie.baseSpeed = baseSpeed * 0.55;
    zombie.r = 36;
    zombie.hp = 22 + sim.wave * 2;
    zombie.maxHp = zombie.hp;
    zombie.targetCore = "host";
  }

  return zombie;
}

function getSpawnPoint(width, height) {
  const edge = Math.floor(Math.random() * 4);
  if (edge === 0) return { x: Math.random() * width, y: -WORLD_EDGE_PAD };
  if (edge === 1) return { x: width + WORLD_EDGE_PAD, y: Math.random() * height };
  if (edge === 2) return { x: Math.random() * width, y: height + WORLD_EDGE_PAD };
  return { x: -WORLD_EDGE_PAD, y: Math.random() * height };
}

function spawnZombie(sim, type = pickZombieType(sim.wave)) {
  if (sim.zombies.length >= MAX_ZOMBIES) return;
  const point = getSpawnPoint(sim.width, sim.height);
  sim.zombies.push(createZombie(sim, type, point.x, point.y));
}

function pushBurst(sim, x, y, type = "shot", life = 0.18, max = life) {
  sim.bursts.push({ x, y, type, life, max });
  if (sim.bursts.length > MAX_BURSTS + 22) {
    sim.bursts.splice(0, sim.bursts.length - (MAX_BURSTS + 22));
  }
}

function getPlayerAimPoint(sim, role) {
  const player = sim.players[role];
  return {
    x: clamp(player.aimX, 0, 1) * sim.width,
    y: clamp(player.aimY, 0, 1) * sim.height,
  };
}

function registerKill(sim, role, x, y, now) {
  const player = sim.players[role];
  const scoreGain = 1;
  sim.score += scoreGain;
  if (player) {
    player.score += scoreGain;
    player.lastKillAt = now;
  }
  pushBurst(sim, x, y, "kill", 0.4, 0.4);
}

function applyDamageToZombie(sim, role, zombie, amount, now, particleBurst = "shot") {
  if (!zombie) return false;
  zombie.hp -= amount;
  if (particleBurst === "shot") {
    pushBurst(sim, zombie.x, zombie.y, "shot", 0.14, 0.14);
  }
  if (zombie.hp > 0) return false;
  registerKill(sim, role, zombie.x, zombie.y, now);
  return true;
}

function applyRayDamage(sim, role, originX, originY, targetX, targetY, damage, now, thickness = 10, limitHits = 1) {
  const dx = targetX - originX;
  const dy = targetY - originY;
  const rayLength = Math.hypot(dx, dy) || 1;
  const nx = dx / rayLength;
  const ny = dy / rayLength;
  let hits = 0;

  for (let i = sim.zombies.length - 1; i >= 0; i -= 1) {
    const zombie = sim.zombies[i];
    const zx = zombie.x - originX;
    const zy = zombie.y - originY;
    const projection = zx * nx + zy * ny;
    if (projection < 0 || projection > rayLength + zombie.r) continue;
    const perpendicular = Math.abs(zx * ny - zy * nx);
    if (perpendicular > zombie.r + thickness) continue;
    if (applyDamageToZombie(sim, role, zombie, damage, now)) {
      sim.zombies.splice(i, 1);
    }
    hits += 1;
    if (hits >= limitHits) break;
  }
}

function spawnBullet(sim, role, bullet) {
  sim.bullets.push({ ...bullet, origin: role });
  if (sim.bullets.length > MAX_BULLETS + 24) {
    sim.bullets.splice(0, sim.bullets.length - (MAX_BULLETS + 24));
  }
}

function fireSingleShot(sim, role, weaponKey) {
  const weapon = WEAPONS[weaponKey] || WEAPONS.blaster;
  const player = sim.players[role];
  if (!player || !player.alive) return;

  const target = getPlayerAimPoint(sim, role);
  const originX = player.x;
  const originY = player.y;
  const dx = target.x - originX;
  const dy = target.y - originY;
  const dist = Math.hypot(dx, dy) || 1;
  const baseAngle = Math.atan2(dy, dx);
  const speed = weapon.speed ? weapon.speed + sim.wave * 22 : 0;

  if (weaponKey === "shotgun") {
    for (let i = 0; i < weapon.pellets; i += 1) {
      const spread = (Math.random() - 0.5) * weapon.spread;
      const pelletAngle = baseAngle + spread;
      spawnBullet(sim, role, {
        x: originX,
        y: originY,
        vx: Math.cos(pelletAngle) * speed,
        vy: Math.sin(pelletAngle) * speed,
        life: 0.7,
        r: 3,
        damage: weapon.damage,
        type: "bullet",
        splash: 0,
      });
    }
    pushBurst(sim, originX, originY, "shot", 0.18, 0.18);
    return;
  }

  if (weaponKey === "grenade") {
    spawnBullet(sim, role, {
      x: originX,
      y: originY,
      vx: (dx / dist) * speed,
      vy: (dy / dist) * speed,
      life: weapon.life,
      r: 6,
      damage: weapon.damage,
      type: "grenade",
      splash: weapon.splash,
    });
    pushBurst(sim, originX, originY, "shot", 0.2, 0.2);
    return;
  }

  spawnBullet(sim, role, {
    x: originX,
    y: originY,
    vx: (dx / dist) * speed,
    vy: (dy / dist) * speed,
    life: 0.85,
    r: 4,
    damage: weapon.damage,
    type: "bullet",
    splash: 0,
  });
  pushBurst(sim, originX, originY, "shot", 0.18, 0.18);
}

function processPlayerFire(sim, role, dt, now) {
  const player = sim.players[role];
  if (!player || !player.alive) return;

  const weaponKey = sanitizeWeapon(player.weapon);
  if (weaponKey === "beam") {
    if (!player.pointerDown) return;
    const target = getPlayerAimPoint(sim, role);
    applyRayDamage(
      sim,
      role,
      player.x,
      player.y,
      target.x,
      target.y,
      WEAPONS.beam.dps * dt,
      now,
      WEAPONS.beam.width * 0.55,
      3
    );
    return;
  }

  const shotSeq = Math.max(0, Math.floor(player.shotSeq || 0));
  if (shotSeq < player.processedShotSeq) {
    player.processedShotSeq = shotSeq;
  }
  const queued = Math.max(0, shotSeq - player.processedShotSeq);
  if (queued <= 0) return;

  const shotsToFire = Math.min(queued, MAX_SHOTS_PER_TICK);
  for (let i = 0; i < shotsToFire; i += 1) {
    fireSingleShot(sim, role, weaponKey);
  }
  player.processedShotSeq += shotsToFire;
}

function explodeGrenade(sim, bullet, now) {
  const radius = Number(bullet.splash) || 70;
  pushBurst(sim, bullet.x, bullet.y, "blast", 0.42, 0.42);

  for (let i = sim.zombies.length - 1; i >= 0; i -= 1) {
    const zombie = sim.zombies[i];
    const dist = Math.hypot(zombie.x - bullet.x, zombie.y - bullet.y);
    if (dist > radius + zombie.r) continue;
    if (applyDamageToZombie(sim, bullet.origin, zombie, Number(bullet.damage) || 1, now, "blast")) {
      sim.zombies.splice(i, 1);
    }
  }
}

function updateBullets(sim, dt, now) {
  for (let i = sim.bullets.length - 1; i >= 0; i -= 1) {
    const bullet = sim.bullets[i];
    bullet.x += (Number(bullet.vx) || 0) * dt;
    bullet.y += (Number(bullet.vy) || 0) * dt;
    bullet.life = Math.max(0, (Number(bullet.life) || 0) - dt);

    if (bullet.type === "grenade" && bullet.life <= 0) {
      explodeGrenade(sim, bullet, now);
      sim.bullets.splice(i, 1);
      continue;
    }

    let hit = false;
    for (let j = sim.zombies.length - 1; j >= 0; j -= 1) {
      const zombie = sim.zombies[j];
      const dx = zombie.x - bullet.x;
      const dy = zombie.y - bullet.y;
      const dist = Math.hypot(dx, dy);
      if (dist < zombie.r + bullet.r) {
        if (bullet.type === "grenade") {
          explodeGrenade(sim, bullet, now);
        } else if (applyDamageToZombie(sim, bullet.origin, zombie, Number(bullet.damage) || 1, now)) {
          sim.zombies.splice(j, 1);
        }
        sim.bullets.splice(i, 1);
        hit = true;
        break;
      }
    }

    if (hit) continue;

    if (
      bullet.life <= 0 ||
      bullet.x < -90 ||
      bullet.x > sim.width + 90 ||
      bullet.y < -90 ||
      bullet.y > sim.height + 90
    ) {
      sim.bullets.splice(i, 1);
    }
  }
}

function updateZombies(sim, dt, now) {
  const host = sim.players.host;
  const guest = sim.players.guest;

  for (let i = sim.zombies.length - 1; i >= 0; i -= 1) {
    const zombie = sim.zombies[i];

    if (!host.alive && !guest.alive) {
      return;
    }

    if (!host.alive) {
      zombie.targetCore = "guest";
    } else if (!guest.alive) {
      zombie.targetCore = "host";
    } else if (zombie.targetCore !== "host" && zombie.targetCore !== "guest") {
      zombie.targetCore = Math.random() < 0.5 ? "host" : "guest";
    }

    const targetRole = zombie.targetCore === "guest" ? "guest" : "host";
    const target = sim.players[targetRole];
    if (!target || !target.alive) continue;

    const dx = target.x - zombie.x;
    const dy = target.y - zombie.y;
    const dist = Math.hypot(dx, dy) || 1;

    zombie.wobble += dt * 4;
    let speed = Number(zombie.baseSpeed) || estimateZombieBaseSpeed(zombie.type, sim.wave);

    if (zombie.type === "dash") {
      zombie.dashCooldown = (Number(zombie.dashCooldown) || 0) - dt;
      if (zombie.dashCooldown <= 0) {
        zombie.dashTimer = 0.35;
        zombie.dashCooldown = 2 + Math.random() * 1.4;
      }
      if ((Number(zombie.dashTimer) || 0) > 0) {
        zombie.dashTimer -= dt;
        speed *= 2.6;
      }
    }

    zombie.x += (dx / dist) * speed * dt;
    zombie.y += (dy / dist) * speed * dt;

    if (dist < zombie.r + CORE_RADIUS) {
      sim.zombies.splice(i, 1);
      target.hp = Math.max(0, target.hp - 1);
      target.alive = target.hp > 0;
      pushBurst(sim, target.x, target.y, "shot", 0.2, 0.2);
      if (!target.alive) {
        target.pointerDown = false;
      }
    }
  }
}

function updateBursts(sim, dt) {
  for (let i = sim.bursts.length - 1; i >= 0; i -= 1) {
    const burst = sim.bursts[i];
    burst.life -= dt;
    if (burst.life <= 0) {
      sim.bursts.splice(i, 1);
    }
  }
}

function syncPlayerInputFromRoom(sim, room, role) {
  const slot = room[role];
  const player = sim.players[role];
  if (!slot || !player) return;

  const maxHpIncoming = clamp(slot.maxHp, 1, 500);
  if (maxHpIncoming > player.maxHp) {
    player.maxHp = maxHpIncoming;
    if (player.hp > player.maxHp) {
      player.hp = player.maxHp;
    }
  }

  player.weapon = sanitizeWeapon(slot.weapon || player.weapon);
  player.aimX = quantizeNorm(slot.aimX ?? player.aimX);
  player.aimY = quantizeNorm(slot.aimY ?? player.aimY);
  player.pointerDown = Boolean(slot.pointerDown) && player.alive;

  const seq = Math.max(0, Math.floor(Number(slot.shotSeq) || 0));
  if (seq < player.processedShotSeq) {
    player.processedShotSeq = seq;
  }
  player.shotSeq = seq;

  slot.weapon = player.weapon;
  slot.aimX = player.aimX;
  slot.aimY = player.aimY;
  slot.pointerDown = player.pointerDown;
  slot.shotSeq = player.shotSeq;
}

function createPlayerState(role, roomSlot, coords) {
  const maxHp = clamp(roomSlot?.maxHp, 1, 500);
  const hp = clamp(roomSlot?.hp ?? maxHp, 0, maxHp);
  const shotSeq = Math.max(0, Math.floor(Number(roomSlot?.shotSeq) || 0));

  return {
    role,
    x: coords.x,
    y: coords.y,
    hp,
    maxHp,
    alive: hp > 0,
    score: Math.max(0, Math.floor(Number(roomSlot?.score) || 0)),
    weapon: sanitizeWeapon(roomSlot?.weapon),
    aimX: quantizeNorm(roomSlot?.aimX ?? 0.5),
    aimY: quantizeNorm(roomSlot?.aimY ?? 0.5),
    pointerDown: Boolean(roomSlot?.pointerDown),
    shotSeq,
    processedShotSeq: shotSeq,
    lastKillAt: 0,
  };
}

function createSimulation(roomId, room) {
  const width = WORLD_WIDTH;
  const height = WORLD_HEIGHT;
  const coords = getCoreCoordinates(width, height);

  const host = createPlayerState("host", room.host, coords.host);
  const guest = createPlayerState("guest", room.guest, coords.guest);
  const score = host.score + guest.score;

  return {
    roomId,
    width,
    height,
    createdAt: nowMs(),
    lastTickAt: nowMs(),
    score,
    wave: Math.max(1, Number(room.sharedWave) || 1),
    lastSpawnAt: nowMs(),
    lastBossWave: 0,
    endedReason: "",
    players: {
      host,
      guest,
    },
    zombies: [],
    bullets: [],
    bursts: [],
    nextZombieId: 1,
    atmosphere: {
      timeCode: 2,
      weatherCode: 0,
      lightning: 0,
    },
  };
}

function applySimulationToRoom(room, sim) {
  room.sharedWave = Math.max(1, sim.wave);
  room.updatedAt = nowMs();

  for (const role of ["host", "guest"]) {
    const slot = room[role];
    const player = sim.players[role];
    if (!slot || !player) continue;
    slot.hp = Math.max(0, Number(player.hp) || 0);
    slot.maxHp = Math.max(1, Number(player.maxHp) || 1);
    slot.alive = player.alive;
    slot.score = Math.max(0, Math.floor(Number(player.score) || 0));
    slot.wave = Math.max(1, sim.wave);
    slot.updatedAt = room.updatedAt;
  }

  if (!sim.players.host.alive && !sim.players.guest.alive) {
    room.status = "ended";
    room.endedReason = sim.endedReason || "Оба ядра уничтожены.";
    room.endedAt = nowMs();
  }
}

function serializeWorld(sim) {
  const baseWidth = Math.max(1, sim.width || WORLD_WIDTH);
  const baseHeight = Math.max(1, sim.height || WORLD_HEIGHT);
  const baseMin = Math.max(1, Math.min(baseWidth, baseHeight));

  const z = sim.zombies.slice(0, MAX_ZOMBIES).map((zombie) => {
    const xNorm = Math.round(clamp(zombie.x / baseWidth, 0, 1) * 10000);
    const yNorm = Math.round(clamp(zombie.y / baseHeight, 0, 1) * 10000);
    const rNorm = Math.round(clamp(zombie.r / baseMin, 0.003, 0.3) * 10000);
    const hp10 = Math.max(0, Math.round((Number(zombie.hp) || 0) * 10));
    const maxHp10 = Math.max(1, Math.round((Number(zombie.maxHp) || 1) * 10));
    const typeCode = ZOMBIE_TYPE_TO_CODE[String(zombie.type || "normal")] ?? 0;
    return [Number(zombie.id) || 0, xNorm, yNorm, rNorm, hp10, maxHp10, typeCode, 0];
  });

  const b = sim.bullets.slice(-MAX_BULLETS).map((bullet) => {
    const xNorm = Math.round(clamp((Number(bullet.x) || 0) / baseWidth, 0, 1) * 10000);
    const yNorm = Math.round(clamp((Number(bullet.y) || 0) / baseHeight, 0, 1) * 10000);
    const vxNorm = Math.round(clamp((Number(bullet.vx) || 0) / 2200, -1, 1) * 10000);
    const vyNorm = Math.round(clamp((Number(bullet.vy) || 0) / 2200, -1, 1) * 10000);
    const rNorm = Math.round(clamp((Number(bullet.r) || 3) / baseMin, 0.0015, 0.08) * 10000);
    const typeCode = bullet.type === "grenade" ? 1 : 0;
    const life100 = Math.max(0, Math.round((Number(bullet.life) || 0) * 100));
    return [xNorm, yNorm, vxNorm, vyNorm, rNorm, typeCode, life100];
  });

  const recentBursts = sim.bursts.slice(-(MAX_BURSTS * 2));
  const priority = recentBursts.filter((burst) => burst && burst.type !== "shot");
  const shots = recentBursts.filter((burst) => burst && burst.type === "shot");
  const snapshotBursts = [];
  if (priority.length >= MAX_BURSTS) {
    snapshotBursts.push(...priority.slice(-MAX_BURSTS));
  } else {
    snapshotBursts.push(...priority);
    const remaining = MAX_BURSTS - snapshotBursts.length;
    if (remaining > 0) {
      snapshotBursts.push(...shots.slice(-remaining));
    }
  }

  const u = snapshotBursts.map((burst) => {
    const xNorm = Math.round(clamp((Number(burst.x) || 0) / baseWidth, 0, 1) * 10000);
    const yNorm = Math.round(clamp((Number(burst.y) || 0) / baseHeight, 0, 1) * 10000);
    const typeCode = burst.type === "kill" ? 1 : burst.type === "blast" ? 2 : 0;
    const life100 = Math.max(0, Math.round((Number(burst.life) || 0) * 100));
    const max100 = Math.max(1, Math.round((Number(burst.max) || Number(burst.life) || 0.2) * 100));
    return [xNorm, yNorm, typeCode, life100, max100];
  });

  return {
    v: nowMs(),
    sw: Math.round(baseWidth),
    sh: Math.round(baseHeight),
    s: Math.max(0, Math.floor(Number(sim.score) || 0)),
    w: Math.max(1, Math.floor(Number(sim.wave) || 1)),
    z,
    b,
    u,
    p: [],
    at: [sim.atmosphere.timeCode, sim.atmosphere.weatherCode, sim.atmosphere.lightning],
    ev: [0, 0, Math.max(1, Math.floor(Number(sim.wave) || 1))],
    fx: [0, 0, 0],
  };
}

function removeSimulation(roomId) {
  simulations.delete(roomId);
  worlds.delete(roomId);
  broadcastSnapshot("world", roomId);
}

function ensureSimulation(roomId, room) {
  if (!isPlainObject(room?.host) || !isPlainObject(room?.guest)) {
    removeSimulation(roomId);
    return null;
  }
  let sim = simulations.get(roomId);
  if (!sim) {
    sim = createSimulation(roomId, room);
    simulations.set(roomId, sim);
  }
  return sim;
}

function reconcileRoomAndSimulation(roomId) {
  const room = rooms.get(roomId);
  if (!room) {
    removeSimulation(roomId);
    return;
  }

  if (room.status !== "running") {
    removeSimulation(roomId);
    return;
  }

  const sim = ensureSimulation(roomId, room);
  if (!sim) {
    room.status = "ended";
    room.endedReason = "Недостаточно игроков для коопа.";
    room.endedAt = nowMs();
    rooms.set(roomId, room);
    broadcastSnapshot("room", roomId);
    return;
  }

  syncPlayerInputFromRoom(sim, room, "host");
  syncPlayerInputFromRoom(sim, room, "guest");
  applySimulationToRoom(room, sim);
  rooms.set(roomId, room);
}

function writeSet(kind, roomId, value) {
  const store = getStore(kind);
  if (value === null || value === undefined) {
    store.delete(roomId);
    if (kind === "room") {
      removeSimulation(roomId);
    }
    return;
  }

  if (kind === "room") {
    const normalized = normalizeRoomForSet(roomId, value);
    if (!normalized) {
      store.delete(roomId);
      removeSimulation(roomId);
      return;
    }
    store.set(roomId, normalized);
    reconcileRoomAndSimulation(roomId);
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
  const safePatch = kind === "room" ? sanitizeRoomPatch(current, patch) : patch;

  for (const [key, value] of Object.entries(safePatch)) {
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
    if (kind === "room") {
      removeSimulation(roomId);
    }
    return;
  }

  if (kind === "room") {
    current.roomId = roomId;
    current.updatedAt = nowMs();
    if (!isPlainObject(current.host)) current.host = null;
    if (!isPlainObject(current.guest)) current.guest = null;
    if (!["waiting", "running", "ended"].includes(String(current.status || ""))) {
      current.status = "waiting";
    }
    current.sharedWave = Math.max(1, Math.floor(Number(current.sharedWave) || 1));
    store.set(roomId, current);
    reconcileRoomAndSimulation(roomId);
    return;
  }

  store.set(roomId, current);
}

function tickSimulations() {
  const now = nowMs();

  for (const [roomId, sim] of simulations.entries()) {
    const room = rooms.get(roomId);
    if (!room || room.status !== "running") {
      removeSimulation(roomId);
      continue;
    }
    if (!room.host || !room.guest) {
      room.status = "ended";
      room.endedReason = "Один из игроков покинул матч.";
      room.endedAt = nowMs();
      rooms.set(roomId, room);
      removeSimulation(roomId);
      broadcastSnapshot("room", roomId);
      continue;
    }

    const dt = clamp((now - (Number(sim.lastTickAt) || now)) / 1000, 0.008, 0.05);
    sim.lastTickAt = now;

    syncPlayerInputFromRoom(sim, room, "host");
    syncPlayerInputFromRoom(sim, room, "guest");

    sim.wave = Math.max(1, 1 + Math.floor(sim.score / 20));
    const spawnInterval = Math.max(320, 1050 - sim.wave * 60);

    if (sim.wave % 5 === 0 && sim.lastBossWave !== sim.wave) {
      const bossAlive = sim.zombies.some((zombie) => zombie.type === "boss");
      if (!bossAlive) {
        spawnZombie(sim, "boss");
        sim.lastBossWave = sim.wave;
      }
    }

    if (now - sim.lastSpawnAt >= spawnInterval && sim.zombies.length < MAX_ZOMBIES) {
      spawnZombie(sim);
      sim.lastSpawnAt = now;
    }

    processPlayerFire(sim, "host", dt, now);
    processPlayerFire(sim, "guest", dt, now);
    updateBullets(sim, dt, now);
    updateZombies(sim, dt, now);
    updateBursts(sim, dt);

    if (!sim.players.host.alive && !sim.players.guest.alive) {
      sim.endedReason = "Оба ядра уничтожены.";
    }

    applySimulationToRoom(room, sim);
    rooms.set(roomId, room);

    if (room.status === "ended") {
      removeSimulation(roomId);
      broadcastSnapshot("room", roomId);
      continue;
    }

    const worldSnapshot = serializeWorld(sim);
    worlds.set(roomId, worldSnapshot);
    broadcastSnapshot("world", roomId);
    broadcastSnapshot("room", roomId);
  }
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

    if (kind === "world" && (action === "set" || action === "update")) {
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
      sims: simulations.size,
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

const simulationLoop = setInterval(() => {
  tickSimulations();
}, SIM_TICK_MS);

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

heartbeat.unref();
simulationLoop.unref();

server.listen(PORT, HOST, () => {
  // eslint-disable-next-line no-console
  console.log(`[ws-server] listening on ${HOST}:${PORT}`);
});

process.on("SIGTERM", () => {
  clearInterval(heartbeat);
  clearInterval(simulationLoop);
  try {
    wss.close();
  } catch (error) {}
  try {
    server.close(() => process.exit(0));
  } catch (error) {
    process.exit(0);
  }
});
