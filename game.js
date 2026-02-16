const canvas = document.getElementById("gameCanvas");
const ctx = canvas.getContext("2d");

const scoreEl = document.getElementById("score");
const waveEl = document.getElementById("wave");
const hpEl = document.getElementById("hp");
const comboEl = document.getElementById("combo");

const overlay = document.getElementById("overlay");
const overlayTitle = document.getElementById("overlayTitle");
const overlayText = document.getElementById("overlayText");
const playBtn = document.getElementById("playBtn");
const startBtn = document.getElementById("startBtn");
const scrollBtn = document.getElementById("scrollBtn");
const pauseBtn = document.getElementById("pauseBtn");
const restartBtn = document.getElementById("restartBtn");
const soundBtn = document.getElementById("soundBtn");
const weaponButtons = document.querySelectorAll(".weapon-btn");
const statusLine = document.getElementById("statusLine");
const emberLayer = document.querySelector(".ember-layer");
const vignetteLayer = document.querySelector(".vignette");

const state = {
  running: false,
  status: "idle",
  score: 0,
  wave: 1,
  hp: 5,
  maxHp: 5,
  combo: 0,
  lastKill: 0,
  lastShot: 0,
  lastSpawn: 0,
  spawnIntervalBase: 1100,
  zombies: [],
  bullets: [],
  bursts: [],
  particles: [],
  floaters: [],
  powerups: [],
  effects: { rapid: 0, shield: 0, slow: 0 },
  weapon: "blaster",
  lastBossWave: 0,
  beamActive: false,
  beamTarget: null,
  event: { type: null, timer: 0, wave: 0 },
  flash: 0,
  shake: 0,
  pointer: { x: 0, y: 0, active: false, down: false },
  muzzleFlash: 0,
  screenShake: 0,
  atmosphere: {
    timeKey: "dusk",
    weatherKey: "clear",
    timeTimer: 24,
    weatherTimer: 20,
    lightning: 0,
    hudUpdateTimer: 0,
  },
};

let width = 0;
let height = 0;
let lastTime = performance.now();
let zombieIdSeed = 1;

const COOP_SYNC_INTERVAL_MS = 620;
const COOP_INPUT_SYNC_INTERVAL_MS = 280;
const COOP_WORLD_SYNC_INTERVAL_MS = 95;
const COOP_SYNC_INTERVAL_WS_MS = 130;
const COOP_INPUT_SYNC_INTERVAL_WS_MS = 75;
const COOP_WORLD_SYNC_INTERVAL_WS_MS = 82;
const COOP_WORLD_MAX_ZOMBIES = 24;
const COOP_WORLD_MAX_ZOMBIES_WS = 18;
const COOP_WORLD_MAX_BULLETS = 42;
const COOP_WORLD_MAX_BURSTS = 26;
const COOP_GUEST_FX_STEP_MS = 16;
const COOP_GUEST_TARGET_FRAME_MS = 16;
const COOP_WS_REQUEST_TIMEOUT_MS = 45000;
const COOP_WS_RECONNECT_BASE_MS = 450;
const COOP_WS_RECONNECT_MAX_MS = 4200;

const ZOMBIE_TYPE_TO_CODE = {
  normal: 0,
  fast: 1,
  tank: 2,
  dash: 3,
  boss: 4,
};

const ZOMBIE_CODE_TO_TYPE = ["normal", "fast", "tank", "dash", "boss"];

const BURST_TYPE_TO_CODE = {
  shot: 0,
  kill: 1,
  blast: 2,
};

const BURST_CODE_TO_TYPE = ["shot", "kill", "blast"];

const coopState = {
  active: false,
  roomId: "",
  role: null,
  roomStatus: "idle",
  actionBusy: false,
  sharedWave: 1,
  remoteName: "",
  remoteHp: 0,
  remoteMaxHp: 5,
  remoteAlive: false,
  remoteWave: 1,
  remoteScore: 0,
  remoteAimX: 0.5,
  remoteAimY: 0.5,
  remotePointerDown: false,
  remoteWeapon: "blaster",
  remoteInputAt: 0,
  remoteLastPacketAt: 0,
  remoteMuzzle: 0,
  remoteShotTimer: 0,
  remoteBeamActive: false,
  remoteBeamTarget: null,
  remoteTracers: [],
  remoteConnected: false,
  unsub: null,
  worldUnsub: null,
  roomWriteInFlight: false,
  roomPendingPayload: null,
  worldWriteInFlight: false,
  worldPendingSnapshot: null,
  lastSyncAt: 0,
  lastInputSyncAt: 0,
  lastWorldSyncAt: 0,
  worldVersion: 0,
  lastRoomPayloadKey: "",
  lastWorldPayloadKey: "",
  resultSaved: false,
  lastGuestFxAt: 0,
  lastGuestRenderAt: 0,
};

const coopWs = {
  url: "",
  socket: null,
  connected: false,
  connecting: false,
  connectPromise: null,
  reconnectTimer: null,
  reconnectDelayMs: COOP_WS_RECONNECT_BASE_MS,
  nextRequestId: 1,
  nextSubId: 1,
  requests: new Map(),
  subs: new Map(),
};

const TIME_OF_DAY_ORDER = ["dawn", "day", "dusk", "night"];

const TIME_OF_DAY_CONFIG = {
  dawn: {
    label: "Рассвет",
    icon: "🌅",
    duration: [30, 44],
    tint: "rgba(255, 179, 126, 0.14)",
    visibility: 0.96,
    spawn: 0.95,
    speed: 0.98,
    accuracy: 1.04,
    score: 1,
  },
  day: {
    label: "День",
    icon: "☀️",
    duration: [36, 50],
    tint: "rgba(150, 200, 255, 0.1)",
    visibility: 1,
    spawn: 1,
    speed: 1,
    accuracy: 1.06,
    score: 1,
  },
  dusk: {
    label: "Сумерки",
    icon: "🌆",
    duration: [28, 42],
    tint: "rgba(180, 120, 255, 0.12)",
    visibility: 0.88,
    spawn: 1.1,
    speed: 1.03,
    accuracy: 0.95,
    score: 1.08,
  },
  night: {
    label: "Ночь",
    icon: "🌙",
    duration: [30, 44],
    tint: "rgba(14, 20, 36, 0.24)",
    visibility: 0.76,
    spawn: 1.18,
    speed: 1.08,
    accuracy: 0.88,
    score: 1.18,
  },
};

const WEATHER_CONFIG = {
  clear: {
    label: "Ясно",
    icon: "🌤️",
    duration: [20, 34],
    tint: "rgba(140, 220, 200, 0.02)",
    visibility: 1,
    spawn: 1,
    speed: 1,
    accuracy: 1,
    score: 1,
    rain: 0,
    fog: 0,
    storm: false,
  },
  rain: {
    label: "Ливень",
    icon: "🌧️",
    duration: [18, 30],
    tint: "rgba(80, 130, 170, 0.14)",
    visibility: 0.9,
    spawn: 1.12,
    speed: 1.04,
    accuracy: 0.9,
    score: 1.08,
    rain: 1,
    fog: 0.1,
    storm: false,
  },
  storm: {
    label: "Буря",
    icon: "⛈️",
    duration: [14, 24],
    tint: "rgba(58, 96, 140, 0.2)",
    visibility: 0.82,
    spawn: 1.22,
    speed: 1.12,
    accuracy: 0.82,
    score: 1.22,
    rain: 1.35,
    fog: 0.15,
    storm: true,
  },
  fog: {
    label: "Туман",
    icon: "🌫️",
    duration: [18, 28],
    tint: "rgba(170, 190, 190, 0.2)",
    visibility: 0.78,
    spawn: 1.08,
    speed: 0.98,
    accuracy: 0.9,
    score: 1.12,
    rain: 0,
    fog: 1,
    storm: false,
  },
};

const COLORS = {
  coreGlow: "#2bdc77",
  coreEdge: "#1aa86b",
  bullet: "#ff8f4d",
  bulletGlow: "rgba(255, 143, 77, 0.3)",
  grenade: "#ffb347",
  beam: "rgba(67, 255, 140, 0.65)",
  zombie: "#1f5a3c",
  zombieEye: "#d9ff9c",
  zombieAccent: "#39b069",
  hpBack: "rgba(4, 10, 7, 0.6)",
  hpFill: "#ff3b3b",
  shield: "rgba(67, 255, 140, 0.5)",
  crosshair: "rgba(230, 245, 238, 0.7)",
  floater: "#ff6b35",
};

const POWERUP_DURATION = {
  rapid: 6,
  shield: 8,
  slow: 7,
};

const WEAPONS = {
  blaster: { label: "Blaster", cooldown: 220, speed: 720, damage: 1, pellets: 1, spread: 0.1 },
  shotgun: { label: "Shotgun", cooldown: 620, speed: 640, damage: 0.6, pellets: 5, spread: 0.65 },
  beam: { label: "Beam", cooldown: 0, dps: 6.5, width: 14 },
  grenade: { label: "Grenade", cooldown: 900, speed: 420, damage: 2.2, pellets: 1, spread: 0.05, splash: 70, life: 0.95 },
};

// ============================================
// ЭКОНОМИКА И ПРОКАЧКА
// ============================================
const UPGRADE_COSTS = {
  // Цена за уровень прокачки (формула: baseCost * level^1.5)
  hp: { base: 50, maxLevel: 20 },
  maxHp: { base: 80, maxLevel: 15 },
  weaponDamage: { base: 100, maxLevel: 25 },
  weaponFireRate: { base: 120, maxLevel: 20 },
};

const WEAPON_PRICES = {
  blaster: 0, // Стартовое оружие
  shotgun: 500,
  grenade: 1400,
  beam: 3200,
};

const BASE_WEAPON_PROGRESS = {
  blaster: { owned: true, damageLevel: 1, fireRateLevel: 1 },
  shotgun: { owned: false, damageLevel: 1, fireRateLevel: 1 },
  grenade: { owned: false, damageLevel: 1, fireRateLevel: 1 },
  beam: { owned: false, damageLevel: 1, fireRateLevel: 1 },
};

const UPGRADE_TYPE_ALIASES = {
  hpLevel: "hp",
  maxHpLevel: "maxHp",
  damageLevel: "weaponDamage",
  fireRateLevel: "weaponFireRate",
};

function createDefaultWeaponProgress() {
  return Object.fromEntries(
    Object.entries(BASE_WEAPON_PROGRESS).map(([weaponKey, progress]) => [weaponKey, { ...progress }])
  );
}

// Золото за убийство (зависит от волны)
function getGoldPerKill(wave) {
  return Math.floor(3 + wave * 0.8);
}

// Золото за выживание волны
function getGoldPerWave(wave) {
  return Math.floor(15 + wave * 5);
}

// Золото за очки
function getGoldPerScore(score) {
  return Math.floor(score / 50);
}

// Получить цену прокачки
function getUpgradeCost(type, currentLevel) {
  const resolvedType = UPGRADE_TYPE_ALIASES[type] || type;
  const config = UPGRADE_COSTS[resolvedType];
  const level = Math.max(1, Number(currentLevel) || 1);
  if (!config || level >= config.maxLevel) return Infinity;
  return Math.floor(config.base * Math.pow(level, 1.5));
}

// Применить бонусы прокачки HP
function applyHpUpgrades(user) {
  if (!user) return { hpBonus: 0, maxHpBonus: 0 };
  const hpLevel = user.hpLevel || 1;
  const maxHpLevel = user.maxHpLevel || 1;
  const hpBonus = (hpLevel - 1) * 2;
  const maxHpBonus = (maxHpLevel - 1) * 5;
  return { hpBonus, maxHpBonus };
}

function getStartHealAtLevel(level) {
  const safeLevel = Math.max(1, Number(level) || 1);
  return (safeLevel - 1) * 2;
}

function getMaxHpAtLevel(level) {
  const safeLevel = Math.max(1, Number(level) || 1);
  return 5 + (safeLevel - 1) * 5;
}

function getWeaponDamageAtLevel(weaponKey, level) {
  const safeLevel = Math.max(1, Number(level) || 1);
  const baseDamage = WEAPONS[weaponKey]?.damage || 0;
  return baseDamage * (1 + (safeLevel - 1) * 0.15);
}

function getWeaponCooldownAtLevel(weaponKey, level) {
  const safeLevel = Math.max(1, Number(level) || 1);
  const baseCooldown = WEAPONS[weaponKey]?.cooldown || 0;
  if (baseCooldown <= 0) return 0;
  const fireRateBonus = Math.min(0.75, (safeLevel - 1) * 0.05);
  return Math.round(baseCooldown * Math.max(0.2, 1 - fireRateBonus));
}

function getBeamDpsAtLevels(damageLevel, fireRateLevel) {
  const safeDamageLevel = Math.max(1, Number(damageLevel) || 1);
  const safeFireRateLevel = Math.max(1, Number(fireRateLevel) || 1);
  const damageMultiplier = 1 + (safeDamageLevel - 1) * 0.15;
  const fireMultiplier = 1 + Math.min(0.75, (safeFireRateLevel - 1) * 0.05);
  return WEAPONS.beam.dps * damageMultiplier * fireMultiplier;
}

function formatCompactNumber(value, digits = 2) {
  if (!Number.isFinite(value)) return "0";
  return Number(value).toFixed(digits).replace(/\.?0+$/, "");
}

// Получить бонус урона оружия
function getWeaponDamageBonus(user, weaponKey) {
  if (!user || !user.weapons) return 0;
  const weapon = user.weapons[weaponKey];
  if (!weapon) return 0;
  return (weapon.damageLevel - 1) * 0.15;
}

// Получить бонус скорострельности оружия
function getWeaponFireRateBonus(user, weaponKey) {
  if (!user || !user.weapons) return 0;
  const weapon = user.weapons[weaponKey];
  if (!weapon) return 0;
  return Math.min(0.75, (weapon.fireRateLevel - 1) * 0.05);
}

function normalizeUserWeapons(weapons) {
  const normalized = createDefaultWeaponProgress();
  const source = weapons && typeof weapons === "object" ? { ...weapons } : {};

  // Легаси-ключ из старых профилей
  if (!source.blaster && source.pistol) {
    source.blaster = source.pistol;
  }

  Object.keys(normalized).forEach((weaponKey) => {
    const stored = source[weaponKey];
    if (!stored || typeof stored !== "object") return;
    normalized[weaponKey] = {
      ...normalized[weaponKey],
      damageLevel: Math.max(1, Number(stored.damageLevel) || 1),
      fireRateLevel: Math.max(1, Number(stored.fireRateLevel) || 1),
      owned: weaponKey === "blaster" ? true : Boolean(stored.owned),
    };
  });

  normalized.blaster.owned = true;
  return normalized;
}

// Миграция профиля - добавить недостающие поля
function migrateProfile(user) {
  if (!user) return user;
  return {
    ...user,
    gold: Math.max(0, Number(user.gold) || 0),
    upgradePoints: Math.max(0, Number(user.upgradePoints) || 0),
    hpLevel: Math.max(1, Number(user.hpLevel) || 1),
    maxHpLevel: Math.max(1, Number(user.maxHpLevel) || 1),
    weapons: normalizeUserWeapons(user.weapons),
  };
}

const EVENTS = {
  fog: {
    label: "Туман",
    duration: 10,
    speed: 1,
    spawn: 1.05,
    score: 1,
    tint: "rgba(120, 150, 130, 0.35)",
  },
  rage: {
    label: "Ярость",
    duration: 9,
    speed: 1.35,
    spawn: 1.35,
    score: 1,
    tint: "rgba(120, 30, 30, 0.25)",
  },
  night: {
    label: "Ночь",
    duration: 11,
    speed: 1.1,
    spawn: 1.6,
    score: 2,
    tint: "rgba(2, 4, 6, 0.5)",
  },
};

const POWERUP_LABELS = {
  rapid: "Ускорение",
  shield: "Щит",
  slow: "Замедление",
};

const POWERUP_FLOATERS = {
  rapid: "УСКОР",
  shield: "ЩИТ",
  slow: "ЗАМЕД",
};

const audio = {
  ctx: null,
  master: null,
  musicGain: null,
  ambientGain: null,
  sfxGain: null,
  enabled: true,
  ambientBuffer: null,
  ambientSource: null,
  ambientLoadPromise: null,
  ambientStopTimer: null,
};

const AMBIENT_GAIN_MENU = 0.1;
const AMBIENT_GAIN_GAME = 0.01;

// ============================================
// GOOGLE AUTH + FIREBASE (персистентное облачное сохранение)
// ============================================

const STORAGE_KEYS = {
  USERS: "zombiesurge_users",
  CURRENT_USER: "zombiesurge_current",
  AUTH_REDIRECT_PENDING: "zombiesurge_auth_redirect_pending",
};

const CLOUD_KEYS = {
  USERS_COLLECTION: "zombiesurge_users_v2",
  COOP_COLLECTION: "zombiesurge_rooms_v1",
  COOP_WORLD_COLLECTION: "zombiesurge_world_v1",
};

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function randomRange(min, max) {
  return min + Math.random() * (max - min);
}

function getDurationFromRange(range) {
  if (!Array.isArray(range) || range.length < 2) return 20;
  return randomRange(range[0], range[1]);
}

function getAtmosphereEffectText(modifiers) {
  const parts = [];

  if (modifiers.visibility < 0.92) {
    parts.push(`видимость ${Math.round(modifiers.visibility * 100)}%`);
  } else if (modifiers.visibility > 1.01) {
    parts.push(`чёткая видимость ${Math.round(modifiers.visibility * 100)}%`);
  }

  if (modifiers.spawn > 1.05) {
    parts.push(`волны плотнее (${Math.round(modifiers.spawn * 100)}%)`);
  } else if (modifiers.spawn < 0.95) {
    parts.push(`волны реже (${Math.round(modifiers.spawn * 100)}%)`);
  }

  if (modifiers.accuracy < 0.95) {
    parts.push(`точность сложнее (${Math.round(modifiers.accuracy * 100)}%)`);
  } else if (modifiers.accuracy > 1.04) {
    parts.push(`стрелять легче (${Math.round(modifiers.accuracy * 100)}%)`);
  }

  if (modifiers.storm) {
    parts.push("возможны вспышки молнии");
  }

  if (parts.length === 0) {
    return "Стабильная видимость и стандартный темп волн.";
  }
  return parts.join(" · ");
}

function updateAtmosphereHud(force = false) {
  const hasHud =
    atmoTimeBadge && atmoWeatherBadge && atmoEffectText && atmoVisibilityValue && atmoSpawnValue && atmoAccuracyValue;
  if (!hasHud) return;

  const timeCfg = getCurrentTimeConfig();
  const weatherCfg = getCurrentWeatherConfig();
  const modifiers = getAtmosphereModifiers();

  atmoTimeBadge.textContent = `${timeCfg.icon} ${timeCfg.label}`;
  atmoWeatherBadge.textContent = `${weatherCfg.icon} ${weatherCfg.label}`;
  atmoVisibilityValue.textContent = `${Math.round(modifiers.visibility * 100)}%`;
  atmoSpawnValue.textContent = `${Math.round(modifiers.spawn * 100)}%`;
  atmoAccuracyValue.textContent = `${Math.round(modifiers.accuracy * 100)}%`;
  atmoEffectText.textContent = getAtmosphereEffectText(modifiers);

  if (vignetteLayer && force) {
    const edgeDarkness = clamp(
      0.34 + (1 - modifiers.visibility) * 0.46 + modifiers.weatherFog * 0.08 + (modifiers.storm ? 0.03 : 0),
      0.28,
      0.82
    );
    vignetteLayer.style.background = `radial-gradient(ellipse at center, transparent 40%, rgba(0, 0, 0, ${edgeDarkness.toFixed(2)}) 100%)`;
  }
}

function updateAtmosphere(dt) {
  state.atmosphere.timeTimer -= dt;
  state.atmosphere.weatherTimer -= dt;
  state.atmosphere.hudUpdateTimer -= dt;

  if (state.atmosphere.timeTimer <= 0) {
    state.atmosphere.timeKey = pickNextTimeKey();
    state.atmosphere.timeTimer = getDurationFromRange(getCurrentTimeConfig().duration);
    state.atmosphere.hudUpdateTimer = 0;
    updateStatusLine();
  }

  if (state.atmosphere.weatherTimer <= 0) {
    state.atmosphere.weatherKey = pickNextWeatherKey();
    state.atmosphere.weatherTimer = getDurationFromRange(getCurrentWeatherConfig().duration);
    state.atmosphere.hudUpdateTimer = 0;
    updateStatusLine();
  }

  const modifiers = getAtmosphereModifiers();
  if (modifiers.storm && Math.random() < dt * 0.55) {
    state.atmosphere.lightning = 1;
    state.flash = Math.max(state.flash, 0.16);
    state.shake = Math.min(20, state.shake + 1.5);
  }
  if (state.atmosphere.lightning > 0) {
    state.atmosphere.lightning = Math.max(0, state.atmosphere.lightning - dt * 3.2);
  }

  if (state.atmosphere.hudUpdateTimer <= 0) {
    updateAtmosphereHud(true);
    state.atmosphere.hudUpdateTimer = 0.4;
  }
}

function getCurrentTimeConfig() {
  return TIME_OF_DAY_CONFIG[state.atmosphere.timeKey] || TIME_OF_DAY_CONFIG.day;
}

function getCurrentWeatherConfig() {
  return WEATHER_CONFIG[state.atmosphere.weatherKey] || WEATHER_CONFIG.clear;
}

function getAtmosphereModifiers() {
  const timeCfg = getCurrentTimeConfig();
  const weatherCfg = getCurrentWeatherConfig();
  return {
    visibility: clamp(timeCfg.visibility * weatherCfg.visibility, 0.45, 1.05),
    spawn: clamp(timeCfg.spawn * weatherCfg.spawn, 0.7, 1.8),
    speed: clamp(timeCfg.speed * weatherCfg.speed, 0.75, 1.7),
    accuracy: clamp(timeCfg.accuracy * weatherCfg.accuracy, 0.65, 1.15),
    score: clamp(timeCfg.score * weatherCfg.score, 0.85, 1.5),
    weatherRain: weatherCfg.rain || 0,
    weatherFog: weatherCfg.fog || 0,
    storm: Boolean(weatherCfg.storm),
    weatherTint: weatherCfg.tint || "rgba(0,0,0,0)",
    timeTint: timeCfg.tint || "rgba(0,0,0,0)",
  };
}

function pickNextTimeKey() {
  const currentIndex = TIME_OF_DAY_ORDER.indexOf(state.atmosphere.timeKey);
  if (currentIndex < 0) return TIME_OF_DAY_ORDER[0];
  return TIME_OF_DAY_ORDER[(currentIndex + 1) % TIME_OF_DAY_ORDER.length];
}

function pickNextWeatherKey() {
  const keys = Object.keys(WEATHER_CONFIG);
  if (keys.length === 0) return "clear";
  const current = state.atmosphere.weatherKey;
  const pool = keys.filter((key) => key !== current);
  const source = pool.length ? pool : keys;
  return source[Math.floor(Math.random() * source.length)];
}

function resetAtmosphere() {
  state.atmosphere.timeKey = "dusk";
  state.atmosphere.weatherKey = Math.random() < 0.5 ? "clear" : "fog";
  state.atmosphere.timeTimer = getDurationFromRange(getCurrentTimeConfig().duration);
  state.atmosphere.weatherTimer = getDurationFromRange(getCurrentWeatherConfig().duration);
  state.atmosphere.lightning = 0;
  state.atmosphere.hudUpdateTimer = 0;
  updateAtmosphereHud(true);
}

function isDualCoreBattleActive() {
  if (!coopState.active || !coopState.remoteConnected) return false;
  return state.status === "running" || state.status === "ko" || state.status === "over";
}

function getOwnCorePosition() {
  if (!isDualCoreBattleActive()) {
    return { x: width / 2, y: height / 2 };
  }
  const offset = Math.min(110, width * 0.11);
  return {
    x: width / 2 - offset,
    y: height / 2 + 10,
  };
}

function getAllyCorePosition() {
  if (!isDualCoreBattleActive()) return null;
  const offset = Math.min(110, width * 0.11);
  return {
    x: width / 2 + offset,
    y: height / 2 - 10,
  };
}

function getPointerNorm(x = state.pointer.x, y = state.pointer.y) {
  return {
    x: clamp((Number(x) || 0) / Math.max(1, width), 0, 1),
    y: clamp((Number(y) || 0) / Math.max(1, height), 0, 1),
  };
}

function quantizeNormCoord(value, steps = 320) {
  const safeSteps = Math.max(1, Number(steps) || 320);
  const normalized = clamp(Number(value) || 0, 0, 1);
  return Math.round(normalized * safeSteps) / safeSteps;
}

function getPointFromNorm(normX = 0.5, normY = 0.5) {
  return {
    x: clamp(Number(normX) || 0.5, 0, 1) * width,
    y: clamp(Number(normY) || 0.5, 0, 1) * height,
  };
}

function getRemoteAimPoint() {
  return getPointFromNorm(coopState.remoteAimX, coopState.remoteAimY);
}

function isGuestMirrorMode() {
  return coopState.active && coopState.role === "guest" && coopState.roomStatus === "running";
}

function serializeCoopWorldSnapshot() {
  const baseWidth = Math.max(1, width || 1);
  const baseHeight = Math.max(1, height || 1);
  const baseMin = Math.max(1, Math.min(baseWidth, baseHeight));

  return {
    v: Date.now(),
    sw: Math.max(1, Math.round(baseWidth)),
    sh: Math.max(1, Math.round(baseHeight)),
    s: Math.max(0, Number(state.score) || 0),
    w: Math.max(1, Number(state.wave) || 1),
    z: state.zombies.slice(0, getCoopWorldMaxZombies()).map((zombie) => {
      if (!Number.isFinite(Number(zombie.id)) || Number(zombie.id) <= 0) {
        zombie.id = zombieIdSeed++;
      }

      const xNorm = Math.round(clamp((Number(zombie.x) || 0) / baseWidth, 0, 1) * 10000);
      const yNorm = Math.round(clamp((Number(zombie.y) || 0) / baseHeight, 0, 1) * 10000);
      const rNorm = Math.round(clamp((Number(zombie.r) || 14) / baseMin, 0.003, 0.3) * 10000);
      const hp10 = Math.max(0, Math.round((Number(zombie.hp) || 0) * 10));
      const maxHp10 = Math.max(1, Math.round((Number(zombie.maxHp) || 1) * 10));
      const typeCode = ZOMBIE_TYPE_TO_CODE[String(zombie.type || "normal")] ?? 0;
      const targetCode = zombie.targetCore === "ally" ? 1 : 0;

      return [Number(zombie.id), xNorm, yNorm, rNorm, hp10, maxHp10, typeCode, targetCode];
    }),
    b: state.bullets.slice(0, COOP_WORLD_MAX_BULLETS).map((bullet) => {
      const xNorm = Math.round(clamp((Number(bullet.x) || 0) / baseWidth, 0, 1) * 10000);
      const yNorm = Math.round(clamp((Number(bullet.y) || 0) / baseHeight, 0, 1) * 10000);
      const vxNorm = Math.round(clamp((Number(bullet.vx) || 0) / 2200, -1, 1) * 10000);
      const vyNorm = Math.round(clamp((Number(bullet.vy) || 0) / 2200, -1, 1) * 10000);
      const rNorm = Math.round(clamp((Number(bullet.r) || 3) / baseMin, 0.0015, 0.08) * 10000);
      const typeCode = bullet.type === "grenade" ? 1 : 0;
      const life100 = Math.max(0, Math.round((Number(bullet.life) || 0) * 100));
      return [xNorm, yNorm, vxNorm, vyNorm, rNorm, typeCode, life100];
    }),
    u: state.bursts.slice(0, COOP_WORLD_MAX_BURSTS).map((burst) => {
      const xNorm = Math.round(clamp((Number(burst.x) || 0) / baseWidth, 0, 1) * 10000);
      const yNorm = Math.round(clamp((Number(burst.y) || 0) / baseHeight, 0, 1) * 10000);
      const typeCode = BURST_TYPE_TO_CODE[String(burst.type || "shot")] ?? 0;
      const life100 = Math.max(0, Math.round((Number(burst.life) || 0) * 100));
      const max100 = Math.max(1, Math.round((Number(burst.max) || Number(burst.life) || 0.2) * 100));
      return [xNorm, yNorm, typeCode, life100, max100];
    }),
  };
}

function applyCoopWorldSnapshot(world) {
  if (!world || typeof world !== "object") return;
  const version = Number(world.v || world.version || 0);
  if (version && version <= coopState.worldVersion) return;

  const zombiesRaw = Array.isArray(world.z) ? world.z : world.zombies;
  if (!Array.isArray(zombiesRaw)) return;
  const bulletsRaw = Array.isArray(world.b) ? world.b : Array.isArray(world.bullets) ? world.bullets : null;
  const burstsRaw = Array.isArray(world.u) ? world.u : Array.isArray(world.bursts) ? world.bursts : null;

  coopState.worldVersion = version || Date.now();
  const prevScore = state.score;
  const prevWave = state.wave;
  state.score = Math.max(0, Number(world.s ?? world.score) || 0);
  state.wave = Math.max(1, Number(world.w ?? world.wave) || 1);
  state.spawnIntervalBase = Math.max(320, 1050 - state.wave * 60);

  const sourceWidth = Math.max(1, Number(world.sw ?? world.width) || width || 1);
  const sourceHeight = Math.max(1, Number(world.sh ?? world.height) || height || 1);
  const sourceMin = Math.max(1, Math.min(sourceWidth, sourceHeight));
  const targetWidth = Math.max(1, width || sourceWidth);
  const targetHeight = Math.max(1, height || sourceHeight);
  const targetMin = Math.max(1, Math.min(targetWidth, targetHeight));
  const scaleX = targetWidth / sourceWidth;
  const scaleY = targetHeight / sourceHeight;
  const radiusScale = clamp(targetMin / sourceMin, 0.55, 2.2);

  const prevById = new Map();
  for (const existing of state.zombies) {
    const existingId = Number(existing?.id || 0);
    if (existingId > 0) {
      prevById.set(existingId, existing);
    }
  }

  const next = [];
  const worldZombies = zombiesRaw.slice(0, getCoopWorldMaxZombies());
  const nowPerf = performance.now();

  for (let index = 0; index < worldZombies.length; index += 1) {
    const zombie = worldZombies[index];

    let incomingId = index + 1;
    let incomingX = 0;
    let incomingY = 0;
    let incomingR = 14;
    let incomingHp = 0;
    let incomingMaxHp = 1;
    let incomingType = "normal";
    let incomingTargetCore = "own";

    if (Array.isArray(zombie)) {
      incomingId = Math.max(1, Number(zombie[0]) || index + 1);
      const xNorm = clamp((Number(zombie[1]) || 0) / 10000, 0, 1);
      const yNorm = clamp((Number(zombie[2]) || 0) / 10000, 0, 1);
      const rNorm = clamp((Number(zombie[3]) || 0) / 10000, 0.003, 0.3);
      incomingX = xNorm * targetWidth;
      incomingY = yNorm * targetHeight;
      incomingR = Math.max(8, rNorm * targetMin);
      incomingHp = Math.max(0, (Number(zombie[4]) || 0) / 10);
      incomingMaxHp = Math.max(1, (Number(zombie[5]) || 10) / 10);
      incomingType = ZOMBIE_CODE_TO_TYPE[Math.round(Number(zombie[6]) || 0)] || "normal";
      incomingTargetCore = Number(zombie[7]) === 1 ? "ally" : "own";
    } else {
      incomingId = Math.max(1, Number(zombie.i ?? zombie.id) || index + 1);
      incomingX = (Number(zombie.x) || 0) * scaleX;
      incomingY = (Number(zombie.y) || 0) * scaleY;
      incomingR = Math.max(8, (Number(zombie.r) || 14) * radiusScale);
      incomingHp = Math.max(0, Number(zombie.h ?? zombie.hp) || 0);
      incomingMaxHp = Math.max(1, Number(zombie.m ?? zombie.maxHp) || 1);
      incomingType = String(zombie.t || zombie.type || "normal");
      incomingTargetCore = zombie.c === 1 || zombie.targetCore === "ally" ? "ally" : "own";
    }

    const existing = prevById.get(incomingId);
    if (existing) {
      const prevNetX = Number.isFinite(Number(existing.netX)) ? Number(existing.netX) : existing.x;
      const prevNetY = Number.isFinite(Number(existing.netY)) ? Number(existing.netY) : existing.y;
      const dtNet = Math.max(0.03, (nowPerf - (Number(existing.netAt) || nowPerf)) / 1000);
      const vx = (incomingX - prevNetX) / dtNet;
      const vy = (incomingY - prevNetY) / dtNet;

      existing.id = incomingId;
      existing.vx = clamp(vx, -520, 520);
      existing.vy = clamp(vy, -520, 520);
      existing.netX = incomingX;
      existing.netY = incomingY;
      existing.netAt = nowPerf;
      const teleportThreshold = Math.max(width, height) * 1.25;
      if (Math.abs(existing.x - incomingX) > teleportThreshold || Math.abs(existing.y - incomingY) > teleportThreshold) {
        existing.x = incomingX;
        existing.y = incomingY;
      }
      existing.x += (incomingX - existing.x) * 0.45;
      existing.y += (incomingY - existing.y) * 0.45;
      existing.r += (incomingR - existing.r) * 0.45;
      existing.hp = incomingHp;
      existing.maxHp = incomingMaxHp;
      existing.type = incomingType;
      existing.targetCore = incomingTargetCore;
      existing.dashCooldown = Number(existing.dashCooldown) || 0;
      existing.dashTimer = Number(existing.dashTimer) || 0;
      existing.baseSpeed = Number(existing.baseSpeed) || 0;
      existing.wobble = Number(existing.wobble) || 0;
      next.push(existing);
      continue;
    }

    next.push({
      id: incomingId,
      x: incomingX,
      y: incomingY,
      r: incomingR,
      hp: incomingHp,
      maxHp: incomingMaxHp,
      type: incomingType,
      wobble: Math.random() * Math.PI,
      dashCooldown: 0,
      dashTimer: 0,
      baseSpeed: 0,
      targetCore: incomingTargetCore,
      vx: 0,
      vy: 0,
      netX: incomingX,
      netY: incomingY,
      netAt: nowPerf,
    });
  }

  state.zombies = next;

  if (Array.isArray(bulletsRaw)) {
    const localVisualBullets = isGuestMirrorMode()
      ? state.bullets.filter((bullet) => bullet?.origin === "local" && Number(bullet.life) > 0)
      : [];
    const nextBullets = [];
    const worldBullets = bulletsRaw.slice(0, COOP_WORLD_MAX_BULLETS);
    for (let index = 0; index < worldBullets.length; index += 1) {
      const bullet = worldBullets[index];
      if (Array.isArray(bullet)) {
        const xNorm = clamp((Number(bullet[0]) || 0) / 10000, 0, 1);
        const yNorm = clamp((Number(bullet[1]) || 0) / 10000, 0, 1);
        const vxNorm = clamp((Number(bullet[2]) || 0) / 10000, -1, 1);
        const vyNorm = clamp((Number(bullet[3]) || 0) / 10000, -1, 1);
        const rNorm = clamp((Number(bullet[4]) || 0) / 10000, 0.0015, 0.08);
        const typeCode = Math.round(Number(bullet[5]) || 0);
        const life100 = Math.max(0, Number(bullet[6]) || 0);
        nextBullets.push({
          x: xNorm * targetWidth,
          y: yNorm * targetHeight,
          vx: vxNorm * 2200,
          vy: vyNorm * 2200,
          r: Math.max(2, rNorm * targetMin),
          life: life100 / 100,
          damage: 0,
          splash: typeCode === 1 ? (WEAPONS.grenade.splash || 70) : 0,
          type: typeCode === 1 ? "grenade" : "bullet",
          origin: "remote",
        });
      } else if (bullet && typeof bullet === "object") {
        nextBullets.push({
          x: (Number(bullet.x) || 0) * scaleX,
          y: (Number(bullet.y) || 0) * scaleY,
          vx: Number(bullet.vx) || 0,
          vy: Number(bullet.vy) || 0,
          r: Math.max(2, (Number(bullet.r) || 3) * radiusScale),
          life: Math.max(0, Number(bullet.life) || 0.4),
          damage: 0,
          splash: Number(bullet.splash) || 0,
          type: String(bullet.type || "bullet"),
          origin: "remote",
        });
      }
    }
    state.bullets = [...nextBullets, ...localVisualBullets].slice(0, COOP_WORLD_MAX_BULLETS + 16);
  }

  if (Array.isArray(burstsRaw)) {
    const localVisualBursts = isGuestMirrorMode()
      ? state.bursts.filter((burst) => burst?.origin === "local" && Number(burst.life) > 0)
      : [];
    const nextBursts = [];
    const worldBursts = burstsRaw.slice(0, COOP_WORLD_MAX_BURSTS);
    for (let index = 0; index < worldBursts.length; index += 1) {
      const burst = worldBursts[index];
      if (Array.isArray(burst)) {
        const xNorm = clamp((Number(burst[0]) || 0) / 10000, 0, 1);
        const yNorm = clamp((Number(burst[1]) || 0) / 10000, 0, 1);
        const typeCode = Math.round(Number(burst[2]) || 0);
        const life100 = Math.max(0, Number(burst[3]) || 0);
        const max100 = Math.max(1, Number(burst[4]) || life100 || 20);
        nextBursts.push({
          x: xNorm * targetWidth,
          y: yNorm * targetHeight,
          type: BURST_CODE_TO_TYPE[typeCode] || "shot",
          life: life100 / 100,
          max: max100 / 100,
          origin: "remote",
        });
      } else if (burst && typeof burst === "object") {
        nextBursts.push({
          x: (Number(burst.x) || 0) * scaleX,
          y: (Number(burst.y) || 0) * scaleY,
          type: String(burst.type || "shot"),
          life: Math.max(0, Number(burst.life) || 0.15),
          max: Math.max(0.05, Number(burst.max) || Number(burst.life) || 0.15),
          origin: "remote",
        });
      }
    }
    state.bursts = [...nextBursts, ...localVisualBursts].slice(0, COOP_WORLD_MAX_BURSTS + 14);
  }

  if (prevScore !== state.score || prevWave !== state.wave || !isGuestMirrorMode()) {
    updateHud();
  }
}

function updateRemoteTracers(dt) {
  if (!coopState.remoteTracers || coopState.remoteTracers.length === 0) return;
  const decay = isGuestMirrorMode() ? dt * 1.4 : dt;
  for (let i = coopState.remoteTracers.length - 1; i >= 0; i -= 1) {
    const tracer = coopState.remoteTracers[i];
    tracer.life -= decay;
    if (tracer.life <= 0) {
      coopState.remoteTracers.splice(i, 1);
    }
  }
  if (isGuestMirrorMode() && coopState.remoteTracers.length > 28) {
    coopState.remoteTracers.splice(0, coopState.remoteTracers.length - 28);
  }
}

function pushRemoteTracer(tracer) {
  if (!coopState.remoteTracers) {
    coopState.remoteTracers = [];
  }
  coopState.remoteTracers.push(tracer);
  const limit = isGuestMirrorMode() ? 28 : 60;
  if (coopState.remoteTracers.length > limit) {
    coopState.remoteTracers.splice(0, coopState.remoteTracers.length - limit);
  }
}

function makeServerTimestamp() {
  return Date.now();
}

function createCoopError(code, message = "") {
  const error = new Error(message || code || "coop-error");
  error.code = code || "coop-error";
  return error;
}

function withTimeout(promise, timeoutMs = 5000, code = "coop-timeout") {
  let timer = null;
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(createCoopError(code)), timeoutMs);
    }),
  ]).finally(() => {
    if (timer) clearTimeout(timer);
  });
}

function getCoopWsUrl() {
  let queryValue = "";
  try {
    queryValue = new URLSearchParams(window.location.search).get("coopWs") || "";
  } catch (error) {}

  const raw = String(
    queryValue || window.COOP_WS_URL || window.FIREBASE_CONFIG?.coopWsUrl || ""
  ).trim();
  if (!raw) return "";
  if (/^wss?:\/\//i.test(raw)) return raw;
  if (/^https?:\/\//i.test(raw)) return raw.replace(/^http/i, "ws");
  return "";
}

function getCoopWsHealthUrl() {
  const wsUrl = getCoopWsUrl();
  if (!wsUrl) return "";
  const httpUrl = wsUrl.replace(/^wss?:\/\//i, (match) =>
    match.toLowerCase() === "wss://" ? "https://" : "http://"
  );
  return httpUrl.endsWith("/") ? `${httpUrl}health` : `${httpUrl}/health`;
}

function isCoopWsEnabled() {
  return Boolean(getCoopWsUrl());
}

function isCoopBackendReady() {
  if (isCoopWsEnabled()) return true;
  return Boolean(cloudState.ready && cloudState.rtdb);
}

function getCoopSyncIntervalMs(forceInput = false) {
  if (isCoopWsEnabled()) {
    return forceInput ? COOP_INPUT_SYNC_INTERVAL_WS_MS : COOP_SYNC_INTERVAL_WS_MS;
  }
  return forceInput ? COOP_INPUT_SYNC_INTERVAL_MS : COOP_SYNC_INTERVAL_MS;
}

function getCoopWorldSyncIntervalMs() {
  return isCoopWsEnabled() ? COOP_WORLD_SYNC_INTERVAL_WS_MS : COOP_WORLD_SYNC_INTERVAL_MS;
}

function getCoopWorldMaxZombies() {
  return isCoopWsEnabled() ? COOP_WORLD_MAX_ZOMBIES_WS : COOP_WORLD_MAX_ZOMBIES;
}

async function warmupCoopWsServer() {
  if (!isCoopWsEnabled()) return;
  const healthUrl = getCoopWsHealthUrl();
  if (!healthUrl) return;
  try {
    await withTimeout(
      fetch(healthUrl, {
        method: "GET",
        cache: "no-store",
        credentials: "omit",
      }),
      22000,
      "coop-ws-warmup-timeout"
    );
  } catch (error) {
    // Ignore warmup failures; ws connect path will still handle retries/timeouts.
  }
}

function makeCoopSnapshot(value) {
  return {
    exists() {
      return value !== null && value !== undefined;
    },
    val() {
      return value;
    },
  };
}

function clearCoopWsReconnectTimer() {
  if (coopWs.reconnectTimer) {
    clearTimeout(coopWs.reconnectTimer);
    coopWs.reconnectTimer = null;
  }
}

function clearCoopWsPendingRequests(reasonCode = "coop-ws-disconnected") {
  for (const [rid, pending] of coopWs.requests.entries()) {
    clearTimeout(pending.timer);
    pending.reject(createCoopError(reasonCode));
    coopWs.requests.delete(rid);
  }
}

function handleCoopWsMessage(rawData) {
  let message = null;
  try {
    message = JSON.parse(String(rawData || "{}"));
  } catch (error) {
    return;
  }
  if (!message || typeof message !== "object") return;

  if (message.type === "response" && message.rid) {
    const rid = String(message.rid);
    const pending = coopWs.requests.get(rid);
    if (!pending) return;
    clearTimeout(pending.timer);
    coopWs.requests.delete(rid);
    if (message.ok === false) {
      pending.reject(createCoopError(message.error || "coop-ws-response-error"));
    } else {
      pending.resolve(message.data);
    }
    return;
  }

  if (message.type === "snapshot" && message.sid) {
    const sid = String(message.sid);
    const sub = coopWs.subs.get(sid);
    if (!sub || typeof sub.onValue !== "function") return;
    try {
      sub.onValue(makeCoopSnapshot(message.data));
    } catch (error) {
      if (typeof sub.onError === "function") {
        sub.onError(error);
      }
    }
  }
}

function scheduleCoopWsReconnect() {
  if (!isCoopWsEnabled()) return;
  if (coopWs.reconnectTimer) return;
  if (coopWs.subs.size === 0 && !coopState.active) return;

  const delay = coopWs.reconnectDelayMs;
  coopWs.reconnectTimer = setTimeout(() => {
    coopWs.reconnectTimer = null;
    ensureCoopWsConnected().catch(() => {});
  }, delay);
  coopWs.reconnectDelayMs = Math.min(
    COOP_WS_RECONNECT_MAX_MS,
    Math.round(coopWs.reconnectDelayMs * 1.6)
  );
}

function resubscribeCoopWs() {
  if (!coopWs.connected || !coopWs.socket) return;
  for (const [sid, sub] of coopWs.subs.entries()) {
    try {
      coopWs.socket.send(
        JSON.stringify({
          type: "request",
          rid: `resub-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
          action: "subscribe",
          kind: sub.kind,
          roomId: sub.roomId,
          payload: { sid },
        })
      );
    } catch (error) {
      if (typeof sub.onError === "function") {
        sub.onError(error);
      }
    }
  }
}

async function ensureCoopWsConnected() {
  const wsUrl = getCoopWsUrl();
  if (!wsUrl) {
    throw createCoopError("coop-ws-disabled");
  }

  if (
    coopWs.connected &&
    coopWs.socket &&
    coopWs.socket.readyState === WebSocket.OPEN &&
    coopWs.url === wsUrl
  ) {
    return coopWs.socket;
  }
  if (coopWs.connectPromise) {
    return coopWs.connectPromise;
  }

  if (coopWs.socket) {
    try {
      coopWs.socket.close();
    } catch (error) {}
  }

  coopWs.url = wsUrl;
  clearCoopWsReconnectTimer();

  coopWs.connecting = true;
  coopWs.connectPromise = new Promise((resolve, reject) => {
    let settled = false;
    let connectTimer = null;
    let socket = null;

    const failConnect = (error) => {
      if (settled) return;
      settled = true;
      if (connectTimer) clearTimeout(connectTimer);
      coopWs.connected = false;
      coopWs.connecting = false;
      coopWs.connectPromise = null;
      reject(error);
    };

    const finishConnect = () => {
      if (settled) return;
      settled = true;
      if (connectTimer) clearTimeout(connectTimer);
      coopWs.connected = true;
      coopWs.connecting = false;
      coopWs.connectPromise = null;
      coopWs.reconnectDelayMs = COOP_WS_RECONNECT_BASE_MS;
      resolve(socket);
      resubscribeCoopWs();
    };

    try {
      socket = new WebSocket(wsUrl);
    } catch (error) {
      failConnect(createCoopError("coop-ws-open-failed", error?.message || ""));
      return;
    }

    coopWs.socket = socket;
    connectTimer = setTimeout(() => {
      failConnect(createCoopError("coop-ws-connect-timeout"));
      try {
        socket.close();
      } catch (error) {}
    }, COOP_WS_REQUEST_TIMEOUT_MS);

    socket.onopen = () => {
      finishConnect();
    };

    socket.onmessage = (event) => {
      handleCoopWsMessage(event.data);
    };

    socket.onerror = () => {};

    socket.onclose = () => {
      const wasConnected = coopWs.connected;
      coopWs.connected = false;
      coopWs.connecting = false;
      coopWs.socket = null;
      coopWs.connectPromise = null;
      clearCoopWsPendingRequests("coop-ws-disconnected");
      if (!settled) {
        failConnect(createCoopError("coop-ws-closed"));
      } else if (wasConnected) {
        scheduleCoopWsReconnect();
      }
    };
  });

  return coopWs.connectPromise;
}

function coopWsSendRequest(message) {
  if (!coopWs.socket || coopWs.socket.readyState !== WebSocket.OPEN) {
    throw createCoopError("coop-ws-not-open");
  }
  coopWs.socket.send(JSON.stringify(message));
}

async function coopWsSendFast(action, kind, roomId, payload = null) {
  await ensureCoopWsConnected();
  const rid = `ff-${Date.now()}-${coopWs.nextRequestId++}`;
  coopWsSendRequest({
    type: "request",
    rid,
    action,
    kind,
    roomId,
    payload,
  });
}

async function coopWsRequest(action, kind, roomId, payload = null, timeoutMs = COOP_WS_REQUEST_TIMEOUT_MS) {
  await ensureCoopWsConnected();
  const rid = `req-${Date.now()}-${coopWs.nextRequestId++}`;

  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      coopWs.requests.delete(rid);
      reject(createCoopError("coop-ws-request-timeout"));
    }, timeoutMs);

    coopWs.requests.set(rid, { resolve, reject, timer });

    try {
      coopWsSendRequest({
        type: "request",
        rid,
        action,
        kind,
        roomId,
        payload,
      });
    } catch (error) {
      clearTimeout(timer);
      coopWs.requests.delete(rid);
      reject(createCoopError("coop-ws-send-failed", error?.message || ""));
    }
  });
}

function coopWsSubscribe(kind, roomId, onValue, onError) {
  const sid = `sub-${Date.now()}-${coopWs.nextSubId++}`;
  coopWs.subs.set(sid, {
    sid,
    kind,
    roomId,
    onValue,
    onError,
  });

  coopWsRequest("subscribe", kind, roomId, { sid }, COOP_WS_REQUEST_TIMEOUT_MS + 1200)
    .then((data) => {
      const sub = coopWs.subs.get(sid);
      if (!sub || typeof sub.onValue !== "function") return;
      sub.onValue(makeCoopSnapshot(data));
    })
    .catch((error) => {
      const sub = coopWs.subs.get(sid);
      if (!sub) return;
      coopWs.subs.delete(sid);
      if (typeof sub.onError === "function") {
        sub.onError(error);
      }
    });

  return sid;
}

function coopWsUnsubscribe(sid) {
  if (!sid) return;
  const sub = coopWs.subs.get(sid);
  if (!sub) return;
  coopWs.subs.delete(sid);
  coopWsRequest("unsubscribe", sub.kind, sub.roomId, { sid }, 2200).catch(() => {});
}

function closeCoopWsIfIdle() {
  if (coopWs.subs.size > 0) return;
  if (coopState.active) return;
  clearCoopWsReconnectTimer();
  clearCoopWsPendingRequests("coop-ws-manual-close");
  if (coopWs.socket) {
    try {
      coopWs.socket.close();
    } catch (error) {}
  }
  coopWs.socket = null;
  coopWs.connected = false;
  coopWs.connecting = false;
  coopWs.connectPromise = null;
}

function createCoopWsRef(kind, roomId) {
  if (!kind || !roomId) return null;
  const listeners = new Map();

  return {
    once(eventName) {
      if (eventName !== "value") {
        return Promise.reject(createCoopError("coop-ws-unsupported-event"));
      }
      return coopWsRequest("once", kind, roomId).then((data) => makeCoopSnapshot(data));
    },
    set(value) {
      return coopWsRequest("set", kind, roomId, value).then(() => null);
    },
    update(value) {
      return coopWsRequest("update", kind, roomId, value).then(() => null);
    },
    remove() {
      return coopWsRequest("remove", kind, roomId).then(() => null);
    },
    on(eventName, onValue, onError) {
      if (eventName !== "value" || typeof onValue !== "function") return;
      const sid = coopWsSubscribe(kind, roomId, onValue, onError);
      listeners.set(onValue, sid);
    },
    off(eventName, onValue) {
      if (eventName !== "value") return;
      if (typeof onValue === "function") {
        const sid = listeners.get(onValue);
        if (!sid) return;
        listeners.delete(onValue);
        coopWsUnsubscribe(sid);
        return;
      }
      for (const sid of listeners.values()) {
        coopWsUnsubscribe(sid);
      }
      listeners.clear();
    },
  };
}

function getRtdbUrlCandidates(config = window.FIREBASE_CONFIG || {}) {
  const projectId = String(config.projectId || "").trim();
  const list = [];
  const pushUnique = (value) => {
    const normalized = String(value || "").trim();
    if (!normalized) return;
    if (!list.includes(normalized)) list.push(normalized);
  };
  pushUnique(config.databaseURL);
  if (projectId) {
    pushUnique(`https://${projectId}-default-rtdb.firebaseio.com`);
    pushUnique(`https://${projectId}-default-rtdb.europe-west1.firebasedatabase.app`);
    pushUnique(`https://${projectId}-default-rtdb.us-central1.firebasedatabase.app`);
    pushUnique(`https://${projectId}-default-rtdb.asia-southeast1.firebasedatabase.app`);
  }
  return list;
}

async function trySwitchRtdbInstance(probeTimeoutMs = 4500) {
  if (!cloudState.ready || typeof window.firebase?.database !== "function") return false;
  const candidates = cloudState.rtdbCandidates?.length
    ? [...cloudState.rtdbCandidates]
    : getRtdbUrlCandidates(window.FIREBASE_CONFIG || {});
  if (!candidates.includes("")) candidates.push("");

  const currentUrl = String(cloudState.rtdbUrl || "").trim();
  const ordered = [
    ...candidates.filter((url) => String(url).trim() !== currentUrl),
    ...candidates.filter((url) => String(url).trim() === currentUrl),
  ];

  let lastError = null;
  for (const candidate of ordered) {
    let instance = null;
    try {
      instance = candidate ? window.firebase.database(candidate) : window.firebase.database();
    } catch (error) {
      lastError = error;
      continue;
    }

    try {
      const probeRef = instance.ref(`${CLOUD_KEYS.COOP_COLLECTION}/__probe__`);
      await withTimeout(probeRef.once("value"), probeTimeoutMs, "coop-probe-timeout");
      cloudState.rtdb = instance;
      cloudState.rtdbUrl = String(candidate || "").trim();
      cloudState.rtdbInitError = "";
      return true;
    } catch (error) {
      lastError = error;
      const code = String(error?.code || error?.message || "");
      if (code.includes("permission-denied") || code.includes("permission_denied")) {
        cloudState.rtdb = instance;
        cloudState.rtdbUrl = String(candidate || "").trim();
        cloudState.rtdbInitError = "";
        return true;
      }
    }
  }

  if (lastError) {
    cloudState.rtdbInitError = String(lastError?.message || lastError || "rtdb-switch-failed");
  }
  return false;
}

async function runCoopOpWithRepair(operation, timeoutCode) {
  if (isCoopWsEnabled()) {
    return operation();
  }
  try {
    return await operation();
  } catch (error) {
    const code = String(error?.code || error?.message || "");
    if (!code.includes(timeoutCode)) throw error;
    const switched = await trySwitchRtdbInstance();
    if (!switched) throw error;
    return operation();
  }
}

function normalizeCoopCode(value) {
  return String(value || "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "")
    .slice(0, 6);
}

function generateCoopCode() {
  const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let code = "";
  for (let i = 0; i < 6; i += 1) {
    code += alphabet[Math.floor(Math.random() * alphabet.length)];
  }
  return code;
}

function getCoopDisplayName() {
  if (authState.user?.displayName) return authState.user.displayName;
  if (authState.user?.email) return authState.user.email.split("@")[0];
  return "Игрок";
}

function getCoopRoomRef(roomId) {
  if (!roomId) return null;
  if (isCoopWsEnabled()) {
    return createCoopWsRef("room", roomId);
  }
  if (!cloudState.rtdb) return null;
  return cloudState.rtdb.ref(`${CLOUD_KEYS.COOP_COLLECTION}/${roomId}`);
}

function getCoopWorldRef(roomId) {
  if (!roomId) return null;
  if (isCoopWsEnabled()) {
    return createCoopWsRef("world", roomId);
  }
  if (!cloudState.rtdb) return null;
  return cloudState.rtdb.ref(`${CLOUD_KEYS.COOP_WORLD_COLLECTION}/${roomId}`);
}

function clearCoopSubscription() {
  if (coopState.unsub) {
    try {
      coopState.unsub();
    } catch (error) {}
    coopState.unsub = null;
  }
  if (coopState.worldUnsub) {
    try {
      coopState.worldUnsub();
    } catch (error) {}
    coopState.worldUnsub = null;
  }
  closeCoopWsIfIdle();
}

function resetCoopLocalState() {
  clearCoopSubscription();
  coopState.active = false;
  coopState.roomId = "";
  coopState.role = null;
  coopState.roomStatus = "idle";
  coopState.sharedWave = 1;
  coopState.remoteName = "";
  coopState.remoteHp = 0;
  coopState.remoteMaxHp = 5;
  coopState.remoteAlive = false;
  coopState.remoteWave = 1;
  coopState.remoteScore = 0;
  coopState.remoteAimX = 0.5;
  coopState.remoteAimY = 0.5;
  coopState.remotePointerDown = false;
  coopState.remoteWeapon = "blaster";
  coopState.remoteInputAt = 0;
  coopState.remoteLastPacketAt = 0;
  coopState.remoteMuzzle = 0;
  coopState.remoteShotTimer = 0;
  coopState.remoteBeamActive = false;
  coopState.remoteBeamTarget = null;
  coopState.remoteTracers = [];
  coopState.remoteConnected = false;
  coopState.roomWriteInFlight = false;
  coopState.roomPendingPayload = null;
  coopState.worldWriteInFlight = false;
  coopState.worldPendingSnapshot = null;
  coopState.lastSyncAt = 0;
  coopState.lastInputSyncAt = 0;
  coopState.lastWorldSyncAt = 0;
  coopState.worldVersion = 0;
  coopState.lastRoomPayloadKey = "";
  coopState.lastWorldPayloadKey = "";
  coopState.resultSaved = false;
  coopState.lastGuestFxAt = 0;
  coopState.lastGuestRenderAt = 0;
  if (playBtn) playBtn.disabled = false;
  closeCoopWsIfIdle();
  resizeCanvas();
  updateCoopUI();
}

function updateCoopUI() {
  const hasCloud = Boolean(cloudState.ready && isCoopBackendReady());
  const isLoggedIn = Boolean(authState.user && !authState.loading);
  const controlsBusy = Boolean(coopState.actionBusy);

  if (coopCodeInput) {
    const normalized = normalizeCoopCode(coopCodeInput.value);
    if (coopCodeInput.value !== normalized) {
      coopCodeInput.value = normalized;
    }
  }

  if (coopCreateBtn) {
    coopCreateBtn.disabled = controlsBusy;
    coopCreateBtn.textContent = controlsBusy ? "Создаём..." : "Создать лобби";
  }
  if (coopJoinBtn) coopJoinBtn.disabled = controlsBusy;
  if (coopCodeInput) coopCodeInput.disabled = controlsBusy;
  if (coopLeaveBtn) coopLeaveBtn.hidden = !coopState.active;

  if (coopRoomCode) {
    coopRoomCode.textContent = coopState.active ? `Лобби: ${coopState.roomId}` : "Лобби: —";
  }

  if (coopOwnCore) {
    coopOwnCore.textContent = `HP: ${Math.max(0, state.hp)}/${Math.max(1, state.maxHp)}`;
  }

  if (coopAllyLabel) {
    coopAllyLabel.textContent = coopState.remoteConnected
      ? `Ядро: ${coopState.remoteName || "Союзник"}`
      : "Ядро союзника";
  }

  if (coopAllyCore) {
    if (coopState.remoteConnected) {
      const hp = Math.max(0, Number(coopState.remoteHp) || 0);
      const maxHp = Math.max(1, Number(coopState.remoteMaxHp) || 5);
      coopAllyCore.textContent = `HP: ${hp}/${maxHp}`;
    } else {
      coopAllyCore.textContent = "HP: —";
    }
  }

  if (coopCores) {
    coopCores.hidden = !(coopState.active && coopState.remoteConnected);
  }

  if (!coopStatus) return;

  if (!hasCloud) {
    coopStatus.textContent = isCoopWsEnabled()
      ? "Кооп недоступен: WebSocket-сервер не отвечает."
      : cloudState.rtdbInitError
      ? "Кооп недоступен: не инициализировался Realtime Database."
      : "Кооп требует Firebase. Проверь конфигурацию проекта.";
    return;
  }
  if (!isLoggedIn) {
    coopStatus.textContent = "Войди в аккаунт, чтобы создать или присоединиться к лобби.";
    return;
  }
  if (!coopState.active) {
    coopStatus.textContent = "Создай лобби и передай код другу, либо войди по коду.";
    return;
  }

  if (!coopState.remoteConnected) {
    coopStatus.textContent =
      coopState.role === "host"
        ? "Лобби создано. Ждём второго игрока по коду."
        : "Ожидание подтверждения лобби...";
    return;
  }

  if (coopState.roomStatus === "waiting") {
    coopStatus.textContent =
      coopState.role === "host"
        ? "Союзник подключён. Нажми «Начать», чтобы запустить кооп-матч."
        : "Подключено. Ждём запуск матча от хоста.";
    return;
  }

  if (coopState.roomStatus === "running") {
    if (state.status === "ko") {
      coopStatus.textContent = "Твоё ядро уничтожено. Союзник ещё в бою.";
    } else {
      coopStatus.textContent = `Матч идёт. Союзник: волна ${coopState.remoteWave}, очки ${coopState.remoteScore}.`;
    }
    return;
  }

  if (coopState.roomStatus === "ended") {
    coopStatus.textContent = "Кооп завершён. Можно создать новое лобби.";
    return;
  }

  coopStatus.textContent = "Состояние лобби обновляется...";
}

function mergeCoopPayload(base, extra) {
  const merged = base ? { ...base } : {};
  if (!extra || typeof extra !== "object") return merged;
  for (const [key, value] of Object.entries(extra)) {
    merged[key] = value;
  }
  return merged;
}

async function flushCoopRoomUpdate(payload) {
  if (!coopState.active || !coopState.roomId) return;
  const ref = getCoopRoomRef(coopState.roomId);
  if (!ref) return;
  coopState.roomWriteInFlight = true;
  try {
    await ref.update(payload);
  } catch (error) {
    console.error("Coop update error:", error);
  } finally {
    coopState.roomWriteInFlight = false;
    if (coopState.roomPendingPayload && coopState.active && coopState.roomId) {
      const nextPayload = coopState.roomPendingPayload;
      coopState.roomPendingPayload = null;
      flushCoopRoomUpdate(nextPayload);
    } else {
      coopState.roomPendingPayload = null;
    }
  }
}

function updateCoopRoom(payload) {
  if (!coopState.active || !coopState.roomId) return;
  if (!payload || typeof payload !== "object") return;
  if (isCoopWsEnabled()) {
    coopWsSendFast("update", "room", coopState.roomId, payload).catch((error) => {
      console.error("Coop WS room fast sync error:", error);
    });
    return;
  }
  if (coopState.roomWriteInFlight) {
    coopState.roomPendingPayload = mergeCoopPayload(coopState.roomPendingPayload, payload);
    return;
  }
  flushCoopRoomUpdate(payload);
}

async function finalizeCoopRoomIfHost(reason = "Оба ядра уничтожены.") {
  if (!coopState.active || coopState.role !== "host" || !coopState.roomId) return;
  const ref = getCoopRoomRef(coopState.roomId);
  if (!ref) return;

  try {
    const snap = await ref.once("value");
    if (!snap.exists()) return;
    const data = snap.val() || {};
    if (data.status === "ended") return;
    await ref.update({
      status: "ended",
      endedReason: reason,
      endedAt: makeServerTimestamp(),
      updatedAt: makeServerTimestamp(),
    });
    const worldRef = getCoopWorldRef(coopState.roomId);
    if (worldRef) {
      await worldRef.remove();
    }
  } catch (error) {
    console.error("Coop finalize error:", error);
  }
}

function handleCoopMatchEnded(reason = "Оба ядра уничтожены.") {
  coopState.roomStatus = "ended";
  coopState.remotePointerDown = false;
  coopState.remoteBeamActive = false;
  coopState.remoteBeamTarget = null;
  coopState.remoteTracers = [];

  if (state.status !== "over") {
    state.running = false;
    state.status = "over";
    state.pointer.down = false;
    state.beamActive = false;
    showOverlay("Кооп завершён", reason, "Играть");
    playBtn.disabled = false;
    updatePauseButton();
    updateMusicMix();
  }

  if (authState.user && !coopState.resultSaved) {
    saveGameResult(state.score, state.wave);
    updateLeaderboard();
    coopState.resultSaved = true;
  }

  updateCoopUI();
}

function handleLocalCoreDestroyed() {
  if (state.status === "ko" || state.status === "over") return;

  state.hp = 0;
  state.running = false;
  state.status = "ko";
  state.pointer.down = false;
  state.beamActive = false;
  updateHud();

  const teammateAlive = coopState.remoteConnected && coopState.remoteAlive;
  if (teammateAlive) {
    showOverlay("Твоё ядро уничтожено", "Союзник ещё в бою. Ждём завершение матча.", "Ожидание...");
    playBtn.disabled = true;
  } else {
    showOverlay("Кооп завершён", "Оба ядра уничтожены.", "Играть");
    playBtn.disabled = false;
  }

  updatePauseButton();
  updateMusicMix();
  syncCoopState(performance.now(), true);

  if (!teammateAlive) {
    if (coopState.role === "host") {
      finalizeCoopRoomIfHost("Оба ядра уничтожены.");
    }
    handleCoopMatchEnded("Оба ядра уничтожены.");
  }
}

function handleCoopSnapshot(roomId, snap) {
  if (!snap.exists()) {
    showShopMessage("Лобби закрыто.", "error");
    resetCoopLocalState();
    return;
  }

  const data = snap.val() || {};
  const host = data.host || null;
  const guest = data.guest || null;
  const uid = authState.user?.uid || "";

  let role = null;
  if (host?.uid === uid) role = "host";
  if (guest?.uid === uid) role = "guest";

  if (!role) {
    showShopMessage("Ты больше не состоишь в этом лобби.", "error");
    resetCoopLocalState();
    return;
  }

  const wasGuestMirror = isGuestMirrorMode();

  coopState.active = true;
  coopState.roomId = roomId;
  coopState.role = role;
  coopState.roomStatus = data.status || "waiting";
  coopState.sharedWave = Math.max(1, Number(data.sharedWave || 1));

  const remoteSlot = role === "host" ? guest : host;
  coopState.remoteConnected = Boolean(remoteSlot && remoteSlot.uid);
  coopState.remoteName = remoteSlot?.name || "";
  coopState.remoteHp = Math.max(0, Number(remoteSlot?.hp || 0));
  coopState.remoteMaxHp = Math.max(1, Number(remoteSlot?.maxHp || 5));
  coopState.remoteAlive = Boolean(remoteSlot && remoteSlot.alive !== false && coopState.remoteHp > 0);
  coopState.remoteWave = Math.max(1, Number(remoteSlot?.wave || 1));
  coopState.remoteScore = Math.max(0, Number(remoteSlot?.score || 0));
  if (remoteSlot && Number.isFinite(Number(remoteSlot.aimX))) {
    coopState.remoteAimX = clamp(Number(remoteSlot.aimX), 0, 1);
  }
  if (remoteSlot && Number.isFinite(Number(remoteSlot.aimY))) {
    coopState.remoteAimY = clamp(Number(remoteSlot.aimY), 0, 1);
  }
  coopState.remoteWeapon = WEAPONS[remoteSlot?.weapon] ? remoteSlot.weapon : "blaster";
  coopState.remoteInputAt = Math.max(0, Number(remoteSlot?.inputAt || 0));
  if (coopState.remoteConnected) {
    coopState.remoteLastPacketAt = performance.now();
  } else {
    coopState.remoteLastPacketAt = 0;
  }
  const inputFresh = coopState.remoteConnected && performance.now() - coopState.remoteLastPacketAt <= 2600;
  coopState.remotePointerDown =
    Boolean(remoteSlot?.pointerDown) &&
    inputFresh &&
    coopState.remoteAlive &&
    coopState.roomStatus === "running";

  const ownSlot = role === "host" ? host : guest;
  if (ownSlot && role === "guest" && coopState.roomStatus === "running") {
    state.maxHp = Math.max(1, Number(ownSlot.maxHp || state.maxHp));
    state.hp = Math.max(0, Number(ownSlot.hp || state.hp));
    if (state.hp <= 0 && state.status === "running") {
      handleLocalCoreDestroyed();
    } else {
      updateHud();
    }
  }

  if (coopState.roomStatus === "running" && coopState.role === "guest" && state.status === "running") {
    const targetWave = Math.max(1, coopState.sharedWave);
    if (state.wave !== targetWave) {
      state.wave = targetWave;
      state.spawnIntervalBase = Math.max(320, 1050 - state.wave * 60);
      updateHud();
    }
  }

  if (coopState.roomStatus === "running" && state.status === "idle") {
    startRun({ reset: true });
    showShopMessage("Кооп-матч начался!", "success");
  }

  if (coopState.roomStatus === "ended") {
    const reason = data.endedReason || "Оба ядра уничтожены.";
    handleCoopMatchEnded(reason);
  } else if (coopState.role === "host" && coopState.roomStatus === "running" && host && guest) {
    const hostAlive = host.alive !== false && Number(host.hp || 0) > 0;
    const guestAlive = guest.alive !== false && Number(guest.hp || 0) > 0;
    if (!hostAlive && !guestAlive) {
      finalizeCoopRoomIfHost("Оба ядра уничтожены.");
    }
  }

  const guestMirrorNow = isGuestMirrorMode();
  if (wasGuestMirror !== guestMirrorNow) {
    coopState.lastGuestFxAt = 0;
    coopState.lastGuestRenderAt = 0;
    resizeCanvas();
  }

  updateCoopUI();
}

function handleCoopWorldSnapshot(roomId, snap) {
  if (!coopState.active || coopState.roomId !== roomId) return;
  if (coopState.role !== "guest" || coopState.roomStatus !== "running") return;
  if (!snap.exists()) return;
  const world = snap.val() || null;
  applyCoopWorldSnapshot(world);
}

function subscribeCoopRoom(roomId) {
  clearCoopSubscription();
  const roomRef = getCoopRoomRef(roomId);
  if (!roomRef) return;

  const onValue = (snap) => {
    handleCoopSnapshot(roomId, snap);
  };
  const onError = (error) => {
    console.error("Coop snapshot error:", error);
    showShopMessage("Ошибка соединения с лобби.", "error");
    resetCoopLocalState();
  };

  roomRef.on("value", onValue, onError);
  coopState.unsub = () => {
    roomRef.off("value", onValue);
  };

  if (coopState.role === "guest") {
    const worldRef = getCoopWorldRef(roomId);
    if (worldRef) {
      const onWorldValue = (snap) => {
        handleCoopWorldSnapshot(roomId, snap);
      };
      const onWorldError = (error) => {
        console.error("Coop world snapshot error:", error);
      };
      worldRef.on("value", onWorldValue, onWorldError);
      coopState.worldUnsub = () => {
        worldRef.off("value", onWorldValue);
      };
    }
  }
}

function setCoopBusy(flag, statusText = "") {
  coopState.actionBusy = Boolean(flag);
  updateCoopUI();
  if (statusText && coopStatus) {
    coopStatus.textContent = statusText;
  }
}

async function ensureCoopSlotForNewLobbyAction() {
  if (!coopState.active) return true;
  if (coopState.roomStatus === "ended") {
    await leaveCoopLobby({ silent: true, skipRemoteUpdate: true });
    return true;
  }
  showShopMessage("Сначала выйди из текущего лобби.", "error");
  return false;
}

async function createCoopLobby() {
  if (coopState.actionBusy) return;
  setCoopBusy(true, "Создаём лобби...");

  if (!cloudState.ready || !isCoopBackendReady() || !authState.user) {
    setCoopBusy(false);
    showShopMessage("Войди в аккаунт и проверь кооп-сервер, чтобы создать лобби.", "error");
    return;
  }

  if (!(await ensureCoopSlotForNewLobbyAction())) {
    setCoopBusy(false);
    return;
  }

  if (isCoopWsEnabled()) {
    setCoopBusy(true, "Пробуждаем кооп-сервер...");
    await warmupCoopWsServer();
    setCoopBusy(true, "Создаём лобби...");
  }

  const playerName = getCoopDisplayName();
  let roomId = "";
  let created = false;

  try {
    for (let attempt = 0; attempt < 10; attempt += 1) {
      const candidate = generateCoopCode();
      const readCandidate = () => {
        const ref = getCoopRoomRef(candidate);
        if (!ref) throw createCoopError("coop-rtdb-missing");
        return withTimeout(ref.once("value"), 8000, "coop-read-timeout");
      };
      const snap = await runCoopOpWithRepair(readCandidate, "coop-read-timeout");
      if (snap.exists()) continue;

      const writeCandidate = () => {
        const ref = getCoopRoomRef(candidate);
        if (!ref) throw createCoopError("coop-rtdb-missing");
        return withTimeout(ref.set({
          roomId: candidate,
          status: "waiting",
          sharedWave: 1,
          createdAt: makeServerTimestamp(),
          updatedAt: makeServerTimestamp(),
          endedReason: "",
          host: {
            uid: authState.user.uid,
            name: playerName,
            hp: Math.max(0, state.hp),
            maxHp: Math.max(1, state.maxHp),
            alive: true,
            score: 0,
            wave: 1,
            aimX: getPointerNorm().x,
            aimY: getPointerNorm().y,
            pointerDown: false,
            weapon: "blaster",
            inputAt: Date.now(),
            updatedAt: Date.now(),
          },
          guest: null,
        }), 8000, "coop-write-timeout");
      };
      await runCoopOpWithRepair(writeCandidate, "coop-write-timeout");

      roomId = candidate;
      created = true;
      break;
    }
  } catch (error) {
    const code = String(error?.code || error?.message || "");
    if (
      code.includes("coop-read-timeout") ||
      code.includes("coop-write-timeout") ||
      code.includes("coop-ws-request-timeout") ||
      code.includes("coop-ws-connect-timeout")
    ) {
      const urlHint = !isCoopWsEnabled() && cloudState.rtdbUrl ? ` RTDB: ${cloudState.rtdbUrl}` : "";
      const wsHint = isCoopWsEnabled() ? " WS-сервер недоступен." : "";
      showShopMessage(`Сервер коопа не ответил вовремя.${urlHint}${wsHint}`, "error");
    } else if (code.includes("coop-ws")) {
      showShopMessage("Не удалось подключиться к WebSocket-серверу коопа.", "error");
    } else if (code.includes("permission-denied") || code.includes("permission_denied")) {
      showShopMessage("Realtime Database Rules блокируют создание лобби. Обнови правила коопа в Firebase.", "error");
    } else if (code.includes("coop-rtdb-missing")) {
      showShopMessage("Realtime Database не инициализирован. Проверь databaseURL в Firebase config.", "error");
    } else {
      showShopMessage("Ошибка создания лобби. Проверь интернет и попробуй снова.", "error");
    }
    setCoopBusy(false);
    return;
  }

  if (!created || !roomId) {
    showShopMessage("Не удалось создать лобби. Попробуй снова.", "error");
    setCoopBusy(false);
    return;
  }

  coopState.active = true;
  coopState.roomId = roomId;
  coopState.role = "host";
  coopState.roomStatus = "waiting";
  coopState.sharedWave = 1;
  coopState.remoteConnected = false;
  coopState.remoteName = "";
  coopState.remoteHp = 0;
  coopState.remoteMaxHp = 5;
  coopState.remoteAlive = false;
  coopState.remoteWave = 1;
  coopState.remoteScore = 0;
  coopState.resultSaved = false;
  coopState.lastSyncAt = 0;
  coopState.lastRoomPayloadKey = "";
  coopState.lastWorldPayloadKey = "";
  coopState.lastGuestFxAt = 0;
  coopState.lastGuestRenderAt = 0;
  setCoopBusy(false);
  subscribeCoopRoom(roomId);
  updateCoopUI();
  showShopMessage(`Лобби ${roomId} создано. Отправь код другу.`, "success");
}

async function joinCoopLobby() {
  if (coopState.actionBusy) return;
  setCoopBusy(true, "Подключаемся к лобби...");

  if (!cloudState.ready || !isCoopBackendReady() || !authState.user) {
    setCoopBusy(false);
    showShopMessage("Войди в аккаунт и проверь кооп-сервер, чтобы войти в лобби.", "error");
    return;
  }

  if (!(await ensureCoopSlotForNewLobbyAction())) {
    setCoopBusy(false);
    return;
  }

  if (isCoopWsEnabled()) {
    setCoopBusy(true, "Пробуждаем кооп-сервер...");
    await warmupCoopWsServer();
    setCoopBusy(true, "Подключаемся к лобби...");
  }

  const roomId = normalizeCoopCode(coopCodeInput?.value || "");
  if (!roomId || roomId.length < 6) {
    setCoopBusy(false);
    showShopMessage("Введи корректный 6-символьный код лобби.", "error");
    return;
  }

  const playerName = getCoopDisplayName();

  try {
    const readRoom = () => {
      const ref = getCoopRoomRef(roomId);
      if (!ref) throw createCoopError("coop-rtdb-missing");
      return withTimeout(ref.once("value"), 8000, "coop-read-timeout");
    };
    const snap = await runCoopOpWithRepair(readRoom, "coop-read-timeout");
    if (!snap.exists()) {
      throw new Error("room-not-found");
    }

    const data = snap.val() || {};
    if (data.status === "ended") {
      throw new Error("room-ended");
    }
    if (data.status === "running") {
      throw new Error("room-running");
    }
    if (!data.host || !data.host.uid) {
      throw new Error("room-broken");
    }
    if (data.host.uid === authState.user.uid) {
      throw new Error("room-self");
    }
    if (data.guest && data.guest.uid && data.guest.uid !== authState.user.uid) {
      throw new Error("room-full");
    }

    const writeRoom = () => {
      const ref = getCoopRoomRef(roomId);
      if (!ref) throw createCoopError("coop-rtdb-missing");
      return withTimeout(ref.update({
        guest: {
          uid: authState.user.uid,
          name: playerName,
          hp: Math.max(0, state.hp),
          maxHp: Math.max(1, state.maxHp),
          alive: true,
          score: 0,
          wave: 1,
          aimX: getPointerNorm().x,
          aimY: getPointerNorm().y,
          pointerDown: false,
          weapon: "blaster",
          inputAt: Date.now(),
          updatedAt: Date.now(),
        },
        status: "waiting",
        endedReason: "",
        updatedAt: makeServerTimestamp(),
      }), 8000, "coop-write-timeout");
    };
    await runCoopOpWithRepair(writeRoom, "coop-write-timeout");
  } catch (error) {
    const code = String(error?.code || error?.message || "");
    if (
      code.includes("coop-read-timeout") ||
      code.includes("coop-write-timeout") ||
      code.includes("coop-ws-request-timeout") ||
      code.includes("coop-ws-connect-timeout")
    ) {
      const urlHint = !isCoopWsEnabled() && cloudState.rtdbUrl ? ` RTDB: ${cloudState.rtdbUrl}` : "";
      const wsHint = isCoopWsEnabled() ? " WS-сервер недоступен." : "";
      showShopMessage(`Сервер коопа не ответил вовремя.${urlHint}${wsHint}`, "error");
    } else if (code.includes("coop-ws")) {
      showShopMessage("Не удалось подключиться к WebSocket-серверу коопа.", "error");
    } else if (code.includes("permission-denied") || code.includes("permission_denied")) {
      showShopMessage("Realtime Database Rules блокируют вход в лобби. Обнови правила коопа в Firebase.", "error");
    } else if (code.includes("coop-rtdb-missing")) {
      showShopMessage("Realtime Database не инициализирован. Проверь databaseURL в Firebase config.", "error");
    } else if (code === "room-not-found") showShopMessage("Лобби не найдено.", "error");
    else if (code === "room-full") showShopMessage("Лобби уже заполнено.", "error");
    else if (code === "room-running") showShopMessage("Матч уже идёт. Подключись к новому лобби.", "error");
    else if (code === "room-ended") showShopMessage("Это лобби уже завершено.", "error");
    else if (code === "room-self") showShopMessage("Это твоё лобби. Используй кнопку «Создать лобби» заново.", "error");
    else showShopMessage("Не удалось войти в лобби. Проверь код и попробуй снова.", "error");
    setCoopBusy(false);
    return;
  }

  coopState.active = true;
  coopState.roomId = roomId;
  coopState.role = "guest";
  coopState.roomStatus = "waiting";
  coopState.sharedWave = 1;
  coopState.remoteConnected = true;
  coopState.resultSaved = false;
  coopState.lastSyncAt = 0;
  coopState.lastRoomPayloadKey = "";
  coopState.lastWorldPayloadKey = "";
  coopState.lastGuestFxAt = 0;
  coopState.lastGuestRenderAt = 0;
  setCoopBusy(false);
  subscribeCoopRoom(roomId);
  updateCoopUI();
  showShopMessage(`Подключено к лобби ${roomId}.`, "success");
}

async function leaveCoopLobby({ silent = false, skipRemoteUpdate = false } = {}) {
  if (!coopState.active) return;

  const roomId = coopState.roomId;
  const role = coopState.role;
  const roomStatus = coopState.roomStatus;

  if (!skipRemoteUpdate && isCoopBackendReady() && roomId && role) {
    const ref = getCoopRoomRef(roomId);
    if (ref) {
      try {
        if (role === "host") {
          await ref.update({
            status: "ended",
            endedReason: "Хост закрыл лобби.",
            host: null,
            updatedAt: makeServerTimestamp(),
            endedAt: makeServerTimestamp(),
          });
        } else {
          if (roomStatus === "running") {
            await ref.update({
              status: "ended",
              endedReason: "Союзник покинул матч.",
              guest: null,
              updatedAt: makeServerTimestamp(),
              endedAt: makeServerTimestamp(),
            });
          } else {
            await ref.update({
              status: "waiting",
              guest: null,
              updatedAt: makeServerTimestamp(),
            });
          }
        }
      } catch (error) {
        console.error("Leave coop room error:", error);
      }
    }
  }

  if (!skipRemoteUpdate && isCoopBackendReady() && roomId && (role === "host" || roomStatus === "running")) {
    const worldRef = getCoopWorldRef(roomId);
    if (worldRef) {
      try {
        await worldRef.remove();
      } catch (error) {
        console.error("Leave coop world cleanup error:", error);
      }
    }
  }

  resetCoopLocalState();
  setCoopBusy(false);
  if (!silent) {
    showShopMessage("Ты вышел из кооп-лобби.", "success");
  }
}

async function flushCoopWorldSync(snapshot) {
  if (!coopState.active || coopState.role !== "host" || !coopState.roomId) return;
  const worldRef = getCoopWorldRef(coopState.roomId);
  if (!worldRef) return;
  coopState.worldWriteInFlight = true;
  try {
    await worldRef.set(snapshot);
  } catch (error) {
    console.error("Coop world sync error:", error);
  } finally {
    coopState.worldWriteInFlight = false;
    if (coopState.worldPendingSnapshot && coopState.active && coopState.role === "host" && coopState.roomId) {
      const pendingSnapshot = coopState.worldPendingSnapshot;
      coopState.worldPendingSnapshot = null;
      flushCoopWorldSync(pendingSnapshot);
    } else {
      coopState.worldPendingSnapshot = null;
    }
  }
}

function syncCoopWorldState(now) {
  if (!coopState.active || coopState.role !== "host" || !coopState.roomId) return;
  if (!isCoopBackendReady() || state.status !== "running" || !coopState.remoteConnected) return;
  if (now - coopState.lastWorldSyncAt < getCoopWorldSyncIntervalMs()) return;

  coopState.lastWorldSyncAt = now;
  const snapshot = serializeCoopWorldSnapshot();
  const snapshotKey = [
    snapshot.sw,
    snapshot.sh,
    snapshot.s,
    snapshot.w,
    (snapshot.z || [])
      .map((zombie) => (Array.isArray(zombie) ? zombie.join(":") : [zombie.i, zombie.x, zombie.y, zombie.r, zombie.h, zombie.c].join(":")))
      .join(";"),
    (snapshot.b || [])
      .map((bullet) => (Array.isArray(bullet) ? bullet.join(":") : [bullet.x, bullet.y, bullet.vx, bullet.vy, bullet.r, bullet.type, bullet.life].join(":")))
      .join(";"),
    (snapshot.u || [])
      .map((burst) => (Array.isArray(burst) ? burst.join(":") : [burst.x, burst.y, burst.type, burst.life, burst.max].join(":")))
      .join(";"),
  ].join("|");
  if (snapshotKey === coopState.lastWorldPayloadKey) {
    return;
  }
  coopState.lastWorldPayloadKey = snapshotKey;

  if (isCoopWsEnabled()) {
    coopWsSendFast("set", "world", coopState.roomId, snapshot).catch((error) => {
      console.error("Coop WS world fast sync error:", error);
    });
    return;
  }

  if (coopState.worldWriteInFlight) {
    coopState.worldPendingSnapshot = snapshot;
    return;
  }
  flushCoopWorldSync(snapshot);
}

function syncCoopState(now, force = false) {
  if (!coopState.active || !coopState.role || !coopState.roomId) return;
  if (!isCoopBackendReady() || !authState.user) return;

  const role = coopState.role;

  const syncInterval = getCoopSyncIntervalMs(force);
  if (now - coopState.lastSyncAt < syncInterval) return;
  const alive = state.hp > 0 && state.status !== "ko" && state.status !== "over";
  const pointerNorm = getPointerNorm();
  const aimX = quantizeNormCoord(pointerNorm.x, 180);
  const aimY = quantizeNormCoord(pointerNorm.y, 180);
  const pointerDown = Boolean(state.pointer.down && state.status === "running");
  const ownHp = Math.max(0, state.hp);
  const ownMaxHp = Math.max(1, state.maxHp);
  const ownScore = Math.max(0, state.score);
  const ownWave = Math.max(1, state.wave);

  const payload = {
    updatedAt: makeServerTimestamp(),
    [`${role}/name`]: getCoopDisplayName(),
    [`${role}/hp`]: ownHp,
    [`${role}/maxHp`]: ownMaxHp,
    [`${role}/alive`]: alive,
    [`${role}/score`]: ownScore,
    [`${role}/wave`]: ownWave,
    [`${role}/aimX`]: aimX,
    [`${role}/aimY`]: aimY,
    [`${role}/pointerDown`]: pointerDown,
    [`${role}/weapon`]: WEAPONS[state.weapon] ? state.weapon : "blaster",
    [`${role}/inputAt`]: Date.now(),
    [`${role}/updatedAt`]: Date.now(),
  };

  if (isGuestMirrorMode()) {
    delete payload[`${role}/hp`];
    delete payload[`${role}/maxHp`];
    delete payload[`${role}/alive`];
    delete payload[`${role}/score`];
    delete payload[`${role}/wave`];
  }

  if (role === "host") {
    payload.sharedWave = ownWave;
    if (coopState.remoteConnected) {
      payload["guest/hp"] = Math.max(0, Number(coopState.remoteHp) || 0);
      payload["guest/maxHp"] = Math.max(1, Number(coopState.remoteMaxHp) || 5);
      payload["guest/alive"] = payload["guest/hp"] > 0;
      payload["guest/wave"] = ownWave;
      payload["guest/score"] = ownScore;
    }
  }

  if (state.status === "running") {
    payload.status = "running";
    payload.endedReason = "";
  }

  const payloadKeyParts = [
    role,
    alive ? 1 : 0,
    ownHp,
    ownMaxHp,
    ownScore,
    ownWave,
    aimX,
    aimY,
    pointerDown ? 1 : 0,
    payload[`${role}/weapon`],
    state.status,
    payload.status || "",
    payload.sharedWave || "",
    payload["guest/hp"] ?? "",
    payload["guest/maxHp"] ?? "",
    payload["guest/alive"] ?? "",
  ];
  const payloadKey = payloadKeyParts.join("|");
  if (payloadKey === coopState.lastRoomPayloadKey) {
    coopState.lastSyncAt = now;
    return;
  }

  coopState.lastRoomPayloadKey = payloadKey;
  coopState.lastSyncAt = now;
  updateCoopRoom(payload);
}

const authState = {
  user: null,
  profile: null,
  sessionStart: null,
  loading: false,
};

const cloudState = {
  ready: false,
  auth: null,
  db: null,
  rtdb: null,
  rtdbUrl: "",
  rtdbCandidates: [],
  rtdbInitError: "",
  saveQueue: Promise.resolve(),
  persistenceReady: Promise.resolve(),
  persistenceMode: "unknown",
  anonymousLeaderboardTried: false,
  leaderboardAccessDenied: false,
};

const authUiState = {
  persistenceHintShown: false,
  methodsOpen: false,
  emailFormOpen: false,
  pendingLinkCredential: null,
  pendingLinkEmail: "",
};

function getUsers() {
  const data = localStorage.getItem(STORAGE_KEYS.USERS);
  return data ? JSON.parse(data) : {};
}

function saveUsersLocalOnly(users) {
  localStorage.setItem(STORAGE_KEYS.USERS, JSON.stringify(users));
}

function saveUsers(users) {
  saveUsersLocalOnly(users);

  const currentEmail = authState.user?.email;
  if (!currentEmail || !users[currentEmail]) return;

  authState.profile = migrateProfile(users[currentEmail]);
  queueCloudProfileSave(authState.profile);
}

function isFirebaseConfigured() {
  const config = window.FIREBASE_CONFIG;
  return (
    typeof window.firebase !== "undefined" &&
    config &&
    typeof config === "object" &&
    config.apiKey &&
    config.authDomain &&
    config.projectId
  );
}

function initCloud() {
  if (cloudState.ready) return true;
  if (!isFirebaseConfigured()) return false;

  const config = window.FIREBASE_CONFIG;
  if (!window.firebase.apps.length) {
    window.firebase.initializeApp(config);
  }

  cloudState.auth = window.firebase.auth();
  cloudState.db = window.firebase.firestore();
  cloudState.rtdb = null;
  cloudState.rtdbUrl = "";
  cloudState.rtdbCandidates = getRtdbUrlCandidates(config);
  cloudState.rtdbInitError = "";
  if (typeof window.firebase.database === "function") {
    try {
      cloudState.rtdb = window.firebase.database();
      cloudState.rtdbUrl = String(config.databaseURL || "").trim();
    } catch (error) {
      for (const candidate of cloudState.rtdbCandidates) {
        if (!candidate) continue;
        try {
          cloudState.rtdb = window.firebase.database(candidate);
          cloudState.rtdbUrl = candidate;
          break;
        } catch (candidateError) {
          cloudState.rtdb = null;
          cloudState.rtdbUrl = "";
        }
      }
      if (!cloudState.rtdb) {
        console.error("Realtime Database init error:", error);
        cloudState.rtdbInitError = String(error?.message || error || "unknown");
      }
    }
  }
  cloudState.persistenceReady = cloudState.auth
    .setPersistence(window.firebase.auth.Auth.Persistence.LOCAL)
    .then(() => {
      cloudState.persistenceMode = "local";
    })
    .catch(async (error) => {
      console.warn("Auth LOCAL persistence error:", error);
      try {
        await cloudState.auth.setPersistence(window.firebase.auth.Auth.Persistence.SESSION);
        cloudState.persistenceMode = "session";
      } catch (sessionError) {
        console.error("Auth SESSION persistence error:", sessionError);
        cloudState.persistenceMode = "none";
      }
    });
  cloudState.ready = true;
  return true;
}

function createStarterProfile(email = "", displayName = "") {
  return migrateProfile({
    email,
    displayName,
    bestScore: 0,
    gamesPlayed: 0,
    totalPlaytime: 0,
    createdAt: Date.now(),
    gold: 100,
    upgradePoints: 0,
    hpLevel: 1,
    maxHpLevel: 1,
    weapons: createDefaultWeaponProgress(),
  });
}

function getAuthErrorText(error) {
  const code = error?.code || "";
  if (code === "auth/popup-closed-by-user") return "Вход прерван. Попробуй ещё раз.";
  if (code === "auth/popup-blocked") return "Браузер заблокировал popup. Разреши всплывающие окна.";
  if (code === "auth/cancelled-popup-request") return "Запрос входа отменён. Нажми кнопку входа ещё раз.";
  if (code === "auth/operation-not-supported-in-this-environment") return "Текущий браузер не поддерживает этот способ входа.";
  if (code === "auth/network-request-failed") return "Проблема с интернетом. Проверь подключение и попробуй снова.";
  if (code === "auth/unauthorized-domain") return "Домен не разрешен. Добавь его в Firebase -> Authentication -> Authorized domains.";
  if (code === "auth/operation-not-allowed") return "Этот способ входа выключен в Firebase Authentication.";
  if (code === "auth/account-exists-with-different-credential") return "Этот email уже связан с другим способом входа.";
  if (code === "auth/email-already-in-use") return "Этот email уже используется. Попробуй «Войти», а не «Создать аккаунт».";
  if (code === "auth/invalid-email") return "Неверный формат email.";
  if (code === "auth/user-not-found") return "Аккаунт с таким email не найден.";
  if (code === "auth/wrong-password") return "Неверный пароль.";
  if (code === "auth/invalid-credential") return "Неверный email или пароль.";
  if (code === "auth/weak-password") return "Слишком простой пароль (минимум 6 символов).";
  if (code === "auth/provider-already-linked") return "Этот способ входа уже привязан к аккаунту.";
  if (code === "auth/credential-already-in-use") return "Эта учетная запись уже привязана к другому профилю.";
  return `Не удалось выполнить вход${code ? ` (${code})` : ""}.`;
}

function normalizeEmail(email = "") {
  return String(email).trim().toLowerCase();
}

function isInAppBrowser() {
  const ua = navigator.userAgent || "";
  return /FBAN|FBAV|Instagram|Line|Telegram|wv/i.test(ua);
}

function isTelegramBrowser() {
  const ua = navigator.userAgent || "";
  return /Telegram/i.test(ua);
}

function isIosDevice() {
  const ua = navigator.userAgent || "";
  return /iPhone|iPad|iPod/i.test(ua);
}

function isIosSafari() {
  const ua = navigator.userAgent || "";
  const isSafari = /Safari/i.test(ua) && !/CriOS|FxiOS|EdgiOS|OPiOS|YaBrowser/i.test(ua);
  return isIosDevice() && isSafari && !isInAppBrowser();
}

function shouldUseRedirectAuth(providerKey = "google") {
  const ua = navigator.userAgent || "";
  const isAndroid = /Android/i.test(ua);
  if (providerKey === "apple" && isIosDevice()) return true;
  // В Telegram сначала пробуем popup, затем fallback на redirect.
  if (isTelegramBrowser()) return false;
  // На iOS redirect часто ломается из-за ограничений браузера, поэтому там всегда popup.
  if (isIosDevice()) return false;
  return isAndroid || isInAppBrowser();
}

function markRedirectPending() {
  localStorage.setItem(STORAGE_KEYS.AUTH_REDIRECT_PENDING, String(Date.now()));
}

function clearRedirectPending() {
  localStorage.removeItem(STORAGE_KEYS.AUTH_REDIRECT_PENDING);
}

function isRedirectPending() {
  return Boolean(localStorage.getItem(STORAGE_KEYS.AUTH_REDIRECT_PENDING));
}

function getRedirectPendingTimestamp() {
  const raw = localStorage.getItem(STORAGE_KEYS.AUTH_REDIRECT_PENDING);
  const ts = Number(raw);
  return Number.isFinite(ts) ? ts : 0;
}

async function ensureAuthPersistenceReady() {
  if (!cloudState.ready || !cloudState.auth) return;
  try {
    await cloudState.persistenceReady;
  } catch (error) {
    console.error("Persistence readiness error:", error);
  }

  if (authUiState.persistenceHintShown) return;
  if (cloudState.persistenceMode === "session") {
    authUiState.persistenceHintShown = true;
    showAuthMessage("Браузер сохраняет вход только до закрытия вкладки.", "error");
  } else if (cloudState.persistenceMode === "none") {
    authUiState.persistenceHintShown = true;
    showAuthMessage("Этот браузер не может запомнить вход. Для постоянной сессии используй Safari/Chrome.", "error");
  }
}

function getProviderLabel(providerKey) {
  return providerKey === "apple" ? "Apple ID" : "Google";
}

function createAuthProvider(providerKey) {
  if (providerKey === "google") {
    return new window.firebase.auth.GoogleAuthProvider();
  }
  if (providerKey === "apple") {
    const provider = new window.firebase.auth.OAuthProvider("apple.com");
    provider.addScope("email");
    provider.addScope("name");
    return provider;
  }
  throw new Error(`Unknown provider: ${providerKey}`);
}

function extractCredentialFromProviderError(error, providerKey) {
  try {
    if (providerKey === "google" && window.firebase.auth.GoogleAuthProvider.credentialFromError) {
      return window.firebase.auth.GoogleAuthProvider.credentialFromError(error);
    }
    if (providerKey === "apple" && window.firebase.auth.OAuthProvider?.credentialFromError) {
      return window.firebase.auth.OAuthProvider.credentialFromError(error);
    }
  } catch (extractError) {
    console.warn("Credential extraction warning:", extractError);
  }
  return error?.credential || null;
}

function clearPendingLinkCredential() {
  authUiState.pendingLinkCredential = null;
  authUiState.pendingLinkEmail = "";
}

function setPendingLinkCredential(credential, email = "") {
  authUiState.pendingLinkCredential = credential || null;
  authUiState.pendingLinkEmail = normalizeEmail(email);
}

function setAuthMethodsOpen(open) {
  authUiState.methodsOpen = Boolean(open);
  if (authMethods) {
    authMethods.hidden = !authUiState.methodsOpen;
  }
  if (!authUiState.methodsOpen) {
    setEmailFormOpen(false);
  }
}

function setEmailFormOpen(open) {
  authUiState.emailFormOpen = Boolean(open);
  if (emailAuthForm) {
    emailAuthForm.hidden = !authUiState.emailFormOpen;
  }
}

function prefillEmailInput(email = "") {
  if (emailInput && email) {
    emailInput.value = email;
  }
}

async function tryLinkPendingCredential(user) {
  if (!user || user.isAnonymous || !authUiState.pendingLinkCredential) return;

  const expectedEmail = authUiState.pendingLinkEmail;
  if (expectedEmail) {
    const currentEmail = normalizeEmail(user.email || "");
    if (!currentEmail || currentEmail !== expectedEmail) {
      return;
    }
  }

  try {
    await user.linkWithCredential(authUiState.pendingLinkCredential);
    showAuthMessage("Способ входа успешно привязан к твоему аккаунту.", "success");
  } catch (error) {
    const code = error?.code || "";
    if (code !== "auth/provider-already-linked" && code !== "auth/credential-already-in-use") {
      console.error("Link credential error:", error);
    }
  } finally {
    clearPendingLinkCredential();
  }
}

async function handleAccountExistsWithDifferentCredential(error, providerKey) {
  const email = normalizeEmail(error?.customData?.email || error?.email || "");
  const credential = extractCredentialFromProviderError(error, providerKey);
  if (credential && email) {
    setPendingLinkCredential(credential, email);
  }

  if (!cloudState.auth || !email) {
    showAuthMessage("Этот email уже связан с другим способом входа. Войди им и повтори попытку.", "error");
    return;
  }

  try {
    const methods = await cloudState.auth.fetchSignInMethodsForEmail(email);
    const hasPassword = methods.includes(window.firebase.auth.EmailAuthProvider.EMAIL_PASSWORD_SIGN_IN_METHOD);
    const hasGoogle = methods.includes("google.com");
    const hasApple = methods.includes("apple.com");

    setAuthMethodsOpen(true);

    if (hasPassword) {
      setEmailFormOpen(true);
      prefillEmailInput(email);
      if (passwordInput) passwordInput.focus();
      showAuthMessage("Этот email уже зарегистрирован. Войди через Email/Пароль, и аккаунты объединятся.", "error");
      return;
    }

    if (hasGoogle) {
      showAuthMessage("Этот email уже связан с Google. Войди через Google, затем можно привязать другие методы.", "error");
      return;
    }

    if (hasApple) {
      showAuthMessage("Этот email уже связан с Apple ID. Войди через Apple ID, затем можно привязать другие методы.", "error");
      return;
    }

    showAuthMessage("Этот email уже имеет другой способ входа. Выбери существующий способ для объединения.", "error");
  } catch (methodsError) {
    console.error("fetchSignInMethodsForEmail error:", methodsError);
    showAuthMessage("Не удалось определить способ входа для этого email.", "error");
  }
}

async function signInWithProvider(providerKey) {
  if (!initCloud()) {
    showAuthMessage("Сначала настрой Firebase (файл firebase-config.js).", "error");
    return false;
  }

  await ensureAuthPersistenceReady();

  if (isInAppBrowser() && !isTelegramBrowser()) {
    showAuthMessage("Встроенный браузер блокирует вход. Открой сайт в Safari/Chrome и попробуй снова.", "error");
    return false;
  }

  const provider = createAuthProvider(providerKey);
  const providerLabel = getProviderLabel(providerKey);

  if (shouldUseRedirectAuth(providerKey)) {
    try {
      markRedirectPending();
      await cloudState.auth.signInWithRedirect(provider);
      return true;
    } catch (redirectError) {
      clearRedirectPending();
      showAuthMessage(getAuthErrorText(redirectError), "error");
      return false;
    }
  }

  try {
    const userCredential = await cloudState.auth.signInWithPopup(provider);
    await tryLinkPendingCredential(userCredential?.user || cloudState.auth.currentUser || null);
    setAuthMethodsOpen(false);
    setEmailFormOpen(false);
    return true;
  } catch (error) {
    if (error?.code === "auth/account-exists-with-different-credential") {
      await handleAccountExistsWithDifferentCredential(error, providerKey);
      return false;
    }

    const canFallbackToRedirect = providerKey === "apple" || !isIosDevice() || isTelegramBrowser();
    if (
      (error?.code === "auth/popup-blocked" ||
        error?.code === "auth/popup-closed-by-user" ||
        error?.code === "auth/operation-not-supported-in-this-environment") &&
      canFallbackToRedirect
    ) {
      try {
        markRedirectPending();
        await cloudState.auth.signInWithRedirect(provider);
        return true;
      } catch (redirectError) {
        clearRedirectPending();
        showAuthMessage(getAuthErrorText(redirectError), "error");
        return false;
      }
    }

    if (isTelegramBrowser()) {
      showAuthMessage(
        `Telegram может блокировать вход через ${providerLabel}. Нажми «Открыть в браузере» и войди там один раз.`,
        "error"
      );
      return false;
    }
    if (isIosDevice()) {
      showAuthMessage(
        "Вход на iPhone не завершился. Попробуй ещё раз в обычной вкладке Safari (не приватной) и без блокировки всплывающих окон.",
        "error"
      );
      return false;
    }
    showAuthMessage(getAuthErrorText(error), "error");
    return false;
  }
}

async function signInWithGoogle() {
  return signInWithProvider("google");
}

async function signInWithApple() {
  return signInWithProvider("apple");
}

function readEmailPasswordCredentials() {
  const email = normalizeEmail(emailInput?.value || "");
  const password = String(passwordInput?.value || "");
  return { email, password };
}

function clearEmailPasswordForm() {
  if (passwordInput) passwordInput.value = "";
}

async function signInWithEmailPassword(mode) {
  if (!initCloud()) {
    showAuthMessage("Сначала настрой Firebase (файл firebase-config.js).", "error");
    return false;
  }

  await ensureAuthPersistenceReady();

  const { email, password } = readEmailPasswordCredentials();
  if (!email) {
    showAuthMessage("Введи email.", "error");
    return false;
  }
  if (!password) {
    showAuthMessage("Введи пароль.", "error");
    return false;
  }
  if (mode === "signup" && password.length < 6) {
    showAuthMessage("Пароль должен быть не короче 6 символов.", "error");
    return false;
  }

  try {
    let userCredential = null;
    if (mode === "signup") {
      userCredential = await cloudState.auth.createUserWithEmailAndPassword(email, password);
      showAuthMessage("Аккаунт создан и вход выполнен.", "success");
    } else {
      userCredential = await cloudState.auth.signInWithEmailAndPassword(email, password);
      showAuthMessage("Вход по email выполнен.", "success");
    }

    await tryLinkPendingCredential(userCredential?.user || cloudState.auth.currentUser || null);
    clearEmailPasswordForm();
    setAuthMethodsOpen(false);
    setEmailFormOpen(false);
    return true;
  } catch (error) {
    if (error?.code === "auth/account-exists-with-different-credential") {
      await handleAccountExistsWithDifferentCredential(error, "google");
      return false;
    }
    if (error?.code === "auth/email-already-in-use" && cloudState.auth) {
      try {
        const methods = await cloudState.auth.fetchSignInMethodsForEmail(email);
        setAuthMethodsOpen(true);
        if (methods.includes(window.firebase.auth.EmailAuthProvider.EMAIL_PASSWORD_SIGN_IN_METHOD)) {
          setEmailFormOpen(true);
          showAuthMessage("Этот email уже зарегистрирован. Используй кнопку «Войти».", "error");
        } else if (methods.includes("google.com")) {
          showAuthMessage("Этот email уже связан с Google. Войди через Google, затем можно привязать Email/Пароль.", "error");
        } else if (methods.includes("apple.com")) {
          showAuthMessage("Этот email уже связан с Apple ID. Войди через Apple ID, затем можно привязать Email/Пароль.", "error");
        }
      } catch (methodsError) {
        console.error("email-already-in-use methods error:", methodsError);
      }
      return false;
    }
    showAuthMessage(getAuthErrorText(error), "error");
    return false;
  }
}

async function handlePendingRedirectResult() {
  if (!cloudState.ready || !cloudState.auth) return;
  const pendingTs = getRedirectPendingTimestamp();
  if (!pendingTs) return;

  // Сбрасываем "зависший" флаг старого редиректа, чтобы не показывать ложную ошибку.
  if (Date.now() - pendingTs > 15 * 60 * 1000) {
    clearRedirectPending();
    return;
  }

  try {
    const result = await cloudState.auth.getRedirectResult();
    if (result?.user) {
      clearRedirectPending();
      await tryLinkPendingCredential(result.user);
      setAuthMethodsOpen(false);
      setEmailFormOpen(false);
      return;
    }

    // Иногда на iOS/webview redirect завершается без результата и без ошибки.
    setTimeout(() => {
      if (isRedirectPending() && !cloudState.auth.currentUser) {
        clearRedirectPending();
        if (isTelegramBrowser()) {
          showAuthMessage("Telegram не завершил вход. Открой ссылку в Safari/Chrome и войди там один раз.", "error");
          return;
        }
        showAuthMessage(
          "Вход не завершился. Если ты уже в Safari, нажми кнопку входа ещё раз в обычной вкладке.",
          "error"
        );
      }
    }, 3200);
  } catch (error) {
    if (error?.code === "auth/account-exists-with-different-credential") {
      const providerId = error?.credential?.providerId || error?.customData?.providerId || "";
      const providerKey = providerId === "apple.com" ? "apple" : "google";
      await handleAccountExistsWithDifferentCredential(error, providerKey);
      clearRedirectPending();
      return;
    }
    console.error("Redirect result error:", error);
    clearRedirectPending();
    showAuthMessage(getAuthErrorText(error), "error");
  }
}

async function signOut() {
  clearPendingLinkCredential();
  setAuthMethodsOpen(false);

  if (coopState.active) {
    await leaveCoopLobby({ silent: true });
  }

  if (cloudState.ready && cloudState.auth) {
    try {
      await cloudState.auth.signOut();
    } catch (error) {
      showAuthMessage("Не удалось выйти из аккаунта.", "error");
    }
    return;
  }

  authState.user = null;
  authState.profile = null;
  authState.sessionStart = null;
  resetCoopLocalState();
  updateAuthUI();
  updateLeaderboard();
}

async function queueCloudProfileSave(profile = authState.profile) {
  if (!cloudState.ready || !cloudState.db || !authState.user?.uid || !profile) return;

  const cleanProfile = migrateProfile(profile);
  const { password, ...safeProfile } = cleanProfile;
  const payload = {
    ...safeProfile,
    email: authState.user.email || safeProfile.email || "",
    displayName: authState.user.displayName || safeProfile.displayName || "",
    createdAt: safeProfile.createdAt || Date.now(),
    updatedAt: Date.now(),
  };

  cloudState.saveQueue = cloudState.saveQueue
    .catch(() => {})
    .then(() =>
      cloudState.db.collection(CLOUD_KEYS.USERS_COLLECTION).doc(authState.user.uid).set(payload, { merge: true })
    )
    .catch((error) => {
      console.error("Cloud save error:", error);
      showAuthMessage("Ошибка сохранения в облаке. Локально данные сохранены.", "error");
    });

  return cloudState.saveQueue;
}

async function readCloudProfile(firebaseUser) {
  const ref = cloudState.db.collection(CLOUD_KEYS.USERS_COLLECTION).doc(firebaseUser.uid);
  const snap = await ref.get();
  if (!snap.exists) return null;
  return migrateProfile(snap.data());
}

async function checkAuth() {
  const cloudEnabled = initCloud();

  if (!cloudEnabled) {
    authState.user = null;
    authState.profile = null;
    authState.sessionStart = null;
    updateAuthUI();
    updateLeaderboard();
    return;
  }

  await ensureAuthPersistenceReady();
  await handlePendingRedirectResult();

  cloudState.auth.onAuthStateChanged(async (firebaseUser) => {
    if (!firebaseUser) {
      authState.user = null;
      authState.profile = null;
      authState.sessionStart = null;
      authState.loading = false;
      localStorage.removeItem(STORAGE_KEYS.CURRENT_USER);
      resetCoopLocalState();
      updateAuthUI();
      updateLeaderboard();
      return;
    }

    if (firebaseUser.isAnonymous) {
      authState.user = null;
      authState.profile = null;
      authState.sessionStart = null;
      authState.loading = false;
      localStorage.removeItem(STORAGE_KEYS.CURRENT_USER);
      resetCoopLocalState();
      updateAuthUI();
      updateLeaderboard();
      return;
    }

    clearRedirectPending();
    await tryLinkPendingCredential(firebaseUser);

    const email = firebaseUser.email || `uid-${firebaseUser.uid}@local`;
    authState.user = {
      uid: firebaseUser.uid,
      email,
      displayName: firebaseUser.displayName || "",
    };
    authState.loading = true;
    authState.sessionStart = Date.now();
    localStorage.setItem(STORAGE_KEYS.CURRENT_USER, email);
    updateAuthUI();

    const users = getUsers();
    const localProfile = users[email] ? migrateProfile(users[email]) : null;

    try {
      const cloudProfile = await readCloudProfile(firebaseUser);
      authState.profile = cloudProfile || localProfile || createStarterProfile(email, authState.user.displayName);
    } catch (error) {
      console.error("Cloud read error:", error);
      authState.profile = localProfile || createStarterProfile(email, authState.user.displayName);
      showAuthMessage("Не удалось загрузить профиль из облака. Использую локальные данные.", "error");
    }

    users[email] = authState.profile;
    saveUsersLocalOnly(users);
    await queueCloudProfileSave(authState.profile);

    authState.loading = false;
    updateAuthUI();
    updateLeaderboard();
  });
}

function saveGameResult(score, wave) {
  if (!authState.user || !authState.profile) return;
  const users = getUsers();
  const email = authState.user.email;
  if (!users[email]) return;
  users[email] = migrateProfile(users[email]);
  const playtimeSeconds = authState.sessionStart ? Math.floor((Date.now() - authState.sessionStart) / 1000) : 0;
  
  // Обновить рекорд
  users[email].bestScore = Math.max(users[email].bestScore || 0, score);
  users[email].gamesPlayed = (users[email].gamesPlayed || 0) + 1;
  users[email].totalPlaytime = (users[email].totalPlaytime || 0) + playtimeSeconds;
  
  // Начислить золото
  const goldKill = getGoldPerKill(wave) * (score / 10); // За убийства
  const goldWave = getGoldPerWave(wave); // За волны
  const goldScore = getGoldPerScore(score); // За очки
  const totalGold = Math.floor(goldKill + goldWave + goldScore);
  
  users[email].gold = (users[email].gold || 0) + totalGold;
  
  saveUsers(users);
  authState.profile = users[email];
  authState.sessionStart = Date.now();
  updateAuthUI();
  updateLeaderboard();
  
  // Показать уведомление о золоте
  showGoldNotification(totalGold);
}

function showGoldNotification(amount) {
  const existing = document.getElementById('goldNotification');
  if (existing) existing.remove();
  
  const notif = document.createElement('div');
  notif.id = 'goldNotification';
  notif.innerHTML = `+${amount} 🪙`;
  notif.style.cssText = `
    position: fixed;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    font-size: 48px;
    font-weight: bold;
    color: #ffd700;
    text-shadow: 0 0 20px #ffd700, 0 2px 4px rgba(0,0,0,0.8);
    z-index: 1000;
    animation: goldPop 2s ease-out forwards;
    pointer-events: none;
  `;
  document.body.appendChild(notif);
  
  // Добавить анимацию
  if (!document.getElementById('goldAnim')) {
    const style = document.createElement('style');
    style.id = 'goldAnim';
    style.textContent = `
      @keyframes goldPop {
        0% { transform: translate(-50%, -50%) scale(0.5); opacity: 0; }
        20% { transform: translate(-50%, -50%) scale(1.2); opacity: 1; }
        80% { transform: translate(-50%, -80%) scale(1); opacity: 1; }
        100% { transform: translate(-50%, -100%) scale(0.8); opacity: 0; }
      }
    `;
    document.head.appendChild(style);
  }
  
  setTimeout(() => notif.remove(), 2000);
}

// Купить оружие
function buyWeapon(weaponKey) {
  if (!authState.user || !authState.profile) {
    showShopMessage("Войдите в аккаунт для покупок!", "error");
    return false;
  }
  
  const users = getUsers();
  const email = authState.user.email;
  if (!users[email]) return false;
  users[email] = migrateProfile(users[email]);
  const price = WEAPON_PRICES[weaponKey];
  
  if (price === undefined) {
    showShopMessage("Оружие не найдено!", "error");
    return false;
  }
  
  if (users[email].weapons[weaponKey]?.owned) {
    showShopMessage("Это оружие уже куплено!", "error");
    return false;
  }
  
  if (users[email].gold < price) {
    showShopMessage(`Недостаточно золота! Нужно ${price} 🪙`, "error");
    return false;
  }
  
  users[email].gold -= price;
  users[email].weapons[weaponKey] = {
    ...(users[email].weapons[weaponKey] || {}),
    owned: true,
    damageLevel: Math.max(1, Number(users[email].weapons[weaponKey]?.damageLevel) || 1),
    fireRateLevel: Math.max(1, Number(users[email].weapons[weaponKey]?.fireRateLevel) || 1),
  };
  saveUsers(users);
  authState.profile = users[email];
  updateAuthUI();
  showShopMessage("Оружие куплено!", "success");
  return true;
}

// Прокачать характеристику
function upgradeStat(type) {
  if (!authState.user || !authState.profile) {
    showShopMessage("Войдите в аккаунт для прокачки!", "error");
    return false;
  }
  
  const users = getUsers();
  const email = authState.user.email;
  if (!users[email]) return false;
  users[email] = migrateProfile(users[email]);
  const currentLevel = users[email][type] || 1;
  const cost = getUpgradeCost(type, currentLevel);
  
  if (cost === Infinity) {
    showShopMessage("Максимальный уровень!", "error");
    return false;
  }
  
  if (users[email].gold < cost) {
    showShopMessage(`Недостаточно золота! Нужно ${cost} 🪙`, "error");
    return false;
  }
  
  users[email].gold -= cost;
  users[email][type] = currentLevel + 1;
  saveUsers(users);
  authState.profile = users[email];
  updateAuthUI();
  showShopMessage("Прокачка выполнена!", "success");
  return true;
}

// Прокачать оружие
function upgradeWeapon(weaponKey, statType) {
  if (!authState.user || !authState.profile) {
    showShopMessage("Войдите в аккаунт для прокачки!", "error");
    return false;
  }
  
  const users = getUsers();
  const email = authState.user.email;
  if (!users[email]) return false;
  users[email] = migrateProfile(users[email]);
  const weapon = users[email].weapons[weaponKey];
  
  if (!weapon || !weapon.owned) {
    showShopMessage("Сначала купите это оружие!", "error");
    return false;
  }
  
  const currentLevel = weapon[statType] || 1;
  const cost = getUpgradeCost(statType, currentLevel);
  
  if (cost === Infinity) {
    showShopMessage("Максимальный уровень!", "error");
    return false;
  }
  
  if (users[email].gold < cost) {
    showShopMessage(`Недостаточно золота! Нужно ${cost} 🪙`, "error");
    return false;
  }
  
  users[email].gold -= cost;
  users[email].weapons[weaponKey][statType] = currentLevel + 1;
  saveUsers(users);
  authState.profile = users[email];
  updateAuthUI();
  showShopMessage("Прокачка оружия выполнена!", "success");
  return true;
}

function showShopMessage(text, type) {
  const existing = document.getElementById('shopMessage');
  if (existing) existing.remove();
  
  const msg = document.createElement('div');
  msg.id = 'shopMessage';
  msg.textContent = text;
  msg.style.cssText = `
    position: fixed;
    bottom: 120px;
    left: 50%;
    transform: translateX(-50%);
    padding: 12px 24px;
    border-radius: 8px;
    font-size: 16px;
    font-weight: 600;
    z-index: 1000;
    animation: slideUp 0.3s ease-out;
    background: ${type === 'success' ? 'rgba(67, 255, 140, 0.9)' : 'rgba(255, 59, 59, 0.9)'};
    color: #000;
    box-shadow: 0 4px 20px rgba(0,0,0,0.4);
  `;
  document.body.appendChild(msg);
  
  setTimeout(() => msg.remove(), 2500);
}

function getLocalLeaderboard() {
  const users = getUsers();
  return Object.entries(users)
    .map(([email, data]) => ({
      email,
      displayName: data.displayName || "",
      bestScore: data.bestScore || 0,
      gamesPlayed: data.gamesPlayed || 0,
      totalPlaytime: data.totalPlaytime || 0,
    }))
    .sort((a, b) => b.bestScore - a.bestScore)
    .slice(0, 20);
}

async function getLeaderboard() {
  if (!cloudState.ready || !cloudState.db) {
    return getLocalLeaderboard();
  }

  const fetchFromCloud = async () => {
    const snap = await cloudState.db
      .collection(CLOUD_KEYS.USERS_COLLECTION)
      .orderBy("bestScore", "desc")
      .limit(20)
      .get();

    return snap.docs.map((doc) => {
      const data = doc.data() || {};
      return {
        email: data.email || "",
        displayName: data.displayName || "",
        bestScore: data.bestScore || 0,
        gamesPlayed: data.gamesPlayed || 0,
        totalPlaytime: data.totalPlaytime || 0,
      };
    });
  };

  const isPermissionDenied = (error) => {
    const code = String(error?.code || "").toLowerCase();
    return code.includes("permission-denied") || code.includes("permission_denied");
  };

  try {
    const leaderboard = await fetchFromCloud();
    cloudState.leaderboardAccessDenied = false;
    return leaderboard;
  } catch (error) {
    if (isPermissionDenied(error) && cloudState.auth && !cloudState.auth.currentUser && !cloudState.anonymousLeaderboardTried) {
      cloudState.anonymousLeaderboardTried = true;
      try {
        await cloudState.auth.signInAnonymously();
        const leaderboard = await fetchFromCloud();
        cloudState.leaderboardAccessDenied = false;
        return leaderboard;
      } catch (anonError) {
        console.error("Anonymous leaderboard access error:", anonError);
      }
    }

    cloudState.leaderboardAccessDenied = isPermissionDenied(error);
    console.error("Leaderboard fetch error:", error);
    return getLocalLeaderboard();
  }
}

async function updateLeaderboard() {
  const leaderboardBody = document.getElementById("leaderboardBody");
  if (!leaderboardBody) return;
  
  const leaderboard = await getLeaderboard();

  if (leaderboardStatus && !authState.user) {
    if (cloudState.leaderboardAccessDenied) {
      leaderboardStatus.textContent =
        "Рейтинг сейчас закрыт правилами Firebase. Открой чтение коллекции рейтинга для гостей.";
    } else {
      leaderboardStatus.textContent = "Рейтинг открыт всем. Войди в аккаунт, чтобы сохранять свой прогресс и рекорды.";
    }
  }
  
  if (leaderboard.length === 0) {
    leaderboardBody.innerHTML = '<tr><td colspan="5" class="empty-row">Пока нет игроков</td></tr>';
    return;
  }
  
  leaderboardBody.innerHTML = leaderboard
    .map((p, i) => {
      const fallbackName = p.email ? p.email.split("@")[0] : "Игрок";
      const name = p.displayName || fallbackName;
      const playtime = formatPlaytime(p.totalPlaytime);
      const isMe = authState.user && p.email === authState.user.email;
      return `
        <tr${isMe ? " class='me'" : ""}>
          <td>${i + 1}</td>
          <td>${escapeHtml(name)}${isMe ? " (ты)" : ""}</td>
          <td>${p.bestScore}</td>
          <td>${p.gamesPlayed}</td>
          <td>${playtime}</td>
        </tr>
      `;
    })
    .join("");
}

// Показать сообщение
function showAuthMessage(text, type) {
  const msgEl = document.getElementById("authMessage");
  if (msgEl) {
    msgEl.textContent = text;
    msgEl.className = "auth-message " + type;
    msgEl.hidden = false;
    setTimeout(() => {
      msgEl.hidden = true;
    }, 4000);
    return;
  }

  showShopMessage(text, type === "success" ? "success" : "error");
}

// Форматирование времени
function formatPlaytime(seconds) {
  if (!seconds || seconds < 60) return `${seconds || 0}с`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}м`;
  const hours = Math.floor(seconds / 3600);
  const mins = Math.floor((seconds % 3600) / 60);
  return `${hours}ч ${mins}м`;
}

// Экранирование HTML
function escapeHtml(text) {
  const div = document.createElement("div");
  div.textContent = text;
  return div.innerHTML;
}

// Обновление UI авторизации
function updateAuthUI() {
  const isLoggedIn = !!authState.user;
  const cloudEnabled = isFirebaseConfigured();

  if (loginBtn) {
    loginBtn.hidden = isLoggedIn;
    loginBtn.disabled = !cloudEnabled;
    loginBtn.textContent = cloudEnabled ? "🔐 Вход / Регистрация" : "⚙️ Нужен Firebase";
  }
  if (logoutBtn) logoutBtn.hidden = !isLoggedIn;

  if (!cloudEnabled || isLoggedIn) {
    setAuthMethodsOpen(false);
  } else if (authMethods) {
    authMethods.hidden = !authUiState.methodsOpen;
  }

  if (isLoggedIn && authState.user) {
    const fallbackName = authState.user.email ? authState.user.email.split("@")[0] : "Игрок";
    const name = authState.user.displayName || fallbackName;
    authName.textContent = name;
    authSub.textContent = authState.loading ? "Синхронизация профиля..." : "Аккаунт подключен";

    if (authState.profile) {
      myBest.textContent = authState.profile.bestScore || 0;
      myGames.textContent = authState.profile.gamesPlayed || 0;
      myPlaytime.textContent = formatPlaytime(authState.profile.totalPlaytime || 0);
      
      // Обновить магазин
      updateShopUI();
    }

    updateWeaponButtons();

    if (leaderboardStatus) leaderboardStatus.textContent = "Рейтинг открыт всем. Твой профиль синхронизируется с облаком.";
  } else {
    authName.textContent = "Гость";
    authSub.textContent = cloudEnabled
      ? "Войди в аккаунт, чтобы сохранить прогресс"
      : "Добавь Firebase конфиг для входа";

    myBest.textContent = "-";
    myGames.textContent = "-";
    myPlaytime.textContent = "-";

    if (leaderboardStatus) {
      leaderboardStatus.textContent = cloudEnabled
        ? "Рейтинг открыт всем. Войди в аккаунт, чтобы сохранять свой прогресс и рекорды."
        : "Нужна настройка Firebase для входа и облачного сохранения.";
    }
    
    // Сбросить магазин
    if (document.getElementById('shopGoldAmount')) {
      document.getElementById('shopGoldAmount').textContent = '0';
      document.getElementById('hpLevel').textContent = '1';
      document.getElementById('maxHpLevel').textContent = '1';
      document.getElementById('hpCost').textContent = getUpgradeCost('hpLevel', 1);
      document.getElementById('maxHpCost').textContent = getUpgradeCost('maxHpLevel', 1);
      const hpImpactEl = document.getElementById('hpImpact');
      const maxHpImpactEl = document.getElementById('maxHpImpact');
      if (hpImpactEl) hpImpactEl.textContent = 'Стартовый бонус: +0 HP → +2 HP';
      if (maxHpImpactEl) maxHpImpactEl.textContent = 'Макс. здоровье: 5 HP → 10 HP';
      const hpBtn = document.getElementById('hpUpgradeBtn');
      const maxHpBtn = document.getElementById('maxHpUpgradeBtn');
      if (hpBtn) {
        hpBtn.disabled = true;
        hpBtn.textContent = 'Войди для прокачки';
      }
      if (maxHpBtn) {
        maxHpBtn.disabled = true;
        maxHpBtn.textContent = 'Войди для прокачки';
      }
    }

    renderWeaponShop();
    updateWeaponButtons();
  }

  updateCoopUI();
}

// Обновление UI магазина
function updateShopUI() {
  if (!authState.profile) return;
  
  const profile = authState.profile;
  const gold = Math.max(0, Number(profile.gold) || 0);
  
  // Золото
  const goldEl = document.getElementById('shopGoldAmount');
  if (goldEl) goldEl.textContent = gold;
  
  // Прокачка HP
  const hpLevel = Math.max(1, Number(profile.hpLevel) || 1);
  const hpEl = document.getElementById('hpLevel');
  const hpCostEl = document.getElementById('hpCost');
  const hpImpactEl = document.getElementById('hpImpact');
  const hpBtn = document.getElementById('hpUpgradeBtn');
  const hpCost = getUpgradeCost('hpLevel', hpLevel);
  const currentHeal = getStartHealAtLevel(hpLevel);
  const nextHeal = getStartHealAtLevel(hpLevel + 1);
  if (hpEl) hpEl.textContent = hpLevel;
  if (hpCostEl) hpCostEl.textContent = hpCost === Infinity ? 'MAX' : hpCost;
  if (hpImpactEl) {
    hpImpactEl.textContent =
      hpCost === Infinity
        ? `Стартовый бонус: +${currentHeal} HP (MAX)`
        : `Стартовый бонус: +${currentHeal} HP → +${nextHeal} HP`;
  }
  if (hpBtn) {
    if (hpCost === Infinity) {
      hpBtn.disabled = true;
      hpBtn.textContent = 'MAX';
    } else if (gold < hpCost) {
      hpBtn.disabled = true;
      hpBtn.textContent = `Нужно ${hpCost} 🪙`;
    } else {
      hpBtn.disabled = false;
      hpBtn.textContent = `Прокачать за ${hpCost} 🪙`;
    }
  }
  
  // Прокачка Max HP
  const maxHpLevel = Math.max(1, Number(profile.maxHpLevel) || 1);
  const maxHpEl = document.getElementById('maxHpLevel');
  const maxHpCostEl = document.getElementById('maxHpCost');
  const maxHpImpactEl = document.getElementById('maxHpImpact');
  const maxHpBtn = document.getElementById('maxHpUpgradeBtn');
  const maxHpCost = getUpgradeCost('maxHpLevel', maxHpLevel);
  const currentMaxHp = getMaxHpAtLevel(maxHpLevel);
  const nextMaxHp = getMaxHpAtLevel(maxHpLevel + 1);
  if (maxHpEl) maxHpEl.textContent = maxHpLevel;
  if (maxHpCostEl) maxHpCostEl.textContent = maxHpCost === Infinity ? 'MAX' : maxHpCost;
  if (maxHpImpactEl) {
    maxHpImpactEl.textContent =
      maxHpCost === Infinity
        ? `Макс. здоровье: ${currentMaxHp} HP (MAX)`
        : `Макс. здоровье: ${currentMaxHp} HP → ${nextMaxHp} HP`;
  }
  if (maxHpBtn) {
    if (maxHpCost === Infinity) {
      maxHpBtn.disabled = true;
      maxHpBtn.textContent = 'MAX';
    } else if (gold < maxHpCost) {
      maxHpBtn.disabled = true;
      maxHpBtn.textContent = `Нужно ${maxHpCost} 🪙`;
    } else {
      maxHpBtn.disabled = false;
      maxHpBtn.textContent = `Прокачать за ${maxHpCost} 🪙`;
    }
  }
  
  // Генерация карточек оружия
  renderWeaponShop();
}

// Генерация карточек оружия в магазине
function renderWeaponShop() {
  const grid = document.getElementById('weaponShopGrid');
  if (!grid) return;

  if (!authState.profile) {
    grid.innerHTML = '<p class="shop-empty-state">Войдите в аккаунт, чтобы покупать оружие и делать апгрейды.</p>';
    return;
  }
  
  const weaponsList = [
    { key: 'blaster', name: 'Бластер', icon: '🔫', desc: 'Стартовое оружие: быстрое и стабильное.', tier: 'Базовый' },
    { key: 'shotgun', name: 'Дробовик', icon: '💥', desc: 'Пять дробин за выстрел, мощный урон вблизи.', tier: 'Ударный' },
    { key: 'grenade', name: 'Гранатомёт', icon: '💣', desc: 'Взрыв по площади. Подходит для плотных волн.', tier: 'Тактический' },
    { key: 'beam', name: 'Луч', icon: '⚡', desc: 'Непрерывный лазер, отлично против боссов.', tier: 'Элитный' },
  ];
  
  const profile = authState.profile;
  const weapons = profile.weapons || {};
  const gold = Math.max(0, Number(profile.gold) || 0);
  
  grid.innerHTML = weaponsList.map(w => {
    const owned = isWeaponOwned(w.key, profile);
    const price = WEAPON_PRICES[w.key] ?? 0;
    const dmgLevel = Math.max(1, Number(weapons[w.key]?.damageLevel) || 1);
    const fireLevel = Math.max(1, Number(weapons[w.key]?.fireRateLevel) || 1);
    const dmgCost = getUpgradeCost('damageLevel', dmgLevel);
    const fireCost = getUpgradeCost('fireRateLevel', fireLevel);
    const canBuy = gold >= price;
    const canUpgradeDamage = dmgCost !== Infinity && gold >= dmgCost;
    const canUpgradeFire = fireCost !== Infinity && gold >= fireCost;
    const currentDamage = getWeaponDamageAtLevel(w.key, dmgLevel);
    const nextDamage = getWeaponDamageAtLevel(w.key, dmgLevel + 1);
    const currentCooldown = getWeaponCooldownAtLevel(w.key, fireLevel);
    const nextCooldown = getWeaponCooldownAtLevel(w.key, fireLevel + 1);
    const currentBeamDps = getBeamDpsAtLevels(dmgLevel, fireLevel);
    const nextBeamDpsByDamage = getBeamDpsAtLevels(dmgLevel + 1, fireLevel);
    const nextBeamDpsByFire = getBeamDpsAtLevels(dmgLevel, fireLevel + 1);
    const damagePreview = w.key === 'beam'
      ? (dmgCost === Infinity
        ? `DPS ${formatCompactNumber(currentBeamDps)} (MAX)`
        : `DPS ${formatCompactNumber(currentBeamDps)} → ${formatCompactNumber(nextBeamDpsByDamage)}`)
      : (dmgCost === Infinity
        ? `DMG ${formatCompactNumber(currentDamage)} (MAX)`
        : `DMG ${formatCompactNumber(currentDamage)} → ${formatCompactNumber(nextDamage)}`);
    const firePreview = w.key === 'beam'
      ? (fireCost === Infinity
        ? `DPS ${formatCompactNumber(currentBeamDps)} (MAX)`
        : `DPS ${formatCompactNumber(currentBeamDps)} → ${formatCompactNumber(nextBeamDpsByFire)}`)
      : (fireCost === Infinity
        ? `КД ${currentCooldown}мс (MAX)`
        : `КД ${currentCooldown}мс → ${nextCooldown}мс`);
    
    return `
      <div class="weapon-shop-card ${owned ? 'owned' : 'locked'}">
        <div class="weapon-shop-header">
          <span class="weapon-shop-name">${w.icon} ${w.name}</span><span class="weapon-shop-tier">${w.tier}</span>
          ${owned ? '<span class="weapon-owned-badge">✓ Куплено</span>' : `<span class="weapon-shop-price">${price} 🪙</span>`}
        </div>
        <p class="weapon-shop-desc">${w.desc}</p>
        
        ${owned ? `
          <div class="weapon-upgrades">
            <div class="weapon-upgrade-row">
              <div class="weapon-upgrade-info">
                <span class="weapon-upgrade-name">⚔️ Урон</span>
                <span class="weapon-upgrade-level">Уровень ${dmgLevel}/25</span>
                <span class="weapon-upgrade-preview">${damagePreview}</span>
              </div>
              <button class="weapon-upgrade-btn" onclick="upgradeWeapon('${w.key}', 'damageLevel')" ${canUpgradeDamage ? '' : 'disabled'}>
                ${dmgCost === Infinity ? 'MAX' : dmgCost + ' 🪙'}
              </button>
            </div>
            <div class="weapon-upgrade-row">
              <div class="weapon-upgrade-info">
                <span class="weapon-upgrade-name">⚡ Скорость</span>
                <span class="weapon-upgrade-level">Уровень ${fireLevel}/20</span>
                <span class="weapon-upgrade-preview">${firePreview}</span>
              </div>
              <button class="weapon-upgrade-btn" onclick="upgradeWeapon('${w.key}', 'fireRateLevel')" ${canUpgradeFire ? '' : 'disabled'}>
                ${fireCost === Infinity ? 'MAX' : fireCost + ' 🪙'}
              </button>
            </div>
          </div>
        ` : `
          <button class="weapon-buy-btn" onclick="buyWeapon('${w.key}')" ${canBuy ? '' : 'disabled'}>
            ${canBuy ? `Купить за ${price} 🪙` : `Нужно ${price} 🪙`}
          </button>
        `}
      </div>
    `;
  }).join('');
}

// Элементы UI
const loginBtn = document.getElementById("loginBtn");
const logoutBtn = document.getElementById("logoutBtn");
const authMethods = document.getElementById("authMethods");
const loginGoogleBtn = document.getElementById("loginGoogleBtn");
const showEmailBtn = document.getElementById("showEmailBtn");
const emailAuthForm = document.getElementById("emailAuthForm");
const emailInput = document.getElementById("emailInput");
const passwordInput = document.getElementById("passwordInput");
const emailSignInBtn = document.getElementById("emailSignInBtn");
const emailSignUpBtn = document.getElementById("emailSignUpBtn");
const authName = document.getElementById("authName");
const authSub = document.getElementById("authSub");
const leaderboardBody = document.getElementById("leaderboardBody");
const leaderboardStatus = document.getElementById("leaderboardStatus");
const refreshLeaderboardBtn = document.getElementById("refreshLeaderboardBtn");
const myBest = document.getElementById("myBest");
const myGames = document.getElementById("myGames");
const myPlaytime = document.getElementById("myPlaytime");
const atmoTimeBadge = document.getElementById("atmoTimeBadge");
const atmoWeatherBadge = document.getElementById("atmoWeatherBadge");
const atmoEffectText = document.getElementById("atmoEffectText");
const atmoVisibilityValue = document.getElementById("atmoVisibilityValue");
const atmoSpawnValue = document.getElementById("atmoSpawnValue");
const atmoAccuracyValue = document.getElementById("atmoAccuracyValue");
const coopCreateBtn = document.getElementById("coopCreateBtn");
const coopJoinBtn = document.getElementById("coopJoinBtn");
const coopLeaveBtn = document.getElementById("coopLeaveBtn");
const coopCodeInput = document.getElementById("coopCodeInput");
const coopStatus = document.getElementById("coopStatus");
const coopRoomCode = document.getElementById("coopRoomCode");
const coopCores = document.getElementById("coopCores");
const coopOwnCore = document.getElementById("coopOwnCore");
const coopAllyCore = document.getElementById("coopAllyCore");
const coopAllyLabel = document.getElementById("coopAllyLabel");

// Обработчики событий авторизации
if (loginBtn) {
  loginBtn.addEventListener("click", () => {
    // Защита от кеш-рассинхрона: старый index.html без новых кнопок/формы.
    if (!authMethods || !loginGoogleBtn || !showEmailBtn) {
      signInWithGoogle();
      return;
    }
    setAuthMethodsOpen(!authUiState.methodsOpen);
  });
}

if (loginGoogleBtn) {
  loginGoogleBtn.addEventListener("click", () => {
    signInWithGoogle();
  });
}

if (showEmailBtn) {
  showEmailBtn.addEventListener("click", () => {
    setEmailFormOpen(!authUiState.emailFormOpen);
    if (authUiState.emailFormOpen && authUiState.pendingLinkEmail) {
      prefillEmailInput(authUiState.pendingLinkEmail);
    }
    if (authUiState.emailFormOpen && emailInput && !emailInput.value) {
      emailInput.focus();
    }
  });
}

if (emailSignInBtn) {
  emailSignInBtn.addEventListener("click", () => {
    signInWithEmailPassword("signin");
  });
}

if (emailSignUpBtn) {
  emailSignUpBtn.addEventListener("click", () => {
    signInWithEmailPassword("signup");
  });
}

if (passwordInput) {
  passwordInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      signInWithEmailPassword("signin");
    }
  });
}

if (emailInput) {
  emailInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      signInWithEmailPassword("signin");
    }
  });
}

if (logoutBtn) {
  logoutBtn.addEventListener("click", () => {
    setAuthMethodsOpen(false);
    signOut();
  });
}

if (refreshLeaderboardBtn) {
  refreshLeaderboardBtn.addEventListener("click", () => {
    updateLeaderboard();
  });
}

if (coopCodeInput) {
  coopCodeInput.addEventListener("input", () => {
    const normalized = normalizeCoopCode(coopCodeInput.value);
    if (coopCodeInput.value !== normalized) {
      coopCodeInput.value = normalized;
    }
  });
  coopCodeInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      joinCoopLobby();
    }
  });
}

if (coopCreateBtn) {
  coopCreateBtn.addEventListener("click", () => {
    createCoopLobby().catch((error) => {
      console.error("Create coop lobby click error:", error);
      setCoopBusy(false);
      showShopMessage("Ошибка при создании лобби. Проверь Firebase и попробуй снова.", "error");
    });
  });
}

if (coopJoinBtn) {
  coopJoinBtn.addEventListener("click", () => {
    joinCoopLobby().catch((error) => {
      console.error("Join coop lobby click error:", error);
      setCoopBusy(false);
      showShopMessage("Ошибка при входе в лобби. Проверь код и попробуй снова.", "error");
    });
  });
}

if (coopLeaveBtn) {
  coopLeaveBtn.addEventListener("click", () => {
    leaveCoopLobby().catch((error) => {
      console.error("Leave coop lobby click error:", error);
      setCoopBusy(false);
      showShopMessage("Ошибка при выходе из лобби.", "error");
    });
  });
}

// Запуск проверки авторизации
checkAuth().catch((error) => {
  console.error("Auth bootstrap error:", error);
});

// Табы энциклопедии
document.querySelectorAll('.encyclo-tab').forEach(tab => {
  tab.addEventListener('click', () => {
    const target = tab.dataset.tab;
    document.querySelectorAll('.encyclo-tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.encyclo-content').forEach(c => c.classList.remove('active'));
    tab.classList.add('active');
    document.getElementById(target).classList.add('active');
  });
});

const ZOMBIE_PREVIEW_CONFIG = {
  normal: { radius: 24, hp: 2, maxHp: 2, body: COLORS.zombie, outline: COLORS.zombieAccent, eye: COLORS.zombieEye, glow: "rgba(31, 90, 60, 0.62)" },
  fast: { radius: 18, hp: 1, maxHp: 1, body: "#2a7a4f", outline: "#57c880", eye: "#e8ffd0", glow: "rgba(74, 185, 95, 0.68)" },
  tank: { radius: 30, hp: 5, maxHp: 5, body: "#28543c", outline: "#70b989", eye: "#d7ffb2", glow: "rgba(80, 120, 80, 0.75)" },
  dash: { radius: 22, hp: 2, maxHp: 2, body: "#1d6c5b", outline: "#4dd4b5", eye: "#d9fffb", glow: "rgba(96, 214, 158, 0.72)" },
  boss: { radius: 34, hp: 24, maxHp: 24, body: "#6c3340", outline: "#d07d86", eye: "#ffd8b1", glow: "rgba(180, 50, 50, 0.82)" },
};

function drawZombiePreview(canvas, type = "normal") {
  const model = ZOMBIE_PREVIEW_CONFIG[type] || ZOMBIE_PREVIEW_CONFIG.normal;
  const logicalWidth = 140;
  const logicalHeight = 120;
  const dpr = window.devicePixelRatio || 1;
  canvas.width = Math.round(logicalWidth * dpr);
  canvas.height = Math.round(logicalHeight * dpr);

  const previewCtx = canvas.getContext("2d");
  if (!previewCtx) return;

  previewCtx.setTransform(dpr, 0, 0, dpr, 0, 0);
  previewCtx.clearRect(0, 0, logicalWidth, logicalHeight);

  const centerX = logicalWidth / 2;
  const centerY = logicalHeight * 0.62;

  previewCtx.shadowBlur = 20;
  previewCtx.shadowColor = model.glow;
  previewCtx.fillStyle = model.body;
  previewCtx.beginPath();
  previewCtx.arc(centerX, centerY, model.radius, 0, Math.PI * 2);
  previewCtx.fill();

  previewCtx.shadowBlur = 0;
  previewCtx.strokeStyle = model.outline;
  previewCtx.lineWidth = type === "boss" || type === "tank" ? 3 : 2;
  previewCtx.beginPath();
  previewCtx.arc(centerX, centerY, model.radius, 0, Math.PI * 2);
  previewCtx.stroke();

  previewCtx.shadowBlur = 10;
  previewCtx.shadowColor = model.eye;
  previewCtx.fillStyle = model.eye;
  const eyeSize = type === "boss" ? 6 : type === "tank" ? 5 : 4;
  previewCtx.beginPath();
  previewCtx.arc(centerX - model.radius * 0.3, centerY - model.radius * 0.12, eyeSize, 0, Math.PI * 2);
  previewCtx.arc(centerX + model.radius * 0.24, centerY + model.radius * 0.14, eyeSize * 0.9, 0, Math.PI * 2);
  previewCtx.fill();
  previewCtx.shadowBlur = 0;

  const hpRatio = model.hp / model.maxHp;
  const hpBarWidth = Math.max(58, model.radius * 2.2);
  const hpBarHeight = 7;
  const hpX = centerX - hpBarWidth / 2;
  const hpY = 10;
  previewCtx.fillStyle = "rgba(4, 10, 7, 0.74)";
  previewCtx.fillRect(hpX, hpY, hpBarWidth, hpBarHeight);
  previewCtx.fillStyle = COLORS.hpFill;
  previewCtx.fillRect(hpX, hpY, hpBarWidth * hpRatio, hpBarHeight);
}

function renderEncyclopediaZombiePreviews() {
  const canvases = document.querySelectorAll("[data-zombie-preview]");
  canvases.forEach((canvas) => {
    const type = canvas.dataset.zombiePreview || "normal";
    drawZombiePreview(canvas, type);
  });
}

// Табы магазина
document.querySelectorAll('.shop-tab').forEach(tab => {
  tab.addEventListener('click', () => {
    const target = tab.dataset.shopTab;
    if (!target) return;

    document.querySelectorAll('.shop-tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.shop-content').forEach(c => c.classList.remove('active'));

    tab.classList.add('active');
    const targetId = 'shop' + target.charAt(0).toUpperCase() + target.slice(1);
    const targetContent = document.getElementById(targetId);
    if (targetContent) {
      targetContent.classList.add('active');
    }

    if (target === 'weapons') {
      renderWeaponShop();
    }
  });
});

function initAudio() {
  if (audio.ctx) {
    return;
  }

  const AudioContext = window.AudioContext || window.webkitAudioContext;
  if (!AudioContext) {
    return;
  }

  audio.ctx = new AudioContext();
  audio.master = audio.ctx.createGain();
  audio.master.gain.value = audio.enabled ? 0.7 : 0;
  audio.master.connect(audio.ctx.destination);

  audio.musicGain = audio.ctx.createGain();
  audio.musicGain.gain.value = 0;
  audio.musicGain.connect(audio.master);

  audio.ambientGain = audio.ctx.createGain();
  audio.ambientGain.gain.value = 0;
  audio.ambientGain.connect(audio.master);

  audio.sfxGain = audio.ctx.createGain();
  audio.sfxGain.gain.value = 0.8;
  audio.sfxGain.connect(audio.master);

  updateMusicMix();
}

function unlockAudio() {
  initAudio();
  if (audio.ctx && audio.ctx.state === "suspended") {
    audio.ctx.resume();
  }
  updateMusicMix();
}

function setAudioEnabled(enabled) {
  audio.enabled = enabled;
  if (audio.master) {
    audio.master.gain.value = enabled ? 0.7 : 0;
  }
  updateMusicMix();
  updateSoundButton();
}

function updateMusicMix() {
  if (audio.musicGain) {
    audio.musicGain.gain.value = 0;
  }
  updateAmbientTrack();
}

async function loadAmbientLoopBuffer() {
  if (!audio.ctx) return null;
  if (audio.ambientBuffer) return audio.ambientBuffer;
  if (!audio.ambientLoadPromise) {
    audio.ambientLoadPromise = fetch("assets/omega.mp3")
      .then((response) => {
        if (!response.ok) {
          throw new Error(`Ambient fetch failed: ${response.status}`);
        }
        return response.arrayBuffer();
      })
      .then((arrayBuffer) =>
        new Promise((resolve, reject) => {
          audio.ctx.decodeAudioData(arrayBuffer, resolve, reject);
        })
      )
      .then((decoded) => {
        audio.ambientBuffer = decoded;
        return decoded;
      })
      .catch((error) => {
        console.error("Ambient decode error:", error);
        audio.ambientLoadPromise = null;
        return null;
      });
  }
  return audio.ambientLoadPromise;
}

function stopAmbientLoop() {
  if (audio.ambientSource) {
    try {
      audio.ambientSource.stop();
    } catch (error) {}
    audio.ambientSource.disconnect();
    audio.ambientSource = null;
  }
}

async function startAmbientLoop() {
  if (!audio.ctx || !audio.ambientGain || audio.ambientSource) {
    return;
  }
  const buffer = audio.ambientBuffer || (await loadAmbientLoopBuffer());
  if (!buffer) return;

  const source = audio.ctx.createBufferSource();
  source.buffer = buffer;
  source.loop = true;
  source.connect(audio.ambientGain);
  source.onended = () => {
    if (audio.ambientSource === source) {
      audio.ambientSource = null;
    }
  };
  source.start(0);
  audio.ambientSource = source;
}

function updateAmbientTrack() {
  if (!audio.ctx || !audio.ambientGain) {
    return;
  }
  const gameActive = state.status === "running";
  const targetGain = gameActive ? AMBIENT_GAIN_GAME : AMBIENT_GAIN_MENU;
  const pageVisible = typeof document.visibilityState === "string" ? document.visibilityState !== "hidden" : true;
  const shouldPlay = audio.enabled && pageVisible;
  const now = audio.ctx.currentTime;
  const gainNode = audio.ambientGain.gain;

  if (shouldPlay) {
    if (audio.ambientStopTimer) {
      clearTimeout(audio.ambientStopTimer);
      audio.ambientStopTimer = null;
    }
    gainNode.cancelScheduledValues(now);
    gainNode.setTargetAtTime(targetGain, now, 0.16);
    startAmbientLoop().catch((error) => {
      console.error("Ambient loop start error:", error);
    });
  } else {
    gainNode.cancelScheduledValues(now);
    gainNode.setTargetAtTime(0, now, 0.08);
    if (audio.ambientStopTimer) {
      clearTimeout(audio.ambientStopTimer);
    }
    audio.ambientStopTimer = setTimeout(() => {
      stopAmbientLoop();
      audio.ambientStopTimer = null;
    }, 260);
  }
}

function playTone({ frequency, duration, type, volume }) {
  if (!audio.enabled) {
    return;
  }

  unlockAudio();
  if (!audio.ctx || !audio.sfxGain) {
    return;
  }

  const now = audio.ctx.currentTime;
  const osc = audio.ctx.createOscillator();
  const gain = audio.ctx.createGain();
  osc.type = type;
  osc.frequency.value = frequency;
  gain.gain.setValueAtTime(volume, now);
  gain.gain.exponentialRampToValueAtTime(0.0001, now + duration);
  osc.connect(gain).connect(audio.sfxGain);
  osc.start(now);
  osc.stop(now + duration + 0.02);
}

function playShotSound() {
  playTone({ frequency: 520, duration: 0.07, type: "square", volume: 0.12 });
}

function playHitSound() {
  playTone({ frequency: 210, duration: 0.12, type: "sawtooth", volume: 0.16 });
  playTone({ frequency: 120, duration: 0.18, type: "triangle", volume: 0.11 });
}

function playDamageSound() {
  playTone({ frequency: 90, duration: 0.2, type: "sawtooth", volume: 0.14 });
}

function getCanvasRenderDpr() {
  const rawDpr = window.devicePixelRatio || 1;
  const isSmallScreen = window.matchMedia("(max-width: 900px)").matches;
  if (isSmallScreen) {
    return Math.min(rawDpr, 1.2);
  }
  return Math.min(rawDpr, 1.9);
}

function resizeCanvas() {
  const rect = canvas.getBoundingClientRect();
  const dpr = getCanvasRenderDpr();
  canvas.width = Math.round(rect.width * dpr);
  canvas.height = Math.round(rect.height * dpr);
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  width = rect.width;
  height = rect.height;
  const ownCore = getOwnCorePosition();
  state.pointer.x = ownCore.x;
  state.pointer.y = ownCore.y;
}

function resetGame() {
  zombieIdSeed = 1;
  // Применить прокачку если игрок авторизован
  let hpBonus = 0;
  let maxHpBonus = 0;
  if (authState.profile) {
    const bonuses = applyHpUpgrades(authState.profile);
    hpBonus = bonuses.hpBonus;
    maxHpBonus = bonuses.maxHpBonus;
  }
  
  state.score = 0;
  state.wave = 1;
  state.maxHp = 5 + maxHpBonus;
  state.hp = state.maxHp + hpBonus; // Лечение при старте
  state.combo = 0;
  state.lastKill = 0;
  state.lastShot = 0;
  state.lastSpawn = performance.now();
  state.spawnIntervalBase = 1100;
  state.zombies = [];
  state.bullets = [];
  state.bursts = [];
  state.particles = [];
  state.floaters = [];
  state.powerups = [];
  state.effects = { rapid: 0, shield: 0, slow: 0 };
  state.weapon = "blaster";
  state.lastBossWave = 0;
  state.beamActive = false;
  state.beamTarget = null;
  resetAtmosphere();
  state.event = { type: null, timer: 0, wave: 0 };
  state.flash = 0;
  state.shake = 0;
  state.pointer.down = false;
  state.pointer.active = false;
  const ownCore = getOwnCorePosition();
  state.pointer.x = ownCore.x;
  state.pointer.y = ownCore.y;
  updateHud();
  updateWeaponButtons();
  updateStatusLine();
}

function showOverlay(title, text, buttonText) {
  overlayTitle.textContent = title;
  overlayText.textContent = text;
  playBtn.textContent = buttonText;
  overlay.classList.remove("hidden");
}

function hideOverlay() {
  overlay.classList.add("hidden");
}

function startRun({ reset }) {
  if (coopState.active) {
    if (coopState.roomStatus === "ended") {
      showShopMessage("Это кооп-лобби уже завершено. Создай новое.", "error");
      return;
    }
    if (!coopState.remoteConnected) {
      showShopMessage("Кооп-матч требует 2 игроков. Ждём союзника.", "error");
      return;
    }
    if (coopState.roomStatus === "waiting" && coopState.role !== "host") {
      showShopMessage("Запуск матча делает хост лобби.", "error");
      return;
    }
    if (state.status === "ko") {
      showShopMessage("Твоё ядро уничтожено. Дождись завершения кооп-матча.", "error");
      return;
    }
  }

  if (reset) {
    resetGame();
    coopState.resultSaved = false;
  }
  state.running = true;
  state.status = "running";
  hideOverlay();
  updatePauseButton();
  canvas.focus();
  unlockAudio();
  updateMusicMix();

  if (coopState.active) {
    coopState.roomStatus = "running";
    updateCoopRoom({
      status: "running",
      endedReason: "",
      updatedAt: makeServerTimestamp(),
    });
    syncCoopState(performance.now(), true);
    updateCoopUI();
  }
}

function pauseGame() {
  if (state.status !== "running") {
    return;
  }
  state.running = false;
  state.status = "paused";
  state.pointer.down = false;
  state.beamActive = false;
  showOverlay("Пауза", "Нажми «Продолжить», чтобы вернуться в игру.", "Продолжить");
  updatePauseButton();
  updateMusicMix();
}

function endGame() {
  if (coopState.active && coopState.roomStatus === "running") {
    handleLocalCoreDestroyed();
    return;
  }

  state.running = false;
  state.status = "over";
  state.beamActive = false;
  showOverlay("Конец игры", `Очки: ${state.score}. Нажми «Играть», чтобы начать заново.`, "Играть");
  updatePauseButton();
  updateMusicMix();

  // Сохраняем результат
  if (authState.user) {
    saveGameResult(state.score, state.wave);
    updateLeaderboard();
  }
}

function updatePauseButton() {
  if (state.status === "paused") {
    pauseBtn.textContent = "▶ Продолжить";
    pauseBtn.disabled = false;
    return;
  }

  pauseBtn.textContent = "⏸ Пауза";
  pauseBtn.disabled = state.status === "idle" || state.status === "over";
}

function updateSoundButton() {
  soundBtn.textContent = audio.enabled ? "🔊 Звук: Вкл" : "🔇 Звук: Выкл";
}

function isWeaponOwned(weaponKey, profile = authState.profile) {
  if (weaponKey === "blaster") return true;
  if (!profile || !profile.weapons) return false;
  return Boolean(profile.weapons[weaponKey]?.owned);
}

function updateWeaponButtons() {
  if (!isWeaponOwned(state.weapon)) {
    state.weapon = "blaster";
  }

  weaponButtons.forEach((button) => {
    const weapon = button.dataset.weapon;
    const owned = isWeaponOwned(weapon);
    button.classList.toggle("active", weapon === state.weapon);
    button.classList.toggle("locked", !owned);
    button.disabled = !owned;
    button.title = owned
      ? ""
      : authState.profile
        ? "Купи это оружие в магазине"
        : "Войдите в аккаунт, чтобы открыть это оружие";
  });
}

function updateStatusLine() {
  const active = [];
  const timeCfg = getCurrentTimeConfig();
  const weatherCfg = getCurrentWeatherConfig();
  active.push(`Атмосфера: ${timeCfg.icon} ${timeCfg.label} / ${weatherCfg.icon} ${weatherCfg.label}`);

  if (state.event.type) {
    const config = getEventConfig();
    if (config) {
      active.push(`Событие: ${config.label} ${state.event.timer.toFixed(0)}с`);
    }
  }
  if (state.effects.rapid > 0) {
    active.push(`${POWERUP_LABELS.rapid} ${state.effects.rapid.toFixed(0)}с`);
  }
  if (state.effects.shield > 0) {
    active.push(`${POWERUP_LABELS.shield} ${state.effects.shield.toFixed(0)}с`);
  }
  if (state.effects.slow > 0) {
    active.push(`${POWERUP_LABELS.slow} ${state.effects.slow.toFixed(0)}с`);
  }
  statusLine.textContent = active.length ? `Статус: ${active.join(" · ")}` : "Статус: спокойно";
}

function setWeapon(type) {
  if (!WEAPONS[type]) {
    return;
  }
  if (!isWeaponOwned(type)) {
    const message = authState.profile
      ? "Сначала купите это оружие в магазине!"
      : "Войдите в аккаунт и откройте это оружие в магазине!";
    showShopMessage(message, "error");
    return;
  }
  state.weapon = type;
  updateWeaponButtons();
  if (coopState.active) {
    syncCoopState(performance.now(), true);
  }
}

function getEventConfig() {
  if (!state.event.type) {
    return null;
  }
  return EVENTS[state.event.type] || null;
}

function startEvent(type) {
  const config = EVENTS[type];
  if (!config) {
    return;
  }
  state.event = { type, timer: config.duration, wave: state.wave };
  updateStatusLine();
}

function maybeStartEvent() {
  if (state.event.type) {
    return;
  }
  if (Math.random() > 0.6) {
    return;
  }
  const types = Object.keys(EVENTS);
  const type = types[Math.floor(Math.random() * types.length)];
  startEvent(type);
}

function updateEvent(dt) {
  if (!state.event.type) {
    return;
  }
  state.event.timer -= dt;
  updateStatusLine();
  if (state.event.timer <= 0) {
    state.event = { type: null, timer: 0, wave: state.wave };
    updateStatusLine();
  }
}

function getSpawnInterval() {
  const config = getEventConfig();
  const eventMultiplier = config ? config.spawn : 1;
  const atmosphereMultiplier = getAtmosphereModifiers().spawn;
  return state.spawnIntervalBase / (eventMultiplier * atmosphereMultiplier);
}

function getSpeedMultiplier() {
  const config = getEventConfig();
  const eventMultiplier = config ? config.speed : 1;
  return eventMultiplier * getAtmosphereModifiers().speed;
}

function getScoreMultiplier() {
  const config = getEventConfig();
  const eventMultiplier = config ? config.score : 1;
  return eventMultiplier * getAtmosphereModifiers().score;
}

function initEmbers() {
  if (!emberLayer || emberLayer.childElementCount > 0) {
    return;
  }
  const count = 28;
  const fragment = document.createDocumentFragment();
  for (let i = 0; i < count; i += 1) {
    const ember = document.createElement("span");
    ember.className = "ember";
    const left = Math.random() * 100;
    const drift = (Math.random() * 30 - 15).toFixed(2);
    const duration = 9 + Math.random() * 9;
    const delay = -Math.random() * duration;
    const scale = 0.4 + Math.random() * 1.2;
    ember.style.left = `${left}vw`;
    ember.style.setProperty("--drift", `${drift}vw`);
    ember.style.setProperty("--duration", `${duration.toFixed(2)}s`);
    ember.style.setProperty("--delay", `${delay.toFixed(2)}s`);
    ember.style.setProperty("--scale", scale.toFixed(2));
    fragment.appendChild(ember);
  }
  emberLayer.appendChild(fragment);
}

function getSpawnPoint() {
  const edge = Math.floor(Math.random() * 4);
  const pad = 50;
  if (edge === 0) {
    return { x: Math.random() * width, y: -pad };
  }
  if (edge === 1) {
    return { x: width + pad, y: Math.random() * height };
  }
  if (edge === 2) {
    return { x: Math.random() * width, y: height + pad };
  }
  return { x: -pad, y: Math.random() * height };
}

function pickZombieType() {
  if (state.wave < 3) {
    return "normal";
  }
  const roll = Math.random();
  if (roll < 0.55) {
    return "normal";
  }
  if (roll < 0.75) {
    return "fast";
  }
  if (roll < 0.92) {
    return "tank";
  }
  return "dash";
}

function createZombie(type, x, y) {
  const baseSpeed = 38 + state.wave * 6;
  let speed = baseSpeed + Math.random() * 14;
  let radius = 16 + Math.random() * 8;
  let hp = 2;
  const zombie = {
    id: zombieIdSeed++,
    x,
    y,
    r: radius,
    wobble: Math.random() * Math.PI,
    type,
    dashCooldown: 0,
    dashTimer: 0,
    targetCore: "own",
  };

  if (type === "fast") {
    speed = baseSpeed * 1.6;
    radius = 12 + Math.random() * 6;
    hp = 1;
  } else if (type === "tank") {
    speed = baseSpeed * 0.65;
    radius = 24 + Math.random() * 10;
    hp = 5;
  } else if (type === "dash") {
    speed = baseSpeed * 1.15;
    radius = 16 + Math.random() * 6;
    hp = 2;
    zombie.dashCooldown = 1.4 + Math.random() * 1.6;
  } else if (type === "boss") {
    speed = baseSpeed * 0.55;
    radius = 36;
    hp = 22 + state.wave * 2;
  }

  zombie.r = radius;
  zombie.hp = hp;
  zombie.maxHp = hp;
  zombie.baseSpeed = speed;
  if (isDualCoreBattleActive() && type !== "boss" && Math.random() < 0.4) {
    zombie.targetCore = "ally";
  }

  return zombie;
}

function spawnZombie(type = pickZombieType()) {
  const point = getSpawnPoint();
  state.zombies.push(createZombie(type, point.x, point.y));
}

function spawnBoss() {
  const bossAlive = state.zombies.some((zombie) => zombie.type === "boss");
  if (bossAlive) {
    return;
  }
  const point = getSpawnPoint();
  state.zombies.push(createZombie("boss", point.x, point.y));
}

function getShotCooldown() {
  const base = WEAPONS[state.weapon].cooldown;
  let cooldown = state.effects.rapid > 0 ? base * 0.55 : base;
  
  // Применить бонус скорострельности
  if (authState.profile && authState.profile.weapons && authState.profile.weapons[state.weapon]) {
    const fireRateBonus = getWeaponFireRateBonus(authState.profile, state.weapon);
    cooldown *= Math.max(0.2, 1 - fireRateBonus);
  }
  
  return cooldown;
}

function spawnBullet({ x, y, vx, vy, life, r, damage, type = "bullet", splash = 0 }) {
  // Применить бонус урона
  if (authState.profile && authState.profile.weapons && authState.profile.weapons[state.weapon]) {
    const damageBonus = getWeaponDamageBonus(authState.profile, state.weapon);
    damage = damage * (1 + damageBonus);
  }
  state.bullets.push({
    x,
    y,
    vx,
    vy,
    life,
    r,
    damage,
    type,
    splash,
    origin: isGuestMirrorMode() ? "local" : "host",
  });
}

function getAccuracyJitter(baseSpread = 0.04) {
  const accuracy = getAtmosphereModifiers().accuracy;
  if (accuracy >= 1.02) {
    return 0;
  }
  const penalty = 1 - accuracy;
  return baseSpread + penalty * 0.22;
}

function shoot(targetX, targetY, now) {
  if (!state.running) {
    return;
  }

  if (state.weapon === "beam") {
    return;
  }

  const cooldown = getShotCooldown();
  if (now - state.lastShot < cooldown) {
    return;
  }

  state.lastShot = now;
  
  // Muzzle flash!
  state.muzzleFlash = 1;

  const ownCore = getOwnCorePosition();
  const originX = ownCore.x;
  const originY = ownCore.y;
  const dx = targetX - originX;
  const dy = targetY - originY;
  const dist = Math.hypot(dx, dy) || 1;
  const angle = Math.atan2(dy, dx);

  const weapon = WEAPONS[state.weapon];
  const speed = weapon.speed + state.wave * 22;
  const jitter = getAccuracyJitter();

  if (state.weapon === "shotgun") {
    for (let i = 0; i < weapon.pellets; i += 1) {
      const spread = (Math.random() - 0.5) * (weapon.spread + jitter * 1.25);
      const pelletAngle = angle + spread;
      const vx = Math.cos(pelletAngle) * speed;
      const vy = Math.sin(pelletAngle) * speed;
      spawnBullet({
        x: originX,
        y: originY,
        vx,
        vy,
        life: 0.7,
        r: 3,
        damage: weapon.damage,
      });
    }
  } else if (state.weapon === "grenade") {
    const finalAngle = angle + (Math.random() - 0.5) * jitter * 0.55;
    const vx = Math.cos(finalAngle) * speed;
    const vy = Math.sin(finalAngle) * speed;
    spawnBullet({
      x: originX,
      y: originY,
      vx,
      vy,
      life: weapon.life,
      r: 6,
      damage: weapon.damage,
      type: "grenade",
      splash: weapon.splash,
    });
  } else {
    const finalAngle = angle + (Math.random() - 0.5) * jitter;
    const vx = Math.cos(finalAngle) * speed;
    const vy = Math.sin(finalAngle) * speed;
    spawnBullet({
      x: originX,
      y: originY,
      vx,
      vy,
      life: 0.85,
      r: 4,
      damage: weapon.damage,
    });
  }

  state.bursts.push({
    x: originX,
    y: originY,
    life: 0.18,
    max: 0.18,
    type: "shot",
    origin: isGuestMirrorMode() ? "local" : "host",
  });
  playShotSound();
}

function spawnHitParticles(x, y, count = 12) {
  for (let i = 0; i < count; i += 1) {
    const angle = Math.random() * Math.PI * 2;
    const speed = 80 + Math.random() * 160;
    state.particles.push({
      x,
      y,
      vx: Math.cos(angle) * speed,
      vy: Math.sin(angle) * speed,
      life: 0.45 + Math.random() * 0.25,
      max: 0.7,
      size: 2 + Math.random() * 2.5,
    });
  }
}

function maybeSpawnPowerup(x, y) {
  if (Math.random() > 0.18) {
    return;
  }
  const types = ["rapid", "shield", "slow"];
  const type = types[Math.floor(Math.random() * types.length)];
  state.powerups.push({ x, y, type, life: 8 });
}

function applyPowerup(type, x, y) {
  const duration = POWERUP_DURATION[type] || 5;
  state.effects[type] = Math.min(state.effects[type] + duration, 12);
  const label = POWERUP_FLOATERS[type] || type.toUpperCase();
  state.floaters.push({ x, y, life: 1.2, max: 1.2, value: label });
  updateStatusLine();
}

function applyDamage(zombie, amount, now, particleCount = 6) {
  zombie.hp -= amount;
  spawnHitParticles(zombie.x, zombie.y, particleCount);
  if (zombie.hp <= 0) {
    registerKill(zombie.x, zombie.y, now);
    return true;
  }
  return false;
}

function registerKill(x, y, now) {
  const scoreGain = Math.round(getScoreMultiplier());
  state.score += scoreGain;
  if (now - state.lastKill < 1200) {
    state.combo += 1;
  } else {
    state.combo = 1;
  }
  state.lastKill = now;
  state.shake = Math.min(18, state.shake + 4);
  state.bursts.push({ x, y, life: 0.4, max: 0.4, type: "kill" });
  state.floaters.push({ x, y, life: 1, max: 1, value: `+${scoreGain}` });
  state.flash = Math.min(0.45, state.flash + 0.18);
  spawnHitParticles(x, y);
  maybeSpawnPowerup(x, y);
  playHitSound();
  updateHud();
}

function updateHud() {
  scoreEl.textContent = state.score;
  waveEl.textContent = state.wave;
  hpEl.textContent = state.hp;
  comboEl.textContent = state.combo;
  if (coopOwnCore) {
    coopOwnCore.textContent = "HP: " + Math.max(0, state.hp) + "/" + Math.max(1, state.maxHp);
  }
}

function updateWave() {
  if (coopState.active && coopState.roomStatus === "running" && coopState.role === "guest") {
    state.spawnIntervalBase = Math.max(320, 1050 - state.wave * 60);
    return;
  }

  const nextWave = 1 + Math.floor(state.score / 20);
  if (nextWave !== state.wave) {
    state.wave = nextWave;
    if (state.wave % 5 === 0 && state.lastBossWave !== state.wave) {
      spawnBoss();
      state.lastBossWave = state.wave;
    }
    maybeStartEvent();
  }
  state.spawnIntervalBase = Math.max(320, 1050 - state.wave * 60);
}

function updateEffects(dt) {
  let changed = false;
  Object.keys(state.effects).forEach((key) => {
    if (state.effects[key] > 0) {
      state.effects[key] = Math.max(0, state.effects[key] - dt);
      changed = true;
    }
  });
  if (changed) {
    updateStatusLine();
  }
}

function updatePowerups(dt) {
  const ownCore = getOwnCorePosition();
  const playerX = ownCore.x;
  const playerY = ownCore.y;
  const collectRadius = 22;
  for (let i = state.powerups.length - 1; i >= 0; i -= 1) {
    const powerup = state.powerups[i];
    const dx = playerX - powerup.x;
    const dy = playerY - powerup.y;
    const dist = Math.hypot(dx, dy) || 1;
    const speed = 22;
    powerup.x += (dx / dist) * speed * dt;
    powerup.y += (dy / dist) * speed * dt;
    powerup.life -= dt;
    if (dist < collectRadius) {
      applyPowerup(powerup.type, powerup.x, powerup.y);
      state.powerups.splice(i, 1);
      continue;
    }
    if (powerup.life <= 0) {
      state.powerups.splice(i, 1);
    }
  }
}

function fireBeam(dt, now) {
  state.beamActive = true;
  const ownCore = getOwnCorePosition();
  const originX = ownCore.x;
  const originY = ownCore.y;
  let targetX = state.pointer.x;
  let targetY = state.pointer.y;

  const dx = targetX - originX;
  const dy = targetY - originY;
  const initialDist = Math.hypot(dx, dy) || 1;
  const beamSpread = getAccuracyJitter(0.018) * 0.22;
  const spreadAngle = Math.atan2(dy, dx) + (Math.random() - 0.5) * beamSpread;
  const finalDist = initialDist;
  const nx = Math.cos(spreadAngle);
  const ny = Math.sin(spreadAngle);
  targetX = originX + nx * finalDist;
  targetY = originY + ny * finalDist;
  state.beamTarget = { x: targetX, y: targetY };
  const beamWidth = WEAPONS.beam.width * 0.5;
  
  let beamDps = WEAPONS.beam.dps;
  if (authState.profile && authState.profile.weapons && authState.profile.weapons.beam) {
    const beamProgress = authState.profile.weapons.beam;
    beamDps = getBeamDpsAtLevels(beamProgress.damageLevel, beamProgress.fireRateLevel);
  }
  const damage = beamDps * dt;

  for (let i = state.zombies.length - 1; i >= 0; i -= 1) {
    const zombie = state.zombies[i];
    const zx = zombie.x - originX;
    const zy = zombie.y - originY;
    const proj = zx * nx + zy * ny;
    if (proj < 0 || proj > finalDist + zombie.r) {
      continue;
    }
    const perpendicular = Math.abs(zx * ny - zy * nx);
    if (perpendicular < zombie.r + beamWidth) {
      if (applyDamage(zombie, damage, now, 2)) {
        state.zombies.splice(i, 1);
      }
    }
  }
}

function applyRemoteRayDamage(originX, originY, targetX, targetY, damage, now, thickness = 10, limitHits = 1) {
  const dx = targetX - originX;
  const dy = targetY - originY;
  const rayLength = Math.hypot(dx, dy) || 1;
  const nx = dx / rayLength;
  const ny = dy / rayLength;
  let hits = 0;

  for (let i = state.zombies.length - 1; i >= 0; i -= 1) {
    const zombie = state.zombies[i];
    const zx = zombie.x - originX;
    const zy = zombie.y - originY;
    const projection = zx * nx + zy * ny;
    if (projection < 0 || projection > rayLength + zombie.r) {
      continue;
    }
    const perpendicular = Math.abs(zx * ny - zy * nx);
    if (perpendicular > zombie.r + thickness) {
      continue;
    }
    if (applyDamage(zombie, damage, now, 3)) {
      state.zombies.splice(i, 1);
    }
    hits += 1;
    if (hits >= limitHits) {
      break;
    }
  }
}

function applyRemoteSupportFire(dt, now) {
  if (!isDualCoreBattleActive() || !coopState.remoteConnected || !coopState.remoteAlive || state.status !== "running") {
    coopState.remoteBeamActive = false;
    coopState.remoteBeamTarget = null;
    return;
  }

  if (coopState.remoteLastPacketAt <= 0 || performance.now() - coopState.remoteLastPacketAt > 2200) {
    coopState.remotePointerDown = false;
  }
  if (!coopState.remotePointerDown) {
    coopState.remoteBeamActive = false;
    coopState.remoteBeamTarget = null;
    return;
  }

  const allyCore = getAllyCorePosition();
  if (!allyCore) return;
  const target = getRemoteAimPoint();
  const weaponKey = WEAPONS[coopState.remoteWeapon] ? coopState.remoteWeapon : "blaster";
  const canApplyDamage = coopState.role === "host";

  if (weaponKey === "beam") {
    coopState.remoteBeamActive = true;
    coopState.remoteBeamTarget = { x: target.x, y: target.y };
    if (canApplyDamage) {
      const beamDps = WEAPONS.beam.dps * 0.72;
      applyRemoteRayDamage(
        allyCore.x,
        allyCore.y,
        target.x,
        target.y,
        beamDps * dt,
        now,
        WEAPONS.beam.width * 0.55,
        3
      );
    }
    return;
  }

  coopState.remoteBeamActive = false;
  coopState.remoteBeamTarget = null;
  coopState.remoteShotTimer = Math.max(0, coopState.remoteShotTimer - dt);
  if (coopState.remoteShotTimer > 0) return;

  const cooldown = Math.max(0.12, (WEAPONS[weaponKey].cooldown || 240) / 1000);
  coopState.remoteShotTimer = cooldown;
  coopState.remoteMuzzle = 1;

  if (weaponKey === "grenade") {
    const gx = allyCore.x + (target.x - allyCore.x) * 0.68;
    const gy = allyCore.y + (target.y - allyCore.y) * 0.68;
    pushRemoteTracer({
      x1: allyCore.x,
      y1: allyCore.y,
      x2: gx,
      y2: gy,
      life: 0.14,
      max: 0.14,
      width: 3.4,
      color: "rgba(174, 228, 255, 0.86)",
    });
    const radius = (WEAPONS.grenade.splash || 70) * 0.9;
    const grenadeDamage = WEAPONS.grenade.damage * 0.72;
    state.bursts.push({ x: gx, y: gy, life: 0.42, max: 0.42, type: "blast" });
    if (canApplyDamage) {
      for (let i = state.zombies.length - 1; i >= 0; i -= 1) {
        const zombie = state.zombies[i];
        if (Math.hypot(zombie.x - gx, zombie.y - gy) < radius + zombie.r) {
          if (applyDamage(zombie, grenadeDamage, now, 8)) {
            state.zombies.splice(i, 1);
          }
        }
      }
    }
    return;
  }

  if (weaponKey === "shotgun") {
    const baseAngle = Math.atan2(target.y - allyCore.y, target.x - allyCore.x);
    const rayLength = Math.max(150, Math.hypot(target.x - allyCore.x, target.y - allyCore.y));
    for (let p = 0; p < 4; p += 1) {
      const spread = (Math.random() - 0.5) * 0.26;
      const angle = baseAngle + spread;
      const shotX = allyCore.x + Math.cos(angle) * rayLength;
      const shotY = allyCore.y + Math.sin(angle) * rayLength;
      pushRemoteTracer({
        x1: allyCore.x,
        y1: allyCore.y,
        x2: shotX,
        y2: shotY,
        life: 0.1,
        max: 0.1,
        width: 2.2,
        color: "rgba(157, 224, 255, 0.78)",
      });
      if (canApplyDamage) {
        applyRemoteRayDamage(allyCore.x, allyCore.y, shotX, shotY, WEAPONS.shotgun.damage * 0.74, now, 14, 1);
      }
    }
    return;
  }

  pushRemoteTracer({
    x1: allyCore.x,
    y1: allyCore.y,
    x2: target.x,
    y2: target.y,
    life: 0.11,
    max: 0.11,
    width: 2.4,
    color: "rgba(152, 223, 255, 0.86)",
  });
  if (canApplyDamage) {
    applyRemoteRayDamage(allyCore.x, allyCore.y, target.x, target.y, WEAPONS.blaster.damage * 0.78, now, 10, 1);
  }
}

function explodeGrenade(grenade, now) {
  const radius = grenade.splash || 70;
  state.bursts.push({ x: grenade.x, y: grenade.y, life: 0.5, max: 0.5, type: "blast" });
  spawnHitParticles(grenade.x, grenade.y, 18);
  state.shake = Math.min(24, state.shake + 6);
  state.flash = Math.min(0.6, state.flash + 0.22);
  playHitSound();

  for (let i = state.zombies.length - 1; i >= 0; i -= 1) {
    const zombie = state.zombies[i];
    const dist = Math.hypot(zombie.x - grenade.x, zombie.y - grenade.y);
    if (dist < radius + zombie.r) {
      if (applyDamage(zombie, grenade.damage, now, 10)) {
        state.zombies.splice(i, 1);
      }
    }
  }
}

function update(dt, now) {
  const guestMirror = isGuestMirrorMode();
  updateWave();

  updateAtmosphere(dt);
  updateEffects(dt);
  updateEvent(dt);
  if (guestMirror) {
    coopState.lastGuestFxAt = now;
  }

  // Update muzzle flash
  if (state.muzzleFlash > 0) {
    state.muzzleFlash -= 0.15;
    if (state.muzzleFlash < 0) state.muzzleFlash = 0;
  }
  if (coopState.remoteMuzzle > 0) {
    coopState.remoteMuzzle = Math.max(0, coopState.remoteMuzzle - dt * 7);
  }
  
  if (!guestMirror) {
    updatePowerups(dt);
  } else if (state.powerups.length > 0) {
    state.powerups = [];
  }
  state.beamActive = false;
  state.beamTarget = null;

  if (state.pointer.down) {
    if (state.weapon === "beam") {
      if (guestMirror) {
        state.beamActive = true;
        state.beamTarget = { x: state.pointer.x, y: state.pointer.y };
      } else {
        fireBeam(dt, now);
      }
    } else {
      shoot(state.pointer.x, state.pointer.y, now);
    }
  }
  applyRemoteSupportFire(dt, now);

  if (coopState.active && coopState.role === "host" && coopState.roomStatus === "running") {
    syncCoopWorldState(now);
  }

  const coopSpawnCap = coopState.active && coopState.roomStatus === "running" ? getCoopWorldMaxZombies() : 999;
  if (!guestMirror && now - state.lastSpawn > getSpawnInterval() && state.zombies.length < coopSpawnCap) {
    spawnZombie();
    state.lastSpawn = now;
  }

  const ownCore = getOwnCorePosition();
  const allyCore = getAllyCorePosition();
  const ownCoreRadius = 16;
  const allyCoreRadius = 16;
  const slowMultiplier = state.effects.slow > 0 ? 0.6 : 1;
  const eventSpeed = getSpeedMultiplier();

  if (!guestMirror) {
    for (let i = state.zombies.length - 1; i >= 0; i -= 1) {
      const zombie = state.zombies[i];
      const allyAlive = Boolean(allyCore) && coopState.remoteAlive;
      const targetsAlly = allyAlive && zombie.targetCore === "ally";

      if (!targetsAlly && zombie.targetCore === "ally") {
        zombie.targetCore = "own";
      }

      const targetX = targetsAlly ? allyCore.x : ownCore.x;
      const targetY = targetsAlly ? allyCore.y : ownCore.y;
      const targetRadius = targetsAlly ? allyCoreRadius : ownCoreRadius;

      const dx = targetX - zombie.x;
      const dy = targetY - zombie.y;
      const dist = Math.hypot(dx, dy) || 1;
      zombie.wobble += dt * 4;
      let speed = zombie.baseSpeed * slowMultiplier * eventSpeed;
      if (zombie.type === "dash") {
        zombie.dashCooldown -= dt;
        if (zombie.dashCooldown <= 0) {
          zombie.dashTimer = 0.35;
          zombie.dashCooldown = 2 + Math.random() * 1.4;
        }
        if (zombie.dashTimer > 0) {
          zombie.dashTimer -= dt;
          speed *= 2.6;
        }
      }

      zombie.x += (dx / dist) * speed * dt;
      zombie.y += (dy / dist) * speed * dt;

      if (dist < zombie.r + targetRadius) {
        state.zombies.splice(i, 1);
        if (targetsAlly) {
          state.bursts.push({ x: targetX, y: targetY, life: 0.2, max: 0.2, type: "shot" });
          state.flash = Math.min(0.28, state.flash + 0.06);
          if (coopState.role === "host" && coopState.roomStatus === "running") {
            coopState.remoteHp = Math.max(0, (Number(coopState.remoteHp) || 0) - 1);
            coopState.remoteAlive = coopState.remoteHp > 0;
            if (!coopState.remoteAlive) {
              coopState.remotePointerDown = false;
            }
            updateCoopUI();
            syncCoopState(now, true);
            if (!coopState.remoteAlive && state.hp <= 0) {
              finalizeCoopRoomIfHost("Оба ядра уничтожены.");
              handleCoopMatchEnded("Оба ядра уничтожены.");
              return;
            }
          }
          continue;
        }

        if (state.effects.shield > 0) {
          state.effects.shield = Math.max(0, state.effects.shield - 1.2);
          updateStatusLine();
          state.shake = Math.min(18, state.shake + 6);
          state.flash = Math.min(0.35, state.flash + 0.12);
        } else {
          state.hp -= 1;
          state.combo = 0;
          state.shake = Math.min(22, state.shake + 10);
          state.flash = Math.min(0.55, state.flash + 0.25);
          playDamageSound();
          updateHud();
          if (state.hp <= 0) {
            if (coopState.active && coopState.roomStatus === "running") {
              handleLocalCoreDestroyed();
            } else {
              endGame();
            }
            return;
          }
        }
      }
    }
  } else {
    for (let i = 0; i < state.zombies.length; i += 1) {
      const zombie = state.zombies[i];
      zombie.wobble += dt * 3.4;

      const netAt = Number(zombie.netAt) || 0;
      const netAge = netAt > 0 ? (performance.now() - netAt) / 1000 : 0;
      const predictLead = isCoopWsEnabled()
        ? clamp(0.02 + netAge * 0.22, 0.02, 0.085)
        : 0;
      const vx = Number(zombie.vx) || 0;
      const vy = Number(zombie.vy) || 0;

      const netX = Number(zombie.netX);
      const netY = Number(zombie.netY);
      const targetX = Number.isFinite(netX) ? netX + vx * predictLead : zombie.x;
      const targetY = Number.isFinite(netY) ? netY + vy * predictLead : zombie.y;

      const responsiveness = isCoopWsEnabled()
        ? netAge > 0.3
          ? 20
          : 14
        : netAge > 0.32
        ? 13
        : 10;
      const catchup = 1 - Math.exp(-responsiveness * dt);
      zombie.x += (targetX - zombie.x) * catchup;
      zombie.y += (targetY - zombie.y) * catchup;

      if (netAge > 1.25) {
        zombie.x = targetX;
        zombie.y = targetY;
      } else if (netAge > 0.95) {
        zombie.vx *= 0.65;
        zombie.vy *= 0.65;
      }

      zombie.x = clamp(zombie.x, -220, width + 220);
      zombie.y = clamp(zombie.y, -220, height + 220);
    }
  }

  if (!guestMirror) {
    for (let i = state.bullets.length - 1; i >= 0; i -= 1) {
      const bullet = state.bullets[i];
      bullet.x += bullet.vx * dt;
      bullet.y += bullet.vy * dt;
      bullet.life -= dt;

      if (bullet.type === "grenade" && bullet.life <= 0) {
        explodeGrenade(bullet, now);
        state.bullets.splice(i, 1);
        continue;
      }

      let hit = false;
      for (let j = state.zombies.length - 1; j >= 0; j -= 1) {
        const zombie = state.zombies[j];
        const dx = zombie.x - bullet.x;
        const dy = zombie.y - bullet.y;
        const dist = Math.hypot(dx, dy);
        if (dist < zombie.r + bullet.r) {
          if (bullet.type === "grenade") {
            explodeGrenade(bullet, now);
          } else if (applyDamage(zombie, bullet.damage, now)) {
            state.zombies.splice(j, 1);
          }
          state.bullets.splice(i, 1);
          hit = true;
          break;
        }
      }

      if (hit) {
        continue;
      }

      if (bullet.life <= 0 || bullet.x < -80 || bullet.x > width + 80 || bullet.y < -80 || bullet.y > height + 80) {
        state.bullets.splice(i, 1);
      }
    }
  } else {
    for (let i = state.bullets.length - 1; i >= 0; i -= 1) {
      const bullet = state.bullets[i];
      bullet.x += (Number(bullet.vx) || 0) * dt;
      bullet.y += (Number(bullet.vy) || 0) * dt;
      bullet.life = Math.max(0, (Number(bullet.life) || 0) - dt);

      if (bullet.type === "grenade" && bullet.life <= 0) {
        state.bursts.push({ x: bullet.x, y: bullet.y, life: 0.34, max: 0.34, type: "blast" });
        state.bullets.splice(i, 1);
        continue;
      }

      if (bullet.life <= 0 || bullet.x < -90 || bullet.x > width + 90 || bullet.y < -90 || bullet.y > height + 90) {
        state.bullets.splice(i, 1);
      }
    }
  }

  for (let i = state.bursts.length - 1; i >= 0; i -= 1) {
    const burst = state.bursts[i];
    burst.life -= dt;
    if (burst.life <= 0) {
      state.bursts.splice(i, 1);
    }
  }

  for (let i = state.particles.length - 1; i >= 0; i -= 1) {
    const particle = state.particles[i];
    particle.x += particle.vx * dt;
    particle.y += particle.vy * dt;
    particle.vx *= 0.92;
    particle.vy *= 0.92;
    particle.life -= dt;
    if (particle.life <= 0) {
      state.particles.splice(i, 1);
    }
  }

  for (let i = state.floaters.length - 1; i >= 0; i -= 1) {
    const floater = state.floaters[i];
    floater.y -= 28 * dt;
    floater.life -= dt;
    if (floater.life <= 0) {
      state.floaters.splice(i, 1);
    }
  }
  updateRemoteTracers(dt);

  if (coopState.active) {
    syncCoopState(now);
  }
}

function drawCoreLite(x, y) {
  ctx.save();
  ctx.fillStyle = "rgba(43, 220, 119, 0.28)";
  ctx.beginPath();
  ctx.arc(x, y, 30, 0, Math.PI * 2);
  ctx.fill();
  ctx.fillStyle = "#34d27f";
  ctx.beginPath();
  ctx.arc(x, y, 18, 0, Math.PI * 2);
  ctx.fill();
  ctx.restore();
}

function drawAllyCoreLite(x, y) {
  const hp = Math.max(0, Number(coopState.remoteHp) || 0);
  const maxHp = Math.max(1, Number(coopState.remoteMaxHp) || 5);
  const alive = hp > 0;

  ctx.save();
  ctx.fillStyle = alive ? "rgba(110, 205, 255, 0.26)" : "rgba(130, 140, 150, 0.22)";
  ctx.beginPath();
  ctx.arc(x, y, 28, 0, Math.PI * 2);
  ctx.fill();

  ctx.fillStyle = alive ? "#77cfff" : "#7a8289";
  ctx.beginPath();
  ctx.arc(x, y, 16, 0, Math.PI * 2);
  ctx.fill();

  const hpRatio = maxHp > 0 ? hp / maxHp : 0;
  drawHpBar(x - 40, y - 42, 80, 5, hpRatio, alive ? "#6ac9ff" : "#727d87", "");
  ctx.restore();
}

function drawCore(x, y) {
  // Pulsing animation
  const pulse = 1 + Math.sin(Date.now() / 300) * 0.08;
  const radius = 20 * pulse;
  
  // Outer glow
  ctx.shadowBlur = 40;
  ctx.shadowColor = COLORS.coreGlow;
  
  const inner = ctx.createRadialGradient(x, y, 2, x, y, 50);
  inner.addColorStop(0, "#ffffff");
  inner.addColorStop(0.3, COLORS.coreGlow);
  inner.addColorStop(0.7, "rgba(43, 220, 119, 0.4)");
  inner.addColorStop(1, "rgba(43, 220, 119, 0)");

  ctx.fillStyle = inner;
  ctx.beginPath();
  ctx.arc(x, y, radius + 25, 0, Math.PI * 2);
  ctx.fill();
  
  // Inner core
  ctx.shadowBlur = 20;
  const coreGrad = ctx.createRadialGradient(x, y, 0, x, y, radius);
  coreGrad.addColorStop(0, "#ffffff");
  coreGrad.addColorStop(0.5, COLORS.coreGlow);
  coreGrad.addColorStop(1, COLORS.coreEdge);
  
  ctx.fillStyle = coreGrad;
  ctx.beginPath();
  ctx.arc(x, y, radius, 0, Math.PI * 2);
  ctx.fill();

  // Core ring
  ctx.shadowBlur = 15;
  ctx.strokeStyle = COLORS.coreEdge;
  ctx.lineWidth = 2;
  ctx.beginPath();
  ctx.arc(x, y, 30 * pulse, 0, Math.PI * 2);
  ctx.stroke();

  // Shield effect
  if (state.effects.shield > 0) {
    const shieldPulse = 1 + Math.sin(Date.now() / 150) * 0.1;
    ctx.shadowBlur = 25;
    ctx.shadowColor = COLORS.shield;
    ctx.strokeStyle = COLORS.shield;
    ctx.lineWidth = 3 + state.effects.shield * 0.3;
    ctx.beginPath();
    ctx.arc(x, y, 38 * shieldPulse, 0, Math.PI * 2);
    ctx.stroke();
  }
  
  ctx.shadowBlur = 0;
}

function drawCoopCoreLink(ownCore, allyCore) {
  if (!ownCore || !allyCore) return;
  ctx.save();
  ctx.strokeStyle = "rgba(88, 212, 185, 0.32)";
  ctx.lineWidth = 2;
  ctx.setLineDash([8, 8]);
  ctx.beginPath();
  ctx.moveTo(ownCore.x, ownCore.y);
  ctx.lineTo(allyCore.x, allyCore.y);
  ctx.stroke();
  ctx.setLineDash([]);
  ctx.restore();
}

function drawAllyCore(x, y) {
  const hp = Math.max(0, Number(coopState.remoteHp) || 0);
  const maxHp = Math.max(1, Number(coopState.remoteMaxHp) || 5);
  const alive = hp > 0;
  const pulse = 1 + Math.sin(Date.now() / 380) * 0.06;
  const radius = 18 * pulse;
  const glowColor = alive ? "rgba(102, 204, 255, 0.85)" : "rgba(140, 150, 160, 0.45)";
  const edgeColor = alive ? "rgba(122, 232, 255, 0.95)" : "rgba(120, 128, 136, 0.7)";

  ctx.save();
  ctx.shadowBlur = 34;
  ctx.shadowColor = glowColor;

  const outer = ctx.createRadialGradient(x, y, 2, x, y, 44);
  outer.addColorStop(0, "rgba(255,255,255,0.96)");
  outer.addColorStop(0.38, glowColor);
  outer.addColorStop(1, "rgba(102, 204, 255, 0)");
  ctx.fillStyle = outer;
  ctx.beginPath();
  ctx.arc(x, y, radius + 20, 0, Math.PI * 2);
  ctx.fill();

  ctx.shadowBlur = 18;
  const coreGrad = ctx.createRadialGradient(x, y, 0, x, y, radius);
  coreGrad.addColorStop(0, "#f8fdff");
  coreGrad.addColorStop(0.55, alive ? "#8ad8ff" : "#89939d");
  coreGrad.addColorStop(1, alive ? "#3f87ae" : "#5f6770");
  ctx.fillStyle = coreGrad;
  ctx.beginPath();
  ctx.arc(x, y, radius, 0, Math.PI * 2);
  ctx.fill();

  ctx.shadowBlur = 12;
  ctx.strokeStyle = edgeColor;
  ctx.lineWidth = 2;
  ctx.beginPath();
  ctx.arc(x, y, 28 * pulse, 0, Math.PI * 2);
  ctx.stroke();
  ctx.shadowBlur = 0;

  if (!alive) {
    ctx.strokeStyle = "rgba(255, 98, 98, 0.8)";
    ctx.lineWidth = 2.2;
    ctx.beginPath();
    ctx.moveTo(x - 12, y - 12);
    ctx.lineTo(x + 12, y + 12);
    ctx.moveTo(x + 12, y - 12);
    ctx.lineTo(x - 12, y + 12);
    ctx.stroke();
  }

  const hpRatio = maxHp > 0 ? hp / maxHp : 0;
  drawHpBar(x - 46, y - 52, 92, 6, hpRatio, alive ? "#6ac9ff" : "#727d87", `${hp}/${maxHp}`);

  const allyName = (coopState.remoteName || "Союзник").slice(0, 14);
  ctx.fillStyle = "rgba(190, 235, 255, 0.9)";
  ctx.font = "600 10px Space Grotesk, sans-serif";
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.fillText(allyName, x, y + 42);
  ctx.restore();
}

function drawRemoteWeapon() {
  if (!isDualCoreBattleActive() || !coopState.remoteConnected || !coopState.remoteAlive) {
    return;
  }
  const allyCore = getAllyCorePosition();
  if (!allyCore) return;
  const target = getRemoteAimPoint();
  const angle = Math.atan2(target.y - allyCore.y, target.x - allyCore.x);
  const weaponKey = WEAPONS[coopState.remoteWeapon] ? coopState.remoteWeapon : "blaster";

  ctx.save();
  ctx.translate(allyCore.x, allyCore.y);
  ctx.rotate(angle);

  const grip = 16;
  let length = 22;
  let thickness = 6;
  let bodyColor = "#1e3543";
  let accentColor = "#74d5ff";

  if (weaponKey === "shotgun") {
    length = 19;
    thickness = 9;
    bodyColor = "#233a48";
    accentColor = "#9ee0ff";
  } else if (weaponKey === "beam") {
    length = 28;
    thickness = 5;
    bodyColor = "#1c2c3e";
    accentColor = "#7cd3ff";
  } else if (weaponKey === "grenade") {
    length = 18;
    thickness = 8;
    bodyColor = "#2d383f";
    accentColor = "#a6deff";
  }

  ctx.fillStyle = bodyColor;
  ctx.fillRect(grip, -thickness / 2, length, thickness);
  ctx.fillStyle = "#0f1f2b";
  ctx.fillRect(grip - 3, thickness / 2, 7, 12);
  ctx.fillStyle = accentColor;
  ctx.beginPath();
  ctx.arc(grip + length + 2, 0, 2.8, 0, Math.PI * 2);
  ctx.fill();

  if (coopState.remoteMuzzle > 0 && weaponKey !== "beam") {
    const flashSize = 13 * coopState.remoteMuzzle;
    const flash = ctx.createRadialGradient(grip + length + 10, 0, 0, grip + length + 10, 0, flashSize);
    flash.addColorStop(0, `rgba(204, 241, 255, ${coopState.remoteMuzzle})`);
    flash.addColorStop(0.35, `rgba(127, 211, 255, ${coopState.remoteMuzzle * 0.82})`);
    flash.addColorStop(1, "rgba(127, 211, 255, 0)");
    ctx.fillStyle = flash;
    ctx.beginPath();
    ctx.arc(grip + length + 10, 0, flashSize, 0, Math.PI * 2);
    ctx.fill();
  }

  ctx.restore();
}

function drawRemoteBeam() {
  if (!coopState.remoteBeamActive) return;
  const allyCore = getAllyCorePosition();
  if (!allyCore) return;
  const target = coopState.remoteBeamTarget || getRemoteAimPoint();

  ctx.save();
  ctx.strokeStyle = "rgba(126, 221, 255, 0.88)";
  ctx.lineWidth = WEAPONS.beam.width * 0.9;
  ctx.lineCap = "round";
  ctx.beginPath();
  ctx.moveTo(allyCore.x, allyCore.y);
  ctx.lineTo(target.x, target.y);
  ctx.stroke();

  ctx.strokeStyle = "rgba(229, 250, 255, 0.72)";
  ctx.lineWidth = WEAPONS.beam.width * 0.32;
  ctx.beginPath();
  ctx.moveTo(allyCore.x, allyCore.y);
  ctx.lineTo(target.x, target.y);
  ctx.stroke();
  ctx.restore();
}

function drawRemoteTracers() {
  if (!coopState.remoteTracers || coopState.remoteTracers.length === 0) return;
  ctx.save();
  for (const tracer of coopState.remoteTracers) {
    const alpha = tracer.max > 0 ? Math.max(0, tracer.life / tracer.max) : 0;
    if (alpha <= 0) continue;
    ctx.strokeStyle = tracer.color.replace(/[\d.]+\)$/g, `${alpha})`);
    ctx.lineWidth = tracer.width || 2;
    ctx.lineCap = "round";
    ctx.beginPath();
    ctx.moveTo(tracer.x1, tracer.y1);
    ctx.lineTo(tracer.x2, tracer.y2);
    ctx.stroke();
  }
  ctx.restore();
}

function drawWeapon() {
  const ownCore = getOwnCorePosition();
  const originX = ownCore.x;
  const originY = ownCore.y;
  const dx = state.pointer.x - originX;
  const dy = state.pointer.y - originY;
  const angle = Math.atan2(dy, dx);

  ctx.save();
  ctx.translate(originX, originY);
  ctx.rotate(angle);

  const grip = 18;
  let length = 24;
  let thickness = 7;
  let color = "#3a3a3a";
  let accentColor = COLORS.bullet;

  // Основной ствол
  ctx.fillStyle = color;
  ctx.fillRect(grip, -thickness / 2, length, thickness);
  
  // Верхняя часть ствола
  ctx.fillStyle = "#2a2a2a";
  ctx.fillRect(grip, -thickness / 2 - 2, length * 0.7, 3);

  // Рукоятка
  ctx.fillStyle = "#1a1a1a";
  ctx.fillRect(grip - 4, thickness / 2, 8, 14);

  // Прицел
  ctx.fillStyle = "#444";
  ctx.fillRect(grip + length - 6, -thickness / 2 - 4, 4, 3);

  if (state.weapon === "shotgun") {
    // Дробовик - толстый и короткий
    length = 20;
    thickness = 10;
    color = "#2d2015";
    accentColor = "#ffb347";
    
    ctx.fillStyle = color;
    ctx.fillRect(grip, -thickness / 2, length, thickness);
    // Два ствола
    ctx.fillStyle = "#1a1210";
    ctx.fillRect(grip + 2, -thickness / 2 + 1, length - 4, 3);
    ctx.fillRect(grip + 2, thickness / 2 - 4, length - 4, 3);
    
  } else if (state.weapon === "beam") {
    // Лазер - тонкий и длинный
    length = 28;
    thickness = 5;
    color = "#1a2a1a";
    accentColor = COLORS.beam;
    
    ctx.fillStyle = color;
    ctx.fillRect(grip, -thickness / 2, length, thickness);
    
    // Светящаяся полоса
    ctx.fillStyle = COLORS.beam;
    ctx.fillRect(grip + 2, -1, length - 4, 2);
    
  } else if (state.weapon === "grenade") {
    // Гранатомёт - толстый
    length = 16;
    thickness = 9;
    color = "#1a1a0a";
    accentColor = COLORS.grenade;
    
    ctx.fillStyle = color;
    ctx.fillRect(grip, -thickness / 2, length, thickness);
    
    // Граната на конце
    ctx.fillStyle = COLORS.grenade;
    ctx.beginPath();
    ctx.arc(grip + length + 5, 0, 7, 0, Math.PI * 2);
    ctx.fill();
    ctx.fillStyle = "#aa7722";
    ctx.beginPath();
    ctx.arc(grip + length + 5, -2, 3, 0, Math.PI);
    ctx.fill();
  }

  // Ствол / дуло
  ctx.fillStyle = accentColor;
  ctx.beginPath();
  ctx.arc(grip + length + 2, 0, 3, 0, Math.PI * 2);
  ctx.fill();

  // Блик
  ctx.fillStyle = "rgba(255, 255, 255, 0.15)";
  ctx.fillRect(grip + 2, -thickness / 2 + 1, length - 4, 2);

  // Muzzle flash effect
  if (state.muzzleFlash > 0) {
    const flashSize = 18 * state.muzzleFlash;
    const gradient = ctx.createRadialGradient(grip + length + 10, 0, 0, grip + length + 10, 0, flashSize);
    gradient.addColorStop(0, `rgba(255, 255, 200, ${state.muzzleFlash})`);
    gradient.addColorStop(0.3, `rgba(255, 200, 100, ${state.muzzleFlash * 0.7})`);
    gradient.addColorStop(1, "rgba(255, 150, 50, 0)");
    ctx.fillStyle = gradient;
    ctx.beginPath();
    ctx.arc(grip + length + 10, 0, flashSize, 0, Math.PI * 2);
    ctx.fill();
  }

  ctx.restore();
}

function drawHpBar(x, y, width, height, ratio, fill, text) {
  ctx.fillStyle = COLORS.hpBack;
  ctx.fillRect(x, y, width, height);
  ctx.fillStyle = fill;
  ctx.fillRect(x, y, width * Math.max(0, Math.min(1, ratio)), height);
  if (text) {
    ctx.fillStyle = "#e6f5ee";
    ctx.font = "600 9px Space Grotesk, sans-serif";
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";
    ctx.fillText(text, x + width / 2, y + height / 2 + 0.2);
  }
}

function drawPlayerHpBar() {
  const ratio = state.hp / state.maxHp;
  const barWidth = 90;
  const barHeight = 6;
  const ownCore = getOwnCorePosition();
  const x = ownCore.x - barWidth / 2;
  const y = ownCore.y - 56;
  drawHpBar(x, y, barWidth, barHeight, ratio, COLORS.hpFill, `${state.hp}/${state.maxHp}`);
}

function drawZombies() {
  for (const zombie of state.zombies) {
    const wobble = Math.sin(zombie.wobble) * 2;
    
    // Glow effect based on zombie type
    if (zombie.type === "boss") {
      ctx.shadowBlur = 25;
      ctx.shadowColor = "rgba(180, 50, 50, 0.8)";
    } else if (zombie.type === "tank") {
      ctx.shadowBlur = 15;
      ctx.shadowColor = "rgba(80, 120, 80, 0.6)";
    } else {
      ctx.shadowBlur = 10;
      ctx.shadowColor = "rgba(31, 90, 60, 0.6)";
    }
    
    // Body
    ctx.fillStyle = COLORS.zombie;
    ctx.beginPath();
    ctx.arc(zombie.x, zombie.y, zombie.r + wobble, 0, Math.PI * 2);
    ctx.fill();

    // Outline
    ctx.strokeStyle = COLORS.zombieAccent;
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.arc(zombie.x, zombie.y, zombie.r + wobble, 0, Math.PI * 2);
    ctx.stroke();

    ctx.shadowBlur = 0;
    
    // Eyes - glowing
    ctx.shadowBlur = 8;
    ctx.shadowColor = COLORS.zombieEye;
    ctx.fillStyle = COLORS.zombieEye;
    ctx.beginPath();
    ctx.arc(zombie.x - zombie.r * 0.3, zombie.y - zombie.r * 0.1, zombie.type === "tank" ? 5 : 3, 0, Math.PI * 2);
    ctx.arc(zombie.x + zombie.r * 0.25, zombie.y + zombie.r * 0.15, zombie.type === "tank" ? 4 : 2.5, 0, Math.PI * 2);
    ctx.fill();
    ctx.shadowBlur = 0;

    // Health bar
    const ratio = zombie.hp / zombie.maxHp;
    const barWidth = Math.max(26, zombie.r * 2.2);
    const barHeight = 4;
    const barX = zombie.x - barWidth / 2;
    const barY = zombie.y - zombie.r - 10;
    drawHpBar(barX, barY, barWidth, barHeight, ratio, COLORS.hpFill, `${Math.max(0, Math.ceil(zombie.hp))}/${zombie.maxHp}`);
  }
}

function drawZombiesLite() {
  for (const zombie of state.zombies) {
    const wobble = Math.sin(zombie.wobble) * 1.2;
    let fill = COLORS.zombie;
    if (zombie.type === "boss") fill = "#7a3a3a";
    else if (zombie.type === "tank") fill = "#365943";
    else if (zombie.type === "fast") fill = "#2f6646";

    ctx.fillStyle = fill;
    ctx.beginPath();
    ctx.arc(zombie.x, zombie.y, zombie.r + wobble, 0, Math.PI * 2);
    ctx.fill();

    const ratio = zombie.maxHp > 0 ? zombie.hp / zombie.maxHp : 0;
    const barWidth = Math.max(20, zombie.r * 1.8);
    const barX = zombie.x - barWidth / 2;
    const barY = zombie.y - zombie.r - 8;
    drawHpBar(barX, barY, barWidth, 3, ratio, COLORS.hpFill, "");
  }
}

function drawBullets() {
  for (const bullet of state.bullets) {
    const glow = bullet.type === "grenade" ? "rgba(248, 197, 55, 0.35)" : COLORS.bulletGlow;
    const fill = bullet.type === "grenade" ? COLORS.grenade : COLORS.bullet;
    const radius = bullet.type === "grenade" ? bullet.r * 2.2 : bullet.r * 3;
    ctx.fillStyle = glow;
    ctx.beginPath();
    ctx.arc(bullet.x, bullet.y, radius, 0, Math.PI * 2);
    ctx.fill();

    ctx.fillStyle = fill;
    ctx.beginPath();
    ctx.arc(bullet.x, bullet.y, bullet.r, 0, Math.PI * 2);
    ctx.fill();
  }
}

function drawPowerups() {
  for (const powerup of state.powerups) {
    let color = COLORS.grenade;
    let label = "У";
    if (powerup.type === "shield") {
      color = "#00a6a6";
      label = "Щ";
    } else if (powerup.type === "slow") {
      color = "#6fa36c";
      label = "З";
    }
    ctx.fillStyle = color;
    ctx.beginPath();
    ctx.arc(powerup.x, powerup.y, 10, 0, Math.PI * 2);
    ctx.fill();
    ctx.fillStyle = "#ffffff";
    ctx.font = "700 10px Space Grotesk, sans-serif";
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";
    ctx.fillText(label, powerup.x, powerup.y + 0.5);
  }
}

function drawParticles() {
  for (const particle of state.particles) {
    const t = particle.life / particle.max;
    // Glow effect
    ctx.shadowBlur = 15;
    ctx.shadowColor = 'rgba(255, 107, 53, 0.8)';
    ctx.fillStyle = `rgba(255, 150, 80, ${t})`;
    ctx.beginPath();
    ctx.arc(particle.x, particle.y, particle.size * (0.6 + t), 0, Math.PI * 2);
    ctx.fill();
    // Inner bright core
    ctx.shadowBlur = 0;
    ctx.fillStyle = `rgba(255, 220, 180, ${t * 0.8})`;
    ctx.beginPath();
    ctx.arc(particle.x, particle.y, particle.size * (0.3 + t * 0.3), 0, Math.PI * 2);
    ctx.fill();
  }
}

function drawBursts() {
  for (const burst of state.bursts) {
    const t = burst.life / burst.max;
    const radius = burst.type === "kill" ? 36 : burst.type === "blast" ? 52 : 20;
    let color = `rgba(0, 166, 166, ${0.6 * t})`;
    if (burst.type === "kill") {
      color = `rgba(255, 107, 53, ${0.7 * t})`;
    } else if (burst.type === "blast") {
      color = `rgba(248, 197, 55, ${0.75 * t})`;
    }
    // Glow
    ctx.shadowBlur = 20;
    ctx.shadowColor = color;
    ctx.strokeStyle = color;
    ctx.lineWidth = 3 * t;
    ctx.beginPath();
    ctx.arc(burst.x, burst.y, radius * (1 - t) + 8, 0, Math.PI * 2);
    ctx.stroke();
    ctx.shadowBlur = 0;
  }
}

function drawFloaters() {
  for (const floater of state.floaters) {
    const t = floater.life / floater.max;
    // Shadow for readability
    ctx.shadowBlur = 8;
    ctx.shadowColor = 'rgba(0, 0, 0, 0.8)';
    ctx.fillStyle = `rgba(255, 200, 100, ${t})`;
    ctx.font = "700 18px Space Grotesk, sans-serif";
    ctx.textAlign = "center";
    ctx.fillText(floater.value, floater.x, floater.y - 12 * (1 - t));
    ctx.shadowBlur = 0;
  }
}

function drawCrosshair() {
  const x = state.pointer.x;
  const y = state.pointer.y;
  const size = 14;
  ctx.strokeStyle = COLORS.crosshair;
  ctx.lineWidth = 1.5;
  ctx.beginPath();
  ctx.moveTo(x - size, y);
  ctx.lineTo(x - size / 2, y);
  ctx.moveTo(x + size, y);
  ctx.lineTo(x + size / 2, y);
  ctx.moveTo(x, y - size);
  ctx.lineTo(x, y - size / 2);
  ctx.moveTo(x, y + size);
  ctx.lineTo(x, y + size / 2);
  ctx.stroke();
}

function drawBeam() {
  if (!state.beamActive) {
    return;
  }
  const ownCore = getOwnCorePosition();
  const originX = ownCore.x;
  const originY = ownCore.y;
  const targetX = state.beamTarget ? state.beamTarget.x : state.pointer.x;
  const targetY = state.beamTarget ? state.beamTarget.y : state.pointer.y;
  ctx.save();
  ctx.strokeStyle = COLORS.beam;
  ctx.lineWidth = WEAPONS.beam.width;
  ctx.lineCap = "round";
  ctx.beginPath();
  ctx.moveTo(originX, originY);
  ctx.lineTo(targetX, targetY);
  ctx.stroke();

  ctx.strokeStyle = "rgba(255, 255, 255, 0.6)";
  ctx.lineWidth = WEAPONS.beam.width * 0.35;
  ctx.beginPath();
  ctx.moveTo(originX, originY);
  ctx.lineTo(targetX, targetY);
  ctx.stroke();
  ctx.restore();
}

function drawEventOverlay() {
  const config = getEventConfig();
  if (!config) {
    return;
  }

  ctx.save();
  ctx.fillStyle = config.tint;
  ctx.fillRect(0, 0, width, height);

  if (state.event.type === "fog") {
    const time = performance.now() * 0.00015;
    for (let i = 0; i < 3; i += 1) {
      const x = (Math.sin(time + i * 2.1) * 0.5 + 0.5) * width;
      const y = (Math.cos(time * 1.1 + i) * 0.5 + 0.5) * height;
      const radius = width * 0.65;
      const grad = ctx.createRadialGradient(x, y, 0, x, y, radius);
      grad.addColorStop(0, "rgba(230, 245, 238, 0.18)");
      grad.addColorStop(1, "rgba(10, 20, 14, 0)");
      ctx.fillStyle = grad;
      ctx.fillRect(0, 0, width, height);
    }
  }

  ctx.restore();
}

function getVisualQualityScale() {
  const scale = width <= 900 ? 0.78 : 1;
  return clamp(scale, 0.35, 1);
}

function drawAtmosphereOverlay(now = performance.now()) {
  const modifiers = getAtmosphereModifiers();
  const quality = getVisualQualityScale();

  ctx.save();

  if (modifiers.timeTint) {
    ctx.fillStyle = modifiers.timeTint;
    ctx.fillRect(0, 0, width, height);
  }
  if (modifiers.weatherTint) {
    ctx.fillStyle = modifiers.weatherTint;
    ctx.fillRect(0, 0, width, height);
  }

  const darkness = clamp((1 - modifiers.visibility) * 0.52 + modifiers.weatherFog * 0.1, 0, 0.48);
  if (darkness > 0.01) {
    ctx.fillStyle = `rgba(2, 6, 10, ${darkness})`;
    ctx.fillRect(0, 0, width, height);
  }

  if (modifiers.weatherRain > 0.01) {
    const dropCount = Math.max(16, Math.round((46 + modifiers.weatherRain * 64) * quality));
    const slant = 26 + modifiers.weatherRain * 12;
    const dropLen = 12 + modifiers.weatherRain * 16;
    ctx.strokeStyle = `rgba(165, 208, 245, ${0.16 + modifiers.weatherRain * 0.1})`;
    ctx.lineWidth = 1.1;
    ctx.beginPath();
    for (let i = 0; i < dropCount; i += 1) {
      const x = ((i * 67.13 + now * (0.22 + modifiers.weatherRain * 0.18)) % (width + 120)) - 60;
      const y = ((i * 109.41 + now * (0.58 + modifiers.weatherRain * 0.5)) % (height + 180)) - 90;
      ctx.moveTo(x, y);
      ctx.lineTo(x - slant * 0.45, y + dropLen);
    }
    ctx.stroke();
  }

  if (modifiers.weatherFog > 0.01) {
    const fogStrength = clamp(0.1 + modifiers.weatherFog * 0.25, 0.06, 0.34);
    const drift = now * 0.00016;
    const fogLayers = Math.max(2, Math.round(4 * quality));
    for (let i = 0; i < fogLayers; i += 1) {
      const fx = (Math.sin(drift + i * 1.3) * 0.5 + 0.5) * width;
      const fy = (Math.cos(drift * 1.15 + i * 0.95) * 0.5 + 0.5) * height;
      const radius = width * (0.42 + i * 0.08);
      const fog = ctx.createRadialGradient(fx, fy, radius * 0.15, fx, fy, radius);
      fog.addColorStop(0, `rgba(210, 225, 228, ${fogStrength * 0.7})`);
      fog.addColorStop(1, "rgba(210, 225, 228, 0)");
      ctx.fillStyle = fog;
      ctx.fillRect(0, 0, width, height);
    }
  }

  if (state.atmosphere.lightning > 0) {
    const alpha = clamp(state.atmosphere.lightning * 0.38, 0, 0.38);
    ctx.fillStyle = `rgba(225, 242, 255, ${alpha})`;
    ctx.fillRect(0, 0, width, height);
  }

  ctx.restore();
}

function render() {
  ctx.clearRect(0, 0, width, height);

  ctx.save();
  if (state.shake > 0) {
    const magnitude = state.shake;
    ctx.translate((Math.random() - 0.5) * magnitude, (Math.random() - 0.5) * magnitude);
    state.shake *= 0.86;
  }

  const ownCore = getOwnCorePosition();
  const allyCore = getAllyCorePosition();
  if (allyCore) {
    drawCoopCoreLink(ownCore, allyCore);
    drawAllyCore(allyCore.x, allyCore.y);
  }

  drawCore(ownCore.x, ownCore.y);
  drawRemoteWeapon();
  drawWeapon();
  drawPlayerHpBar();
  drawRemoteTracers();
  drawBursts();
  drawPowerups();
  drawRemoteBeam();
  drawBeam();
  drawParticles();
  drawZombies();
  drawBullets();
  drawFloaters();
  drawEventOverlay();
  drawAtmosphereOverlay(lastTime);

  if (state.pointer.active) {
    drawCrosshair();
  }

  ctx.restore();

  if (state.flash > 0.01) {
    ctx.fillStyle = `rgba(255, 107, 53, ${state.flash})`;
    ctx.fillRect(0, 0, width, height);
    state.flash *= 0.88;
  }
}

function tick(now) {
  const dt = Math.min(0.033, (now - lastTime) / 1000);
  lastTime = now;

  if (state.running) {
    update(dt, now);
  } else if (coopState.active && coopState.roomStatus === "running") {
    syncCoopState(now);
  }

  render();
  requestAnimationFrame(tick);
}

function getPointerPosition(event) {
  const rect = canvas.getBoundingClientRect();
  return {
    x: event.clientX - rect.left,
    y: event.clientY - rect.top,
  };
}

function setPointerFromEvent(event) {
  const pos = getPointerPosition(event);
  state.pointer.x = pos.x;
  state.pointer.y = pos.y;
  return pos;
}

canvas.addEventListener("pointerdown", (event) => {
  const pos = setPointerFromEvent(event);
  state.pointer.active = true;
  state.pointer.down = true;
  unlockAudio();
  shoot(pos.x, pos.y, performance.now());
  if (coopState.active) {
    syncCoopState(performance.now(), true);
  }
});

canvas.addEventListener("pointermove", (event) => {
  setPointerFromEvent(event);
  state.pointer.active = true;
  if (coopState.active && state.status === "running") {
    const now = performance.now();
    if (now - coopState.lastInputSyncAt >= getCoopSyncIntervalMs(true)) {
      coopState.lastInputSyncAt = now;
      syncCoopState(now, true);
    }
  }
});

canvas.addEventListener("pointerleave", () => {
  state.pointer.active = false;
  state.pointer.down = false;
  if (coopState.active) {
    syncCoopState(performance.now(), true);
  }
});

canvas.addEventListener("pointerup", () => {
  state.pointer.down = false;
  if (coopState.active) {
    syncCoopState(performance.now(), true);
  }
});

canvas.addEventListener("pointercancel", () => {
  state.pointer.down = false;
  if (coopState.active) {
    syncCoopState(performance.now(), true);
  }
});

playBtn.addEventListener("click", () => {
  if (state.status === "paused") {
    startRun({ reset: false });
  } else {
    startRun({ reset: true });
  }
});

startBtn.addEventListener("click", () => {
  startRun({ reset: true });
  document.getElementById("gameSection").scrollIntoView({ behavior: "smooth" });
});

pauseBtn.addEventListener("click", () => {
  if (state.status === "paused") {
    startRun({ reset: false });
  } else {
    pauseGame();
  }
});

restartBtn.addEventListener("click", () => {
  startRun({ reset: true });
});

soundBtn.addEventListener("click", () => {
  unlockAudio();
  setAudioEnabled(!audio.enabled);
});

weaponButtons.forEach((button) => {
  button.addEventListener("click", () => {
    setWeapon(button.dataset.weapon);
  });
});

document.addEventListener("keydown", (event) => {
  if (event.key === "1") {
    setWeapon("blaster");
  } else if (event.key === "2") {
    setWeapon("shotgun");
  } else if (event.key === "3") {
    setWeapon("beam");
  } else if (event.key === "4") {
    setWeapon("grenade");
  } else if (event.key === "p" || event.key === "P") {
    if (state.status === "paused") {
      startRun({ reset: false });
    } else {
      pauseGame();
    }
  }
});

let lastTouchEnd = 0;
document.addEventListener(
  "touchend",
  (event) => {
    const now = Date.now();
    if (now - lastTouchEnd <= 300) {
      event.preventDefault();
    }
    lastTouchEnd = now;
  },
  { passive: false }
);

document.addEventListener(
  "gesturestart",
  (event) => {
    event.preventDefault();
  },
  { passive: false }
);

const primeAudio = () => {
  unlockAudio();
  updateAmbientTrack();
};

document.addEventListener("pointerdown", primeAudio, { once: true });
document.addEventListener("touchstart", primeAudio, { once: true, passive: true });
document.addEventListener("click", primeAudio, { once: true });
document.addEventListener("visibilitychange", updateAmbientTrack);
window.addEventListener("focus", updateAmbientTrack);
window.addEventListener("blur", updateAmbientTrack);
window.addEventListener("pagehide", updateAmbientTrack);
window.addEventListener("pageshow", updateAmbientTrack);

scrollBtn.addEventListener("click", () => {
  document.getElementById("rules").scrollIntoView({ behavior: "smooth" });
});

renderEncyclopediaZombiePreviews();
updateAtmosphereHud(true);
resizeCanvas();
updateHud();
updateSoundButton();
updatePauseButton();
updateWeaponButtons();
updateStatusLine();
initEmbers();
updateAmbientTrack();
requestAnimationFrame(tick);
window.addEventListener("resize", () => {
  resizeCanvas();
  renderEncyclopediaZombiePreviews();
});
