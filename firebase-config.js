// Firebase config для Google-входа.
// Заполни значениями из Firebase Console:
// Project settings -> General -> Your apps -> Web app -> Firebase SDK snippet -> Config.
window.FIREBASE_CONFIG = {
  apiKey: "AIzaSyAfC5Z8xU-6QTAjimNHAFmIBuJVsqvx_VE",
  authDomain: "zombiesurge.firebaseapp.com",
  projectId: "zombiesurge",
  databaseURL: "https://zombiesurge-default-rtdb.firebaseio.com",
  storageBucket: "zombiesurge.firebasestorage.app",
  messagingSenderId: "572319578623",
  appId: "1:572319578623:web:268379f5461094db8d4fca",
};

// URL кооп-WebSocket сервера (для прода укажи wss://...).
// Локально по умолчанию: ws://localhost:8787
// Пример:
// window.COOP_WS_URL = "wss://your-name.onrender.com";
if (!window.COOP_WS_URL) {
  const isLocal = /^(localhost|127\.0\.0\.1)$/i.test(window.location.hostname || "");
  window.COOP_WS_URL = isLocal ? "ws://localhost:8787" : "";
}
