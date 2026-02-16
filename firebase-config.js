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

window.COOP_WS_URL = /^(localhost|127\.0\.0\.1)$/i.test(window.location.hostname || "")
  ? "ws://localhost:8787"
  : "wss://zombiesurge-ws.onrender.com";
