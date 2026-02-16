# WebSocket кооп для Zombie Surge

## 1) Локальный запуск (проверка без деплоя)

```bash
cd /Users/bogdanbogdanov/Desktop/GameSite/ws-server
npm install
npm start
```

Сервер поднимется на `ws://localhost:8787`.

В `firebase-config.js` уже стоит авто-настройка:
- локально используется `ws://localhost:8787`
- на проде нужно вручную задать `wss://...`

Проверка health:
- [http://localhost:8787/health](http://localhost:8787/health)

## 2) Прод (бесплатно и просто)

Самый простой путь: Render (Free Web Service).

1. Залей проект в GitHub.
2. На Render создай `Web Service`.
3. Укажи:
   - Root Directory: `ws-server`
   - Build Command: `npm install`
   - Start Command: `npm start`
4. После запуска получишь URL вида:
   - `https://your-name.onrender.com`
5. В `firebase-config.js` укажи:

```js
window.COOP_WS_URL = "wss://your-name.onrender.com";
```

6. Задеплой сайт:

```bash
cd /Users/bogdanbogdanov/Desktop/GameSite
firebase deploy --only hosting
```

## 3) Важно

- Пока `window.COOP_WS_URL` пустой, игра использует старый RTDB-транспорт.
- Для стабильного коопа на телефонах и ПК нужен именно `wss://` URL.
