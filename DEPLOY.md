# Деплой игры (Firebase Hosting)

## Один раз
```bash
npm i -g firebase-tools
firebase login
```

Если в Safari снова ошибка с `localhost:9005`, используй:
```bash
firebase login --no-localhost
```

## Каждый деплой
```bash
cd /Users/bogdanbogdanov/Desktop/GameSite
firebase deploy --only hosting,database
```

После деплоя Firebase покажет ссылку вида:
- `https://zombiesurge.web.app`
- `https://zombiesurge.firebaseapp.com`

## Кастомный домен (опционально)
1. Firebase Console -> Hosting -> Add custom domain.
2. Добавь DNS записи у регистратора.
3. После подключения домена добавь его в:
Authentication -> Settings -> Authorized domains.

## Кооп без лагов (WebSocket)
Для нового кооп-режима нужен отдельный WS-сервер.
Пошагово: `WS_COOP_SETUP.md`
