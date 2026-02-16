# Google-вход и облачное сохранение (самый простой бесплатный вариант)

## 1. Создай Firebase проект
1. Открой [Firebase Console](https://console.firebase.google.com/).
2. Нажми `Add project`.
3. Назови проект, Analytics можно выключить.

## 2. Подключи Web App
1. В проекте нажми `</>` (Web app).
2. Назови приложение (например `gamesite`).
3. Скопируй `firebaseConfig`.
4. Вставь значения в файл `/Users/bogdanbogdanov/Desktop/GameSite/firebase-config.js`.

## 3. Включи Google-вход
1. Firebase -> `Authentication` -> `Get started`.
2. `Sign-in method` -> `Google` -> `Enable`.
3. Сохрани.

## 4. Включи Firestore
1. Firebase -> `Firestore Database` -> `Create database`.
2. Выбери `Production mode`.
3. Регион можно любой ближайший.

## 5. Поставь правила Firestore
В `Firestore Database -> Rules` вставь:

```txt
rules_version = '2';
service cloud.firestore {
  match /databases/{database}/documents {
    match /zombiesurge_users_v2/{userId} {
      allow read: if request.auth != null;
      allow create, update: if request.auth != null && request.auth.uid == userId;
      allow delete: if false;
    }
  }
}
```

Нажми `Publish`.

## 6. Проверь
1. Открой игру.
2. Нажми `Войти через Google`.
3. Сыграй и сделай покупку/прокачку.
4. Перезагрузи страницу: золото, апгрейды и статистика должны остаться.

## Стоимость
- Это работает на бесплатном тарифе Firebase `Spark`.
- Для обычной небольшой игры этого обычно хватает.
