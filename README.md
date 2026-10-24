# Share Trip Notification

Сервис для создания и получения уведомлений Share Trip.

## Требования

- Go 1.26+
- Docker
- Make

## Запуск

Установить зависимости и инструменты:

```bash
make deps
```

Запустить PostgreSQL и применить миграции:

```bash
make up
make migrate-up
```

Запустить сервис:

```bash
make run
```

По умолчанию API доступно по адресу `http://localhost:4001`.

## API

### Создать уведомление

```http
POST /api/notification/
Content-Type: application/json

{
  "recipient_id": "user-123",
  "type": "trip_created",
  "payload": {
    "trip_id": "550e8400-e29b-41d4-a716-446655440000"
  }
}
```

### Получить уведомление

```http
GET /api/notification/{notificationId}
```

## Проверки

```bash
make test
make lint
```

Интеграционные тесты самостоятельно запускают временный PostgreSQL-контейнер и применяют миграции.

## Остановка PostgreSQL

```bash
make down
```

## Конфигурация и Kubernetes

Обязательные параметры и настройки запуска описаны в репозитории **share-trip**, в `k8s/README.md`.
Манифесты этого сервиса находятся там же: `k8s/notification-config.yaml`,
`k8s/notification-secret.example.yaml`, `k8s/notification-deployment.yaml`.
Для локального запуска используется `.env.example`; реальные секреты в Git не сохраняются.
