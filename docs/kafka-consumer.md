# Consumer TripPublished

Сервис читает topic `trip.events` в consumer group `notification-service`.
Ожидается плоский JSON, который отправляет producer ShareTrip:

```json
{
  "event_id": "78910abc-1234-4567-89ab-1234567890ab",
  "event_type": "TripPublished",
  "trip_id": "f9b711e3-8f45-4dc3-8cab-07623f5de6e5",
  "client_id": "a1b2c3d4-1111-4111-8111-123456789abc",
  "company_id": "company-acme",
  "occurred_at": "2026-10-22T20:00:00Z"
}
```

## Обработка

1. `FetchMessage` читает сообщение без автоматического подтверждения offset.
2. На транспортном уровне проверяются JSON, тип события, UUID и обязательные поля.
3. Другой непустой event_type пропускается с подтверждением offset,
   без обращения в БД. Обрабатывается только `TripPublished`.
4. Сервис открывает транзакцию и вставляет `event_id` в `processed_events`
   через `ON CONFLICT DO NOTHING`.
5. Для нового события создаётся уведомление: `recipient_id=client_id`,
   `type=trip_published`, `status=created`. Payload содержит trip_id, company_id,
   occurred_at. Внешняя доставка email/SMS/push не выполняется.
6. Отметка события и уведомление фиксируются одним COMMIT.
7. Только после этого вызывается `CommitMessages` с `CommitInterval=0`.

Повтор того же event_id не создаёт уведомление, но его Kafka offset подтверждается.
Уникальный ключ processed_events защищает также от одновременной обработки дублей.
Если создание уведомления не удалось, откатывается и отметка обработки события.

## Ошибки и перезапуск

При ошибке JSON, валидации, БД или commit offset consumer завершает работу с ошибкой.
Приложение останавливает HTTP-сервер и закрывает reader. Offset проблемного события
не подтверждается; следующие сообщения не обрабатываются и не подтверждаются поверх него.
После перезапуска событие доступно для повторной доставки. Для ошибки данных
сначала нужно устранить причину; автоматического DLQ или пропуска некорректных сообщений нет.

Если БД уже закоммичена, а Kafka offset подтвердить не удалось, повторная доставка
безопасна благодаря processed_events. Это защита от повторного создания уведомления,
а не атомарная транзакция между Kafka и PostgreSQL.

Группа без сохранённого offset начинает чтение с `FirstOffset`. Существующая группа
продолжает с сохранённой позиции. Смена KAFKA_GROUP_ID создаёт независимую позицию
чтения, но сохранённые event_id по-прежнему защищают БД от дублей.

## Запуск

Сначала запустить Kafka и kafka-init в проекте ShareTrip, затем в этом проекте:

```text
make up
make migrate-up
make run
```

Новая миграция создаёт `processed_events`; её нужно применить до запуска consumer.
Настройки окружения:

```text
KAFKA_BROKERS=localhost:9092
KAFKA_TRIP_TOPIC=trip.events
KAFKA_GROUP_ID=notification-service
HTTP_PORT=4001
```

KAFKA_BROKERS принимает список через запятую. Для подключения из Docker-сети
ShareTrip используется `kafka:29092`; брокер должен быть доступен из сети приложения.
API уведомлений продолжает работать вместе с consumer. Обработка одного события
ограничена 10 секундами. При сигнале остановки отменяется контекст consumer,
HTTP-запросам даётся до 5 секунд на завершение, после чего reader и пул закрываются.

В Kafka UI на http://localhost:8085 после запуска сервиса доступны topic trip.events
и группа notification-service. После новой публикации поездки сообщение должно
быть обработано, offset группы продвинут, а в notifications появиться запись.

Диаграмма: [PlantUML](diagrams/publish-trip-notification.puml).

## Проверки

```text
make test
make lint
make build
```

Unit-тесты consumer проверяют фильтрацию, валидацию, порядок handler → commit и
отсутствие commit при ошибках. Интеграционные тесты PostgreSQL проверяют payload,
повторные и конкурентные доставки, откат processed_events и возможность повтора.

Для сквозного теста с реальной Kafka и временным PostgreSQL, PowerShell:

```powershell
$env:KAFKA_TEST_BROKERS = 'localhost:9092'
go test ./internal/api_test -run TestConsumer_KafkaAndPostgres -count=1 -v
```

Тест отправляет два одинаковых события и один другой тип, проверяет одну запись
уведомления, одну отметку processed_events и committed offset 3. Временные topic
и consumer group удаляются после проверки. Без KAFKA_TEST_BROKERS этот тест пропускается.

Ограничение producer ShareTrip остаётся прежним: доставка из его outbox пока
не автоматизирована. Consumer обеспечивает идемпотентность только для событий,
которые действительно попали в Kafka.
