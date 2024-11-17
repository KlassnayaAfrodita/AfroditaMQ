![Логотип AfroditaMQ](logo.png)

# Message Broker

Проект предоставляет простой API для работы с брокером сообщений и тестируемый клиентский интерфейс для взаимодействия с ним.

## Преимущества

- **Простота использования**: API брокера и клиента легко интегрировать в существующие проекты.
- **Поддержка приоритетов и TTL сообщений**: Сообщения могут быть отправлены с приоритетом и временем жизни.
- **Модульность**: Брокер разделён на несколько компонентов, что облегчает поддержку и расширение.

## Описание

Проект состоит из двух основных частей:

1. **Брокер сообщений**: Сервис, который управляет топиками, подписками и сообщениями.
2. **Клиент**: Интерфейс для отправки сообщений на брокер и подписки на топики.

### Важные компоненты:

- **Broker**: Управляет топиками и подписчиками, а также получает и отправляет сообщения.
- **Publisher**: Публикует сообщения на сервер.
- **Subscriber**: Подписывается на топики и получает сообщения.

## Как запустить

### 1. Запуск брокера:

Для запуска брокера, просто выполните команду:

```bash
go run MessageBroker/main.go
