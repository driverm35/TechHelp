# SupportBot (Minimal)

Production-ready минималистичный бот техподдержки: Aiogram 3 + FastAPI webhook + Redis FSM + Postgres (SQLAlchemy 2).

## Быстрый старт

```bash
cp .env.example .env
# отредактируй токен/URL/идентификаторы

docker compose up -d --build
# Бот поднимет FastAPI на :8080, настроенный webhook: 2025-11-12
```

### Caddy (reverse proxy)
```caddyfile
support.example.com {
    encode gzip zstd
    reverse_proxy bot:8080
}
```

## Структура
- `app/config/settings.py` — конфиг через pydantic-settings
- `app/db/*` — engine, модели, миграции (alembic)
- `app/cache/redis.py` — Redis клиент + утилиты
- `app/services/*` — бизнес-логика тикетов/моста/опросов/таблиц
- `app/bot/*` — инициализация aiogram, роутеры и фильтры
- `app/web/server.py` — FastAPI приложение с `/webhook` и `/healthz`

## Миграции
```bash
docker compose exec bot alembic upgrade head
```
При первом старте `alembic upgrade head` выполняется автоматически из `app/bot/main.py`.

## Важно
- Включи темы в главной и тех-группах.
- Пропиши корректные `MAIN_GROUP_ID` и `TECH_GROUPS_MAPPING`.
