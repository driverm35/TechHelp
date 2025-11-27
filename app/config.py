# app/config.py
from __future__ import annotations
import json

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict



class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ENV mode
    app_env: str = Field("prod", alias="APP_ENV")  # dev | prod
    use_polling: bool = Field(False, alias="USE_POLLING")
    use_sqlite: bool = Field(False, alias="USE_SQLITE")
    disable_redis: bool = Field(False, alias="DISABLE_REDIS")

    # Logging
    log_level: str = Field("INFO", alias="LOG_LEVEL")
    log_file: str = Field("logs/app.log", alias="LOG_FILE")
    timezone: str = Field("Europe/Moscow", alias="TIMEZONE")

    # Telegram
    bot_token: str = Field(alias="BOT_TOKEN")
    webhook_path: str = Field("/webhook", alias="WEBHOOK_PATH")
    webhook_url: str = Field(..., alias="WEBHOOK_URL")
    webhook_secret_token: str = Field(..., alias="WEBHOOK_SECRET_TOKEN")

    # Chats
    main_group_id: int = Field(..., alias="MAIN_GROUP_ID")

    # DB (Postgres + SQLite dev)
    pg_host: str = Field("postgres", alias="POSTGRES_HOST")
    pg_port: int = Field(5432, alias="POSTGRES_PORT")
    pg_db: str = Field("supportdb", alias="POSTGRES_DB")
    pg_user: str = Field("support", alias="POSTGRES_USER")
    pg_password: str = Field("support", alias="POSTGRES_PASSWORD")
    sqlite_path: str = Field("sqlite+aiosqlite:///./dev.db", alias="SQLITE_PATH")

    # Redis
    redis_host: str = Field("redis", alias="REDIS_HOST")
    redis_port: int = Field(6379, alias="REDIS_PORT")
    redis_url: str = Field(
        "redis://localhost:6379/0",
        alias="REDIS_URL"
    )
    redis_db: int = Field(0, alias="REDIS_DB")

    # 🔹 ИСПРАВЛЕНО: только строковое поле, парсим в model_validator
    admin_ids_raw: str = Field("", alias="ADMIN_IDS")

    # Legacy (можно оставить)
    tech_groups_mapping_raw: str = Field("{}", alias="TECH_GROUPS_MAPPING")

    google_sheets_enabled: bool = Field(False, alias="GOOGLE_SHEETS_ENABLED")
    google_sheets_json_path: str | None = Field(None, alias="GOOGLE_SERVICE_ACCOUNT_JSON_PATH")
    gspread_spreadsheet: str = Field("", alias="GSPREAD_SPREADSHEET")

    auto_delete_service_messages: bool = Field(
        True,
        alias="AUTO_DELETE_SERVICE_MESSAGES"
    )
    delete_pinned_messages: bool = Field(True, alias="DELETE_PINNED_MESSAGES")
    delete_topic_changes: bool = Field(True, alias="DELETE_TOPIC_CHANGES")
    delete_new_chat_members: bool = Field(True, alias="DELETE_NEW_CHAT_MEMBERS")
    delete_left_chat_member: bool = Field(True, alias="DELETE_LEFT_CHAT_MEMBER")
    delete_chat_title_changes: bool = Field(True, alias="DELETE_CHAT_TITLE_CHANGES")
    delete_chat_photo_changes: bool = Field(True, alias="DELETE_CHAT_PHOTO_CHANGES")

    # 🔹 Внутренние кешированные поля (не из .env)
    _admin_ids: list[int] | None = None
    _tech_groups_mapping: dict[str, int] | None = None

    @model_validator(mode="after")
    def parse_complex_fields(self):
        """Парсим сложные поля после создания модели."""

        # 🔹 Парсинг admin_ids
        raw = self.admin_ids_raw.strip()
        if raw:
            # Вариант "1,2,3"
            if "," in raw:
                try:
                    self._admin_ids = [int(p.strip()) for p in raw.split(",") if p.strip()]
                except Exception as e:
                    print(f"⚠️ Ошибка парсинга ADMIN_IDS (запятая): {e}")
                    self._admin_ids = []
            # Вариант одиночного числа "769068893"
            elif raw.isdigit():
                self._admin_ids = [int(raw)]
            # Вариант JSON "[1, 2, 3]"
            else:
                try:
                    arr = json.loads(raw)
                    if isinstance(arr, list):
                        self._admin_ids = [int(x) for x in arr]
                    else:
                        print(f"⚠️ ADMIN_IDS не является массивом: {raw}")
                        self._admin_ids = []
                except Exception as e:
                    print(f"⚠️ Ошибка парсинга ADMIN_IDS (JSON): {e}")
                    self._admin_ids = []
        else:
            self._admin_ids = []

        # 🔹 Парсинг tech_groups_mapping
        raw_mapping = self.tech_groups_mapping_raw.strip()
        if raw_mapping:
            try:
                self._tech_groups_mapping = json.loads(raw_mapping)
            except Exception:
                try:
                    self._tech_groups_mapping = json.loads(raw_mapping.replace("'", '"'))
                except Exception as e:
                    print(f"⚠️ Ошибка парсинга TECH_GROUPS_MAPPING: {e}")
                    self._tech_groups_mapping = {}
        else:
            self._tech_groups_mapping = {}

        return self

    def get_admin_ids(self) -> list[int]:
        """Получить список ID админов."""
        if self._admin_ids is None:
            return []
        return self._admin_ids

    def is_admin(self, user_id: int) -> bool:
        """Проверка, является ли пользователь админом."""
        return user_id in self.get_admin_ids()

    @property
    def tech_groups_mapping(self) -> dict[str, int]:
        """Получить маппинг техников."""
        if self._tech_groups_mapping is None:
            return {}
        return self._tech_groups_mapping

    @property
    def db_dsn(self) -> str:
        """Получить DSN для подключения к БД."""
        if self.use_sqlite or self.app_env.lower() == "dev":
            return self.sqlite_path
        return f"postgresql+asyncpg://{self.pg_user}:{self.pg_password}@{self.pg_host}:{self.pg_port}/{self.pg_db}"

    @property
    def is_dev(self) -> bool:
        """Проверка режима разработки."""
        return self.app_env.lower() == "dev"

    @property
    def is_prod(self) -> bool:
        """Проверка production режима."""
        return self.app_env.lower() == "prod"

    @property
    def use_redis(self) -> bool:
        """Использовать ли Redis (только в prod)."""
        return self.is_prod


settings = Settings()