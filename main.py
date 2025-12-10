from __future__ import annotations
import os
import boto3
import asyncio
import contextlib
import logging
import signal
import sys
import uvicorn

from app.bot.bot import setup_bot, shutdown_bot  # ✅ Добавляем shutdown_bot
from app.config import settings
from app.utils.cache import cache
from app.utils.startup_timeline import StartupTimeline
from app.utils.timezone import TimezoneAwareFormatter
from app.web.server import create_app
from pathlib import Path

from app.db.database import init_db
from app.workers.mirror_worker import mirror_worker  # ✅ Правильный импорт


async def check_s3_connection(logger: logging.Logger) -> None:
    """Проверка доступности S3-бакета при старте приложения."""
    endpoint = os.getenv('S3_ENDPOINT_URL')
    bucket = os.getenv('S3_BUCKET_NAME')
    region = os.getenv('S3_REGION', 'ru-1')
    access_key = os.getenv('S3_ACCESS_KEY')
    secret_key = os.getenv('S3_SECRET_KEY')

    if not all([endpoint, bucket, access_key, secret_key]):
        logger.warning("S3 не настроен (нет части переменных окружения), пропускаем проверку")
        return

    def _sync_check():
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint,
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        s3_client.head_bucket(Bucket=bucket)
        test_key = "test/supportbot_startup_check.txt"
        s3_client.put_object(
            Bucket=bucket,
            Key=test_key,
            Body=b"SupportBot S3 startup test"
        )
        s3_client.delete_object(Bucket=bucket, Key=test_key)

    try:
        await asyncio.to_thread(_sync_check)
    except Exception as e:
        if settings.app_env.lower() in ("prod", "production"):
            logger.error("❌ Проверка S3 не пройдена, останавливаем запуск: %s", e)
            raise
        else:
            logger.warning("⚠️ Проверка S3 не пройдена (DEV/TEST режим): %s", e)


class GracefulExit:
    def __init__(self):
        self.exit = False
    
    def exit_gracefully(self, signum, frame):
        logging.getLogger(__name__).info(f"Получен сигнал {signum}. Корректное завершение работы...")
        self.exit = True


async def main():
    # === ЛОГИ ===
    log_path = Path(settings.log_file)
    log_dir = log_path.parent
    log_dir.mkdir(parents=True, exist_ok=True)

    formatter = TimezoneAwareFormatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        timezone_name=settings.timezone,
    )

    file_handler = logging.FileHandler(settings.log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        handlers=[file_handler, stream_handler],
    )
    
    # Установим более высокий уровень логирования для "мусорных" логов
    logging.getLogger("aiohttp.access").setLevel(logging.ERROR)
    logging.getLogger("aiohttp.client").setLevel(logging.WARNING)
    logging.getLogger("aiohttp.internal").setLevel(logging.WARNING)
    logging.getLogger("app.external.remnawave_api").setLevel(logging.WARNING)
    logging.getLogger("aiogram").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.ERROR)
    logging.getLogger("uvicorn.error").setLevel(logging.WARNING)

    logger = logging.getLogger(__name__)

    timeline = StartupTimeline(logger, "SupportBot")
    timeline.log_banner([
        ("Уровень логирования", settings.log_level),
        ("APP_ENV", settings.app_env),
        ("Режим БД", settings.db_dsn),
        ("Режим работы", "polling" if (settings.use_polling or settings.is_dev) else "webhook"),
        ("Вебхук URL", settings.webhook_url if settings.webhook_url else "не установлен"),
        ("ADMIN IDS", settings.get_admin_ids())
    ])

    killer = GracefulExit()
    signal.signal(signal.SIGINT, killer.exit_gracefully)
    signal.signal(signal.SIGTERM, killer.exit_gracefully)

    polling_task = None
    worker_task = None  # ✅ Добавляем
    web_server = None
    bot = None
    dp = None

    try:
        async with timeline.stage(
            "Инициализация базы данных", "🗄️", success_message="База данных готова"
        ):
            await init_db()

        async with timeline.stage(
            "Проверка S3 backup-хранилища", "💾", success_message="S3 доступен"
        ):
            await check_s3_connection(logger)
            logger.info(
                "S3 endpoint=%s bucket=%s region=%s",
                os.getenv('S3_ENDPOINT_URL'),
                os.getenv('S3_BUCKET_NAME'),
                os.getenv('S3_REGION', 'ru-1'),
            )

        async with timeline.stage("Настройка бота", "🤖", success_message="Бот настроен") as stage:
            bot, dp = await setup_bot()
            stage.log("Кеш, FSM и Redis Streams подготовлены")
        
        # ✅ ИСПРАВЛЕНО: Запускаем воркер только в режиме polling
        if settings.use_polling or settings.is_dev:
            async with timeline.stage("Запуск Mirror Worker", "👷", success_message="Worker готов") as stage:
                worker_task = asyncio.create_task(mirror_worker())
                stage.log("Mirror worker запущен в фоне")

        # DEV: polling
        if settings.use_polling or settings.is_dev:
            async with timeline.stage("Запуск polling", "🔌", success_message="Aiogram polling запущен"):
                try:
                    await bot.delete_webhook(drop_pending_updates=True)
                except Exception as e:
                    logger.warning("Не удалось снять webhook: %s", e)
                polling_task = asyncio.create_task(dp.start_polling(bot, skip_updates=True))
        else:
            async with timeline.stage("Запуск HTTP/ASGI (webhook)", "🌐", success_message="Webhook активен"):
                app = create_app(dp, bot)
                await bot.set_webhook(
                    url=settings.webhook_url,
                    secret_token=settings.webhook_secret_token,
                    drop_pending_updates=True,
                    allowed_updates=None
                )
                config = uvicorn.Config(app, host="0.0.0.0", port=8080, log_level="info")
                web_server = uvicorn.Server(config)
                logger.info(f"Webhook установлен: {settings.webhook_url}")
                await web_server.serve()

        timeline.log_summary()

        # основной цикл для graceful-stop в polling
        if polling_task:
            while not killer.exit:
                if polling_task.done():
                    exc = polling_task.exception()
                    if exc:
                        logger.error("Polling завершился с ошибкой: %s", exc)
                        break
                
                # ✅ Проверяем воркер
                if worker_task and worker_task.done():
                    exc = worker_task.exception()
                    if exc:
                        logger.error("Worker завершился с ошибкой: %s", exc)
                        break
                
                await asyncio.sleep(1)

    except Exception as e:
        logger.error("❌ Критическая ошибка при запуске: %s", e, exc_info=True)
        raise
    finally:
        logger.info("🛑 Завершение...")
        try:
            # ✅ Останавливаем воркер
            if worker_task and not worker_task.done():
                logger.info("Остановка mirror worker...")
                worker_task.cancel()
                try:
                    await worker_task
                except asyncio.CancelledError:
                    pass

            # Останавливаем polling
            if polling_task and not polling_task.done():
                logger.info("Остановка polling...")
                polling_task.cancel()
                try:
                    await polling_task
                except asyncio.CancelledError:
                    pass

            # ✅ Вызываем shutdown_bot
            await shutdown_bot()

        finally:
            if bot:
                if not (settings.use_polling or settings.is_dev):
                    logger.info("Снятие webhook...")
                    with contextlib.suppress(Exception):
                        await bot.delete_webhook(drop_pending_updates=False)
                with contextlib.suppress(Exception):
                    await bot.session.close()
                logger.info("✅ Сессия бота закрыта")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Бот остановлен пользователем")
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")