from __future__ import annotations
import asyncio
import contextlib
import logging
import signal
import sys
import uvicorn

from app.bot.bot import setup_bot
from app.config import settings
from app.utils.cache import cache
from app.utils.startup_timeline import StartupTimeline
from app.utils.timezone import TimezoneAwareFormatter
from app.web.server import create_app

from app.db.database import init_db

class GracefulExit:
    def __init__(self):
        self.exit = False
    def exit_gracefully(self, signum, frame):
        logging.getLogger(__name__).info(f"Получен сигнал {signum}. Корректное завершение работы...")
        self.exit = True


async def main():
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
        ("ADMIN IDS", settings.get_admin_ids())
    ])

    killer = GracefulExit()
    signal.signal(signal.SIGINT, killer.exit_gracefully)
    signal.signal(signal.SIGTERM, killer.exit_gracefully)

    polling_task = None
    web_server = None
    bot = None
    dp = None

    try:
        async with timeline.stage(
            "Инициализация базы данных", "🗄️", success_message="База данных готова"
        ):
            await init_db()

        async with timeline.stage("Настройка бота", "🤖", success_message="Бот настроен") as stage:
            bot, dp = await setup_bot()
            stage.log("Кеш и FSM подготовлены")

        # DEV: polling
        if settings.use_polling or settings.is_dev:
            async with timeline.stage("Запуск polling", "🔌", success_message="Aiogram polling запущен"):
                # снимаем вебхук на всякий
                try:
                    await bot.delete_webhook(drop_pending_updates=True)
                except Exception as e:
                    logger.warning("Не удалось снять webhook: %s", e)
                polling_task = asyncio.create_task(dp.start_polling(bot, skip_updates=True))
                stage.log("skip_updates=True")
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
                stage.log(f"Webhook установлен: {settings.webhook_url}")
                # в режиме webhook блокируем поток сервером uvicorn
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
                await asyncio.sleep(1)

    except Exception as e:
        logger.error("❌ Критическая ошибка при запуске: %s", e)
        raise
    finally:
        logger.info("🛑 Завершение...")
        try:
            # Закрываем кеш
            await cache.disconnect()

            if polling_task and not polling_task.done():
                logger.info("Остановка polling...")
                polling_task.cancel()
                try:
                    await polling_task
                except asyncio.CancelledError:
                    pass
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
