from fastapi import FastAPI, Request, HTTPException, status
from aiogram import Bot, Dispatcher
from aiogram.types import Update
from app.config import settings


def create_app(dp: Dispatcher, bot: Bot) -> FastAPI:
    app = FastAPI()

    @app.post(settings.webhook_path)
    async def telegram_webhook(request: Request):
        # ✅ Проверяем секретный токен
        secret_header = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
        if secret_header != settings.webhook_secret_token:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Invalid secret token",
            )

        data = await request.json()
        update = Update.model_validate(data)
        await dp.feed_update(bot, update)
        return {"ok": True}

    @app.get("/healthz")
    async def healthz():
        return {"ok": True}

    return app
