from __future__ import annotations

import time
import logging
from dataclasses import dataclass
from typing import Sequence

@dataclass
class StageResult:
    title: str
    icon: str
    duration: float
    status: str  # OK | WARN | SKIP | FAIL
    messages: list[str]

class StageCtx:
    def __init__(self, logger: logging.Logger, title: str, icon: str, success_message: str | None = None):
        self.logger = logger
        self.title = title
        self.icon = icon
        self.success_message = success_message
        self._start = 0.0
        self.messages: list[str] = []
        self.status = "OK"

    async def __aenter__(self):
        self._start = time.perf_counter()
        self.logger.info("%s %s — старт", self.icon, self.title)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        dur = time.perf_counter() - self._start
        if exc:
            self.status = "FAIL"
            self.logger.error("%s %s — ошибка (%.2fs): %s", self.icon, self.title, dur, exc)
            return False
        if self.status == "OK" and self.success_message:
            self.logger.info("✅ %s", self.success_message)
        self.logger.info("⏱️ %s завершено за %.2fs", self.title, dur)

    def log(self, msg: str):
        self.messages.append(msg)
        self.logger.info(" • %s", msg)

    def success(self, msg: str):
        self.status = "OK"
        self.log(msg)

    def warning(self, msg: str):
        self.status = "WARN"
        self.logger.warning(" • %s", msg)

    def skip(self, msg: str):
        self.status = "SKIP"
        self.logger.info("⏭️ %s", msg)

class StartupTimeline:
    def __init__(self, logger: logging.Logger, app_name: str):
        self.logger = logger
        self.app_name = app_name
        self.results: list[StageResult] = []
        self.banner_printed = False

    def log_banner(self, items: Sequence[tuple[str, str | int | bool]]):
        if self.banner_printed:
            return
        self.logger.info("────────────────────────────────────────")
        self.logger.info("🚀 %s — запуск", self.app_name)
        for k, v in items:
            self.logger.info("  %s: %s", k, v)
        self.logger.info("────────────────────────────────────────")
        self.banner_printed = True

    def add_manual_step(self, title: str, icon: str, status: str, note: str | None = None):
        self.logger.info("%s %s — %s%s", icon, title, status, f" ({note})" if note else "")

    def log_section(self, title: str, lines: Sequence[str], icon: str = "•"):
        self.logger.info("%s %s", icon, title)
        for ln in lines:
            self.logger.info("   - %s", ln)

    def log_summary(self):
        self.logger.info("✅ Итог: приложение готово к работе")

    def stage(self, title: str, icon: str, success_message: str | None = None):
        ctx = StageCtx(self.logger, title, icon, success_message)
        return ctx
