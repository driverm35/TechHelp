"""
Клиент для работы с Google Sheets через gspread.
"""
import os
import asyncio
import gspread
import time
from typing import Any, List, Optional, Union
from gspread import Spreadsheet
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import AuthorizedSession
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from app.config import settings

import logging

log = logging.getLogger("gspread_client")

_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def _make_creds() -> Credentials:
    """
    Создаёт credentials из переменной окружения GOOGLE_SERVICE_ACCOUNT_JSON_PATH
    (для Docker) или из settings.GOOGLE_SERVICE_ACCOUNT_JSON_PATH (для локального запуска).
    """

    # 1) Пробуем из переменной окружения (удобно для Docker)
    path = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH", "").strip()

    # 2) Если не установлена — пробуем из настроек (локальный запуск / .env)
    if not path:
        path = getattr(settings, "google_sheets_json_path", "") or ""
        path = path.strip()

    if not path:
        raise RuntimeError(
            "Google credentials не найдены.\n"
            "Установите GOOGLE_SERVICE_ACCOUNT_JSON_PATH (env) или "
            "GOOGLE_SERVICE_ACCOUNT_JSON_PATH в settings."
        )

    if not os.path.exists(path):
        raise RuntimeError(
            f"Файл credentials не найден по пути: {path}\n"
            f"Проверь путь и файловую систему (для Docker — volume)."
        )

    log.debug(f"Загружаю credentials из файла: {path}")
    return Credentials.from_service_account_file(path, scopes=_SCOPES)


def _make_client(creds):
    """Создает gspread клиент с retry логикой"""
    session = AuthorizedSession(creds)
    session.proxies = {}

    # Настраиваем retry для надежности
    retry = Retry(
        total=3,
        backoff_factor=0.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=False  # Retry для всех методов
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return gspread.Client(auth=creds, session=session)

def _ensure_spreadsheet(
    spreadsheet: Union[gspread.Spreadsheet, str]
) -> gspread.Spreadsheet:
    """
    Гарантирует, что у нас есть объект gspread.Spreadsheet.

    spreadsheet может быть:
      • gspread.Spreadsheet — тогда вернём как есть
      • str — тогда считаем, что это либо spreadsheet_id, либо URL
    """
    if isinstance(spreadsheet, gspread.Spreadsheet):
        return spreadsheet

    if not isinstance(spreadsheet, str):
        raise TypeError(
            f"Ожидался gspread.Spreadsheet или str, получено: {type(spreadsheet)!r}"
        )

    key_or_url = spreadsheet.strip()
    if not key_or_url:
        raise ValueError("Пустая строка spreadsheet_id/url")

    # Если это URL
    if key_or_url.startswith("http://") or key_or_url.startswith("https://"):
        log.debug(f"Открываю Google Sheet по URL: {key_or_url}")
        return _GC.open_by_url(key_or_url)

    # Иначе считаем, что это spreadsheet_id
    log.debug(f"Открываю Google Sheet по ключу: {key_or_url}")
    return _GC.open_by_key(key_or_url)


# Глобальные объекты
_CREDS = _make_creds()
_GC = _make_client(_CREDS)

# Кеш для листов: ключ = (spreadsheet_id, worksheet_title)
_WS_CACHE: dict[tuple[str, str], tuple[float, gspread.Worksheet]] = {}
_WS_TTL = 600  # секунд (10 минут)


async def to_thread(func, *args, **kwargs):
    """Выполняет синхронную функцию в отдельном потоке"""
    return await asyncio.to_thread(func, *args, **kwargs)


def _get_worksheet_from_spreadsheet(
    spreadsheet: gspread.Spreadsheet,
    worksheet_name: str
) -> gspread.Worksheet:
    """
    Получает worksheet по имени из spreadsheet с кешированием.

    Args:
        spreadsheet: Объект таблицы
        worksheet_name: Название листа

    Returns:
        Объект Worksheet
    """
    cache_key = (spreadsheet.id, worksheet_name)
    now = time.time()

    # Проверяем кеш
    if cache_key in _WS_CACHE:
        cached_time, cached_ws = _WS_CACHE[cache_key]
        if now - cached_time < _WS_TTL:
            log.debug(f"Возвращаю worksheet '{worksheet_name}' из кеша")
            return cached_ws

    # Получаем из API
    log.debug(f"Загружаю worksheet '{worksheet_name}' из Google Sheets")
    worksheet = spreadsheet.worksheet(worksheet_name)
    _WS_CACHE[cache_key] = (now, worksheet)

    return worksheet

def _ensure_spreadsheet(spreadsheet: Union[Spreadsheet, str]) -> Spreadsheet:
    """
    Превращает то, что нам передали, в gspread.Spreadsheet.
    Поддерживает:
      • уже готовый Spreadsheet
      • строку с ID таблицы
      • строку с URL таблицы
    """
    if isinstance(spreadsheet, Spreadsheet):
        return spreadsheet

    key_or_url = str(spreadsheet).strip()
    if not key_or_url:
        raise ValueError("Пустое значение spreadsheet")

    # Если это URL
    if key_or_url.startswith("http://") or key_or_url.startswith("https://"):
        log.debug(f"Открываю Spreadsheet по URL: {key_or_url}")
        return _GC.open_by_url(key_or_url)

    # Иначе считаем, что это ID
    log.debug(f"Открываю Spreadsheet по ключу: {key_or_url}")
    return _GC.open_by_key(key_or_url)

async def get_all_values_from_sheet(
    spreadsheet: gspread.Spreadsheet,
    worksheet_name: str
) -> List[List[str]]:
    """
    Получает все значения из конкретного листа.

    Args:
        spreadsheet: Объект таблицы
        worksheet_name: Название листа

    Returns:
        Двумерный список значений
    """
    def _get():
        ws = _get_worksheet_from_spreadsheet(spreadsheet, worksheet_name)
        return ws.get_all_values()

    log.debug(f"Получаю все значения из листа '{worksheet_name}'")
    return await to_thread(_get)

async def get_user_data_by_tg_id(
    spreadsheet: gspread.Spreadsheet,
    tg_id: int
) -> Optional[dict[str, Any]]:
    """
    Ищет данные пользователя по Telegram ID.

    Args:
        spreadsheet: Объект таблицы
        tg_id: Telegram ID пользователя

    Returns:
        Словарь с данными пользователя или None
    """
    def _find():
        ws = _get_worksheet_from_spreadsheet(spreadsheet, "Clients")
        records = ws.get_all_records()
        for record in records:
            if record.get("tg_id") == tg_id:
                return record
        return None

    log.debug(f"Ищу данные пользователя с tg_id={tg_id}")
    return await to_thread(_find)

async def find_in_column_j_across_sheets(
    spreadsheet,
    value: Any,
    *,
    exact: bool = True,
) -> Optional[dict]:
    """
    Ищет по колонке J (индекс 9) среди всех листов
    и возвращает СРАЗУ нормализованный словарь данных клиента.
    spreadsheet: Spreadsheet или str (id / url)
    """

    def _search():
        ss = _ensure_spreadsheet(spreadsheet)
        search_str = str(value).strip()

        for ws in ss.worksheets():
            rows = ws.get_all_values()
            if not rows:
                continue

            # можно пропустить первую строку, если там заголовки
            for row_index, row in enumerate(rows[1:], start=2):
                if len(row) <= 9:
                    continue

                cell = str(row[9]).strip()  # колонка J - TG ID

                if exact:
                    match = (cell == search_str)
                else:
                    match = (search_str in cell)

                if not match:
                    continue

                # Преобразуем строку в правильный словарь
                data = {
                    "username":       row[0] if len(row) > 0 else None,   # A
                    "phone":          row[1] if len(row) > 1 else None,   # B
                    "fio":            row[2] if len(row) > 2 else None,   # C
                    "city":           row[3] if len(row) > 3 else None,   # D
                    "model":          row[4] if len(row) > 4 else None,   # E
                    "serial":         row[5] if len(row) > 5 else None,   # F
                    "warranty_date":  row[6] if len(row) > 6 else None,   # G
                    "warranty_file_id": row[7] if len(row) > 7 else None, # H
                    "created_at":     row[8] if len(row) > 8 else None,   # I
                    "tg_id":          row[9] if len(row) > 9 else None,   # J
                    "birthday":       row[10] if len(row) > 10 else None, # K
                    "platform":       row[11] if len(row) > 11 else None, # L
                    "order_date":     row[12] if len(row) > 12 else None, # M
                    "worksheet_title": ws.title,
                    "row_index":      row_index,
                }

                log.info(
                    f"Поиск по колонке J: совпадение в листе '{ws.title}', "
                    f"строка {row_index}, tg_id={data['tg_id']}"
                )
                return data

        log.info(
            f"Поиск по колонке J завершен: совпадений не найдено "
            f"для значения '{search_str}'"
        )
        return None

    log.debug(f"🔍 Запускаю поиск по колонке J во всех листах, value={value!r}, exact={exact}")
    return await to_thread(_search)
