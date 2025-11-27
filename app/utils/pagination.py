# app/utils/pagination.py
from __future__ import annotations

from dataclasses import dataclass
from math import ceil
from typing import Generic, Iterable, List, Sequence, TypeVar

T = TypeVar("T")


@dataclass(slots=True)
class PaginationInfo:
    """
    Базовая информация о пагинации.

    total_items   — всего элементов
    per_page      — элементов на странице
    current_page  — текущая страница (1..total_pages)
    total_pages   — всего страниц
    offset        — смещение (для запросов в БД)
    limit         — лимит (per_page)
    has_prev      — есть ли предыдущая страница
    has_next      — есть ли следующая страница
    """
    total_items: int
    per_page: int
    current_page: int
    total_pages: int
    offset: int
    limit: int
    has_prev: bool
    has_next: bool


def get_pagination_info(
    total_items: int,
    page: int,
    per_page: int,
) -> PaginationInfo:
    """
    Рассчитать параметры пагинации.

    total_items — общее количество элементов
    page        — запрошенная страница (1..N)
    per_page    — количество элементов на странице
    """
    if per_page <= 0:
        raise ValueError("per_page must be positive")

    total_pages = max(1, ceil(total_items / per_page)) if total_items > 0 else 1
    current_page = max(1, min(page, total_pages))

    offset = (current_page - 1) * per_page
    limit = per_page

    has_prev = current_page > 1
    has_next = current_page < total_pages

    return PaginationInfo(
        total_items=total_items,
        per_page=per_page,
        current_page=current_page,
        total_pages=total_pages,
        offset=offset,
        limit=limit,
        has_prev=has_prev,
        has_next=has_next,
    )


def paginate_list(
    items: Sequence[T] | Iterable[T],
    page: int,
    per_page: int,
) -> List[T]:
    """
    Простая пагинация уже загруженного списка.

    Использование:
        page_items = paginate_list(items, page=2, per_page=10)

    Если передашь итератор — он будет приведён к list().
    """
    if per_page <= 0:
        raise ValueError("per_page must be positive")

    if not isinstance(items, Sequence):
        items = list(items)

    total_items = len(items)

    info = get_pagination_info(total_items=total_items, page=page, per_page=per_page)

    start = info.offset
    end = start + info.limit
    return list(items[start:end])
