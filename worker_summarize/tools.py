"""
Инструменты (tools) для agentic-цикла GigaChat.

Во время анализа встречи GigaChat может сам решить вызвать одну из этих
функций, получить результат и продолжить рассуждение. Это реализация
подхода "Agentic LLM pipelines / function calling".

Доступные инструменты:
1. send_telegram_alert(meeting_id, message) — уведомление о завершении встречи.
2. get_meeting_stats(period)                 — статистика встреч за период
                                               (реальный запрос к PostgreSQL).
3. create_calendar_event(task, deadline)     — создание события календаря
                                               по action item.

Примечание: telegram и calendar — рабочие симуляции (логирование + Redis),
т.к. реальные внешние интеграции здесь не подключены. get_meeting_stats —
настоящий запрос к БД.
"""

import json
from datetime import datetime, timedelta

from gigachat.models import Function
from sqlalchemy import func

from app.database import SessionLocal, Meeting


def parse_arguments(arguments):
    """
    Аргументы функции от GigaChat приходят то строкой (JSON),
    то уже готовым dict. Нормализуем в dict для execute_tool.
    """
    if isinstance(arguments, dict):
        return arguments
    if isinstance(arguments, str) and arguments.strip():
        try:
            return json.loads(arguments)
        except json.JSONDecodeError:
            return {}
    return {}


# ===========================================================
# СХЕМЫ ФУНКЦИЙ ДЛЯ GIGACHAT (JSON Schema -> dict)
# ===========================================================
def _fn(name: str, description: str, properties: dict, required: list) -> Function:
    """Вспомогательный конструктор объекта Function."""
    return Function(
        name=name,
        description=description,
        parameters={
            "type": "object",
            "properties": properties,
            "required": required,
        },
    )


def get_function_schemas() -> list:
    """Возвращает список схем функций, которые передаются в Chat(functions=[...])."""
    return [
        _fn(
            name="send_telegram_alert",
            description="Отправить уведомление о завершении обработки встречи в Telegram.",
            properties={
                "meeting_id": {"type": "string", "description": "Идентификатор встречи."},
                "message": {"type": "string", "description": "Текст уведомления."},
            },
            required=["meeting_id", "message"],
        ),
        _fn(
            name="get_meeting_stats",
            description=(
                "Получить статистику встреч за последний период "
                "(количество, выполненные, с ошибками). Период: day, week или month."
            ),
            properties={
                "period": {
                    "type": "string",
                    "enum": ["day", "week", "month"],
                    "description": "Период: 'day', 'week' или 'month'.",
                }
            },
            required=["period"],
        ),
        _fn(
            name="create_calendar_event",
            description="Создать событие в календаре по задаче (action item) со сроком выполнения.",
            properties={
                "task": {"type": "string", "description": "Описание задачи."},
                "deadline": {
                    "type": "string",
                    "description": "Срок выполнения в формате YYYY-MM-DD.",
                },
            },
            required=["task"],
        ),
    ]


# ===========================================================
# РЕАЛИЗАЦИИ ИНСТРУМЕНТОВ
# ===========================================================
def _send_telegram_alert(args: dict) -> dict:
    """
    СИМУЛЯЦИЯ уведомления в Telegram.
    В production замените на вызов Bot API:
      https://api.telegram.org/bot<TOKEN>/sendMessage?chat_id=<ID>&text=<msg>
    """
    meeting_id = args.get("meeting_id", "")
    message = args.get("message", "")
    payload = {
        "tool": "send_telegram_alert",
        "meeting_id": meeting_id,
        "message": message,
        "sent_at": datetime.utcnow().isoformat(),
    }
    print(f"[TOOL] send_telegram_alert -> {json.dumps(payload, ensure_ascii=False)}")
    return {"status": "sent", **payload}


def _get_meeting_stats(args: dict) -> dict:
    """
    Реальная статистика встреч из PostgreSQL за последний период.
    Возвращает количество всего, выполненных, с ошибками и в обработке.
    """
    period = args.get("period", "week")
    days = {"day": 1, "week": 7, "month": 30}.get(period, 7)
    since = datetime.utcnow() - timedelta(days=days)

    db = SessionLocal()
    try:
        def _count(**filters):
            query = db.query(func.count(Meeting.id)).filter(Meeting.created_at >= since)
            for key, value in filters.items():
                query = query.filter(getattr(Meeting, key) == value)
            return query.scalar()

        total = _count()
        done = _count(status="done")
        error = _count(status="error")
        return {
            "period": period,
            "since": since.isoformat(),
            "total_meetings": total,
            "done": done,
            "error": error,
            "in_progress": max(0, total - done - error),
        }
    finally:
        db.close()


def _create_calendar_event(args: dict) -> dict:
    """
    СИМУЛЯЦИЯ создания события календаря.
    В production замените на интеграцию с Google Calendar / Outlook API.
    """
    task = args.get("task", "")
    deadline = args.get("deadline", "не указан")
    payload = {
        "tool": "create_calendar_event",
        "task": task,
        "deadline": deadline,
        "created_at": datetime.utcnow().isoformat(),
    }
    print(f"[TOOL] create_calendar_event -> {json.dumps(payload, ensure_ascii=False)}")
    return {"status": "created", **payload}


# Реестр: имя функции -> обработчик
_TOOL_HANDLERS = {
    "send_telegram_alert": _send_telegram_alert,
    "get_meeting_stats": _get_meeting_stats,
    "create_calendar_event": _create_calendar_event,
}


def execute_tool(name: str, args: dict) -> dict:
    """
    Выполняет инструмент по имени и возвращает JSON-совместимый результат,
    который отправится обратно модели как сообщение role='function'.
    Неизвестный инструмент возвращает ошибку (модель это увидит).
    """
    handler = _TOOL_HANDLERS.get(name)
    if handler is None:
        return {"error": f"Unknown tool: {name}"}
    try:
        result = handler(args)
        return result if isinstance(result, dict) else {"result": result}
    except Exception as exc:  # pragma: no cover
        return {"error": f"{name} failed: {exc}"}