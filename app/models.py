"""
Pydantic-модели для Structured Output.
Описывают структуру данных, которую должна возвращать LLM.

Pydantic автоматически:
- Проверяет типы полей (валидация)
- Преобразует данные (например, dict -> модель)
- Генерирует JSON-схему для передачи LLM
"""

from typing import List
from pydantic import BaseModel, Field


class ActionItem(BaseModel):
    """
    Одна задача (action item), извлечённая из текста встречи.
    """
    task: str = Field(
        description="Что нужно сделать. Например: 'Подготовить отчёт по продажам'."
    )
    assignee: str = Field(
        default="не указан",
        description="Кто отвечает за задачу. Если в тексте не указано — оставить 'не указан'."
    )
    deadline: str = Field(
        default="не указан",
        description="Срок выполнения. Если не указан — оставить 'не указан'."
    )


class MeetingSummary(BaseModel):
    """
    Полный структурированный результат анализа встречи.
    Это то, что LLM должна вернуть в формате JSON.
    """
    summary: str = Field(
        description="Краткое описание встречи в 2-3 предложениях."
    )
    key_points: List[str] = Field(
        default_factory=list,
        description="Список ключевых пунктов, обсуждённых на встрече."
    )
    action_items: List[ActionItem] = Field(
        default_factory=list,
        description="Список задач (что нужно сделать после встречи)."
    )


# Пример использования:
# если LLM вернёт такой JSON:
# {
#   "summary": "Обсудили релиз",
#   "key_points": ["Дата: 15 марта"],
#   "action_items": [{"task": "Обновить доки"}]
# }
#
# то Pydantic автоматически преобразует его в:
# MeetingSummary(
#     summary="Обсудили релиз",
#     key_points=["Дата: 15 марта"],
#     action_items=[ActionItem(task="Обновить доки", assignee="не указан", deadline="не указан")]
# )