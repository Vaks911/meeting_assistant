"""
Демонстрация Function Calling / агентного цикла с GigaChat.

Запуск внутри контейнера воркера суммаризации:
    docker exec worker_summarize python -u -m worker_summarize.demo_tools

Что происходит:
1. Модели передаётся задача, требующая использования инструментов.
2. Модель сама решает вызвать create_calendar_event и get_meeting_stats.
3. Наши обработчики выполняются и возвращают результат модели.
4. Модель формирует финальный текстовый ответ.
"""

import os
import json

from gigachat import GigaChat
from gigachat.models import Chat, Messages, MessagesRole

from worker_summarize import tools as agent_tools


def main():
    credentials = os.getenv("GIGACHAT_CREDENTIALS", "").strip()
    client = GigaChat(
        credentials=credentials,
        verify_ssl_certs=False,
        scope="GIGACHAT_API_PERS",
    )
    functions = agent_tools.get_function_schemas()

    system = "You are an assistant with access to tools. Answer in Russian."
    user = (
        "Создай события календаря для двух задач со сроками: "
        "'Подготовить отчёт' (2026-09-20) и 'Обновить документацию' (2026-09-25). "
        "Затем получи статистику встреч за неделю. "
        "После вызовов инструментов ответь коротким текстом, что ты сделал."
    )

    messages = [
        Messages(role=MessagesRole.SYSTEM, content=system),
        Messages(role=MessagesRole.USER, content=user),
    ]

    print("=== AGENTIC LOOP ===")
    for step in range(6):
        response = client.chat(Chat(
            model=os.getenv("GIGACHAT_MODEL", "GigaChat-3-Ultra"),
            messages=messages,
            functions=functions,
            function_call="auto",
            temperature=0.3,
            max_tokens=1000,
        ))
        msg = response.choices[0].message

        if getattr(msg, "function_call", None):
            name = msg.function_call.name
            args = agent_tools.parse_arguments(getattr(msg.function_call, "arguments", None))
            print(f"\n[{step}] Модель вызвала функцию: {name}")
            print(f"     аргументы: {json.dumps(args, ensure_ascii=False)}")

            result = agent_tools.execute_tool(name, args)
            print(f"     результат: {json.dumps(result, ensure_ascii=False)}")

            messages.append(Messages(
                role=MessagesRole.ASSISTANT,
                content=msg.content,
                function_call=msg.function_call,
            ))
            messages.append(Messages(
                role=MessagesRole.FUNCTION,
                name=name,
                content=json.dumps(result, ensure_ascii=False),
            ))
        else:
            print(f"\n[{step}] Финальный ответ модели:\n{msg.content}")
            break
    else:
        print("Не удалось завершить за 6 шагов.")


if __name__ == "__main__":
    main()