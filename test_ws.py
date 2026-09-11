"""
Тестовый скрипт: загружает аудио через API и сразу подключается к WebSocket.
Так мы гарантированно не пропустим сообщения из Redis Pub/Sub.
"""
import asyncio
import json
import os
import sys
import httpx
import websockets


API_URL = "http://localhost:8011"
WS_URL = "ws://localhost:8011"


async def main(audio_file: str):
    if not os.path.exists(audio_file):
        print(f"[!] Файл не найден: {audio_file}")
        return

    # 1. Загружаем аудио
    print(f"[1] Загружаю: {audio_file}")
    async with httpx.AsyncClient(timeout=120.0) as client:
        with open(audio_file, "rb") as f:
            files = {"file": (os.path.basename(audio_file), f, "audio/mpeg")}
            r = await client.post(f"{API_URL}/upload/audio", files=files)

    if r.status_code != 200:
        print(f"[!] Ошибка загрузки: {r.status_code} {r.text}")
        return

    meeting_id = r.json()["meeting_id"]
    print(f"[2] meeting_id = {meeting_id}")

    # 2. СРАЗУ подключаемся к WebSocket
    url = f"{WS_URL}/ws/{meeting_id}"
    print(f"[3] Подключаюсь к {url}")

    async with websockets.connect(url) as ws:
        print("[4] Слушаю сообщения...")
        try:
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=180)
                data = json.loads(raw)
                print(f"    --> {json.dumps(data, ensure_ascii=False)}")
                if data.get("status") in ("done", "error"):
                    print("[5] Финал. Выход.")
                    break
        except asyncio.TimeoutError:
            print("[!] 180 секунд без сообщений.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Использование: python test_ws.py <аудиофайл>")
        sys.exit(1)
    asyncio.run(main(sys.argv[1]))