"""
Модуль для работы с базой данных PostgreSQL через SQLAlchemy.
Содержит:
- Настройку подключения
- Модель таблицы meetings
- Функции для CRUD-операций (Create, Read, Update)
"""

import os
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# ===========================================================
# 1. НАСТРОЙКА ПОДКЛЮЧЕНИЯ К БАЗЕ ДАННЫХ
# ===========================================================
# URL берётся из переменной окружения DATABASE_URL (задана в docker-compose).
# Формат: postgresql://<user>:<password>@<host>:<port>/<dbname>
# Если переменная не задана (например, локальный запуск без Docker),
# используется значение по умолчанию (localhost).
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5433/meetings')

# create_engine создаёт "движок" — объект, который управляет соединениями с БД.
# Он ленивый: реальное соединение устанавливается при первом запросе.
engine = create_engine(DATABASE_URL)

# SessionLocal — фабрика сессий. Каждая сессия — это "разговор" с БД:
# открыть, сделать несколько операций, закрыть.
# autocommit=False — мы сами решаем, когда сохранять (db.commit()).
# autoflush=False — не отправляем изменения автоматически, только по команде.
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base — базовый класс для всех наших моделей.
# От него мы будем наследовать класс Meeting.
Base = declarative_base()


# ===========================================================
# 2. МОДЕЛЬ ТАБЛИЦЫ "meetings"
# ===========================================================
class Meeting(Base):
    """
    Модель встречи. Соответствует таблице meetings в PostgreSQL.
    """
    __tablename__ = "meetings"      # Имя таблицы в БД

    id = Column(String, primary_key=True, index=True)         # Уникальный ID (UUID)
    filename = Column(String)                                  # Путь к аудиофайлу
    status = Column(String, default="pending")                 # Статус обработки
    transcription = Column(Text, default="")                   # Расшифровка
    summary = Column(Text, default="")                         # Саммари
    action_items = Column(Text, default="")                    # Список задач в виде JSON-строки
    created_at = Column(DateTime, default=datetime.utcnow)     # Дата создания


# ===========================================================
# 3. ФУНКЦИИ ДЛЯ РАБОТЫ С БАЗОЙ
# ===========================================================
def init_db():
    """
    Создаёт все таблицы в БД, если их ещё нет.
    Вызывается один раз при старте приложения (например, в main.py).
    """
    Base.metadata.create_all(bind=engine)


def create_meeting(meeting_id: str, file_path: str):
    """
    Создаёт новую запись о встрече со статусом 'pending'.
    :param meeting_id: UUID встречи
    :param file_path: путь к загруженному аудио
    """
    db = SessionLocal()                          # Открываем сессию
    try:
        meeting = Meeting(id=meeting_id, filename=file_path)
        db.add(meeting)                          # Добавляем объект в сессию
        db.commit()                              # Сохраняем в БД
    finally:
        db.close()                               # Всегда закрываем сессию


def update_meeting(meeting_id: str, **kwargs):
    """
    Обновляет поля встречи по её ID.
    Пример: update_meeting("uuid-123", status="done", summary="...")
    :param meeting_id: UUID встречи
    :param kwargs: любые поля модели Meeting для обновления
    """
    db = SessionLocal()
    try:
        meeting = db.query(Meeting).filter(Meeting.id == meeting_id).first()
        if meeting:
            # Перебираем переданные поля и присваиваем новые значения
            for key, value in kwargs.items():
                setattr(meeting, key, value)
            db.commit()
    finally:
        db.close()


def get_meeting(meeting_id: str):
    """
    Возвращает данные встречи в виде словаря.
    Если встреча не найдена, возвращает None.
    """
    db = SessionLocal()
    try:
        meeting = db.query(Meeting).filter(Meeting.id == meeting_id).first()
        if meeting:
            return {
                "id": meeting.id,
                "filename": meeting.filename,
                "status": meeting.status,
                "transcription": meeting.transcription,
                "summary": meeting.summary,
                "action_items": meeting.action_items,
                "created_at": meeting.created_at.isoformat() if meeting.created_at else None,
            }
        return None
    finally:
        db.close()