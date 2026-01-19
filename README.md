# WB API Bot

Telegram бот для парсинга данных с API Wildberries в таблицу Excel.

## Установка

1. Создайте виртуальное окружение:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# или
venv\Scripts\activate  # Windows
```

2. Установите зависимости:
```bash
pip install -r requirements.txt
```

3. Настройте переменные окружения:
Скопируйте `.env.example` в `.env` и заполните значения:
- `WB_API_KEY` - API ключ Wildberries
- `TELEGRAM_BOT_TOKEN` - Токен Telegram бота

## Запуск

```bash
python bot.py
```

## Использование

После запуска бота в Telegram используйте команды:

- `/start` - Начать работу с ботом
- `/parse` - Запустить парсинг данных за сегодня
- `/parse YYYY-MM-DD` - Запустить парсинг за конкретную дату
- `/parse YYYY-MM-DD YYYY-MM-DD` - Запустить парсинг за период
- `/help` - Показать справку

## Структура проекта

```
wb-api/
├── bot.py                 # Основной файл бота
├── wb_api/               # Модуль для работы с WB API
│   ├── __init__.py
│   ├── client.py         # Клиент для API Wildberries
│   └── excel_handler.py  # Обработчик Excel файлов
├── requirements.txt       # Зависимости проекта
├── .env                  # Переменные окружения (не коммитится)
└── README.md             # Документация
```

## Примечания

- Данные сохраняются в файл `wb_data.xlsx`
- Зеленые поля в таблице (комментарии, ставки и т.д.) вносятся вручную
- Остальные данные берутся автоматически с API Wildberries

