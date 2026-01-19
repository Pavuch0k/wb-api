"""Telegram бот для парсинга данных Wildberries"""
import os
import logging
import asyncio
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from wb_api.client import WBAPIClient
from wb_api.excel_handler import ExcelHandler

# Загружаем переменные окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class WBBot:
    """Telegram бот для работы с данными Wildberries"""
    
    def __init__(self):
        """Инициализация бота"""
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        if not self.token:
            raise ValueError("TELEGRAM_BOT_TOKEN не найден в переменных окружения")
        
        self.wb_client = WBAPIClient()
        self.excel_handler = ExcelHandler()
        
        # Создаем приложение
        self.application = Application.builder().token(self.token).build()
        
        # Регистрируем обработчики команд
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("parse", self.parse_command))
        self.application.add_handler(CommandHandler("help", self.help_command))
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start"""
        welcome_message = (
            "Привет! Я бот для парсинга данных Wildberries.\n\n"
            "Доступные команды:\n"
            "/parse - Запустить парсинг данных за последний день\n"
            "/parse YYYY-MM-DD - Запустить парсинг за указанную дату\n"
            "/parse YYYY-MM-DD YYYY-MM-DD - Запустить парсинг за период\n"
            "/help - Показать справку"
        )
        await update.message.reply_text(welcome_message)
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /help"""
        help_message = (
            "📊 Бот для парсинга данных Wildberries\n\n"
            "Команды:\n"
            "• /start - Начать работу с ботом\n"
            "• /parse - Парсинг за сегодня\n"
            "• /parse 2025-01-15 - Парсинг за конкретную дату\n"
            "• /parse 2025-01-10 2025-01-15 - Парсинг за период\n"
            "• /help - Эта справка\n\n"
            "Данные сохраняются в файл wb_data.xlsx"
        )
        await update.message.reply_text(help_message)
    
    async def parse_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /parse"""
        try:
            args = context.args
            
            if len(args) == 0:
                # Парсинг за сегодня
                date_to = datetime.now()
                date_from = date_to - timedelta(days=1)
            elif len(args) == 1:
                # Парсинг за конкретную дату
                date_from = datetime.strptime(args[0], "%Y-%m-%d")
                date_to = date_from + timedelta(days=1)
            elif len(args) == 2:
                # Парсинг за период
                date_from = datetime.strptime(args[0], "%Y-%m-%d")
                date_to = datetime.strptime(args[1], "%Y-%m-%d")
            else:
                await update.message.reply_text(
                    "❌ Неверный формат команды. Используйте:\n"
                    "/parse - за сегодня\n"
                    "/parse YYYY-MM-DD - за дату\n"
                    "/parse YYYY-MM-DD YYYY-MM-DD - за период"
                )
                return
            
            await update.message.reply_text(
                f"🔄 Начинаю парсинг данных с {date_from.strftime('%d.%m.%Y')} "
                f"по {date_to.strftime('%d.%m.%Y')}..."
            )
            
            # Получаем данные из API
            date_from_str = date_from.strftime("%Y-%m-%d")
            date_to_str = date_to.strftime("%Y-%m-%d")
            
            # Получаем статистику продаж
            sales_data = self.wb_client.get_statistics(date_from_str, date_to_str)
            
            # Получаем остатки
            stocks_data = self.wb_client.get_stocks()
            
            # Получаем заказы
            orders_data = self.wb_client.get_orders(date_from_str, date_to_str)
            
            # Обрабатываем и сохраняем данные
            processed_count = 0
            for sale in sales_data:
                # Форматируем данные для таблицы
                sale_date = datetime.fromisoformat(sale.get("date", "")).strftime("%d.%m.%Y")
                
                wb_row_data = {
                    "К день": sale.get("totalPrice", 0),
                    "Касса день": sale.get("totalPrice", 0),
                    "заказы": sale.get("quantity", 0),
                    "ЦЕНА товара": sale.get("priceWithDisc", 0),
                }
                
                # Находим остаток для этого товара
                article = sale.get("nmId", "")
                stock_info = next((s for s in stocks_data if s.get("nmId") == article), None)
                if stock_info:
                    wb_row_data["остаток товар"] = stock_info.get("quantity", 0)
                
                # Добавляем данные в таблицу
                self.excel_handler.add_daily_data(sale_date, wb_row_data)
                processed_count += 1
            
            await update.message.reply_text(
                f"✅ Парсинг завершен!\n"
                f"Обработано записей: {processed_count}\n"
                f"Данные сохранены в файл: wb_data.xlsx"
            )
            
        except ValueError as e:
            await update.message.reply_text(f"❌ Ошибка формата даты: {str(e)}")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                error_msg = (
                    "⚠️ Превышен лимит запросов к API Wildberries.\n\n"
                    "Попробуйте позже или уменьшите частоту запросов.\n"
                    "Бот автоматически повторит попытку с задержкой."
                )
            elif e.response.status_code == 401:
                error_msg = "❌ Ошибка авторизации. Проверьте API ключ в .env файле."
            elif e.response.status_code == 403:
                error_msg = "❌ Доступ запрещен. Проверьте права доступа API ключа."
            else:
                error_msg = f"❌ HTTP ошибка {e.response.status_code}: {str(e)}"
            
            logger.error(f"HTTP ошибка при парсинге: {str(e)}", exc_info=True)
            await update.message.reply_text(error_msg)
        except requests.exceptions.Timeout:
            error_msg = (
                "⏱️ Превышено время ожидания ответа от API.\n"
                "Попробуйте позже или проверьте интернет-соединение."
            )
            logger.error("Таймаут при запросе к API", exc_info=True)
            await update.message.reply_text(error_msg)
        except Exception as e:
            logger.error(f"Ошибка при парсинге: {str(e)}", exc_info=True)
            await update.message.reply_text(
                f"❌ Произошла ошибка: {str(e)}\n\n"
                f"Проверьте логи для подробностей."
            )
    
    def run(self):
        """Запускает бота"""
        logger.info("Запуск бота...")
        self.application.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
            close_loop=False
        )


if __name__ == "__main__":
    bot = WBBot()
    bot.run()

