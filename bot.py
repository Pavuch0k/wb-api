"""Telegram бот для парсинга данных Wildberries"""
import os
import logging
import asyncio
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from telegram import Update
from telegram.error import TimedOut, NetworkError, TelegramError
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
        
        # Создаем приложение
        self.application = Application.builder().token(self.token).build()
        
        # Регистрируем обработчики команд
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("parse", self.parse_command))
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start"""
        welcome_message = (
            "Привет! Я бот для парсинга данных Wildberries.\n\n"
            "Команда:\n"
            "/parse - Запустить парсинг данных за сегодня\n\n"
            "Каждый раз создается новый файл с датой и временем парсинга."
        )
        await self._safe_reply(update, welcome_message)
    
    async def _safe_reply(self, update: Update, text: str, max_retries: int = 3):
        """
        Безопасная отправка сообщения с повторными попытками
        
        Args:
            update: Обновление от Telegram
            text: Текст сообщения
            max_retries: Максимальное количество попыток
        """
        for attempt in range(max_retries):
            try:
                await update.message.reply_text(text)
                return
            except (TimedOut, NetworkError) as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    logger.warning(f"Ошибка отправки сообщения в Telegram: {str(e)}. Повтор через {wait_time} сек ({attempt + 1}/{max_retries})")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Не удалось отправить сообщение после {max_retries} попыток: {str(e)}")
                    raise
            except TelegramError as e:
                logger.error(f"Ошибка Telegram API: {str(e)}")
                raise
    
    async def parse_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /parse - парсинг данных за текущий день"""
        try:
            # Парсинг только за сегодня
            today = datetime.now().date()
            date_from = today
            date_to = today
            
            # Создаем новый ExcelHandler с новым файлом (на основе шаблона wb_data.xlsx)
            # Файлы сохраняются в папку excel_files
            excel_dir = "excel_files"
            os.makedirs(excel_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            excel_file_path = os.path.join(excel_dir, f"wb_data_{timestamp}.xlsx")
            excel_handler = ExcelHandler(file_path=excel_file_path, template_path="wb_data.xlsx")
            
            await self._safe_reply(update, f"🔄 Начинаю парсинг данных за {date_from.strftime('%d.%m.%Y')}...")
            
            # Получаем данные из API
            date_from_str = date_from.strftime("%Y-%m-%d")
            date_to_str = date_to.strftime("%Y-%m-%d")
            
            # Получаем статистику продаж за день
            # Лимиты API соблюдаются автоматически в _wait_for_rate_limit
            logger.info("Получение данных о продажах...")
            sales_data = self.wb_client.get_statistics(date_from_str, date_to_str)
            
            # Касса месяц - не получаем, используем 0
            month_total = 0
            
            # Получаем остатки (требуется dateFrom)
            logger.info("Получение данных об остатках...")
            stocks_data = self.wb_client.get_stocks(date_from_str)
            
            # Получаем заказы
            logger.info("Получение данных о заказах...")
            orders_data = self.wb_client.get_orders(date_from_str, date_to_str)
            
            # Получаем рекламную статистику (если доступна)
            logger.info("Получение данных о рекламе...")
            try:
                advert_data = self.wb_client.get_advert_statistics(date_from_str, date_to_str)
                logger.info(f"Получено {len(advert_data)} записей рекламной статистики")
            except Exception as e:
                logger.warning(f"Не удалось получить рекламную статистику: {str(e)}")
                advert_data = []
            
            # Получаем данные воронки продаж (если доступна)
            logger.info("Получение данных воронки продаж...")
            try:
                funnel_data = self.wb_client.get_sales_funnel(date_from_str, date_to_str)
                logger.info(f"Получено {len(funnel_data)} записей воронки продаж")
            except Exception as e:
                logger.warning(f"Не удалось получить данные воронки продаж: {str(e)}")
                funnel_data = []
            
            # Обрабатываем и сохраняем данные
            # Группируем данные по датам для оптимизации
            from collections import defaultdict
            data_by_date = defaultdict(lambda: {
                "sales": [],
                "total_price": 0,
                "total_quantity": 0,
                "articles": set()
            })
            
            logger.info(f"Получено {len(sales_data)} записей продаж, {len(stocks_data)} записей остатков, {len(orders_data)} заказов")
            sales_details = [f"арт.{s.get('nmId', '?')} цена {s.get('priceWithDisc', 0)} кол-во {s.get('quantity', 0)}" for s in sales_data[:5]]
            logger.info(f"Детали продаж (первые 5): {sales_details}")
            
            # Группируем продажи по датам
            for sale in sales_data:
                sale_date = datetime.fromisoformat(sale.get("date", "")).strftime("%d.%m.%Y")
                article = sale.get("nmId", "")
                price = sale.get("priceWithDisc", 0)
                # В sales нет quantity, считаем количество продаж по записям (isRealization=True)
                quantity = 1 if sale.get("isRealization", False) else 0
                total_price = sale.get("totalPrice", 0)
                
                data_by_date[sale_date]["sales"].append(sale)
                data_by_date[sale_date]["total_price"] += total_price
                data_by_date[sale_date]["total_quantity"] += quantity
                data_by_date[sale_date]["articles"].add(article)
            
            # Группируем заказы по датам для получения правильного количества заказов
            orders_by_date = defaultdict(lambda: {"count": 0, "total_price": 0})
            for order in orders_data:
                try:
                    order_date = datetime.fromisoformat(order.get("date", "")).strftime("%d.%m.%Y")
                    # Считаем только не отмененные заказы
                    if not order.get("isCancel", False):
                        orders_by_date[order_date]["count"] += 1
                        orders_by_date[order_date]["total_price"] += order.get("totalPrice", 0)
                except Exception as e:
                    logger.debug(f"Ошибка обработки заказа: {str(e)}")
            
            # Агрегируем данные по рекламе по датам
            advert_by_date = defaultdict(lambda: {
                "impressions": 0,
                "clicks": 0,
                "ctr": 0.0,
                "cost_auc": 0.0,
                "cost_ark": 0.0,
                "click_price_auc": 0.0,
                "click_price_ark": 0.0
            })
            
            for advert in advert_data:
                try:
                    # Дата может быть в формате YYYY-MM-DD или уже в нужном формате
                    adv_date_str = advert.get("date", "")
                    if adv_date_str:
                        try:
                            adv_date = datetime.strptime(adv_date_str, "%Y-%m-%d").strftime("%d.%m.%Y")
                        except:
                            adv_date = adv_date_str  # Уже в нужном формате
                    else:
                        continue
                    
                    advert_by_date[adv_date]["impressions"] += advert.get("impressions", 0) or 0
                    advert_by_date[adv_date]["clicks"] += advert.get("clicks", 0) or 0
                    # Расход может быть общий, разделяем на АУКЦ и АРК (если есть разделение)
                    cost_total = advert.get("sum", 0) or advert.get("cost", 0) or 0
                    advert_by_date[adv_date]["cost_auc"] += cost_total  # Пока используем общий расход
                    advert_by_date[adv_date]["cost_ark"] += 0  # АРК пока не разделяем
                    
                    # Цена клика - вычисляем из расхода и кликов, если cpc не указан
                    clicks = advert.get("clicks", 0) or 0
                    cpc = advert.get("cpc", 0) or 0
                    if cpc > 0:
                        # Используем цену клика из API
                        advert_by_date[adv_date]["click_price_auc"] = cpc
                        advert_by_date[adv_date]["click_price_ark"] = cpc
                    elif clicks > 0 and cost_total > 0:
                        # Вычисляем цену клика: расход / клики
                        calculated_cpc = cost_total / clicks
                        advert_by_date[adv_date]["click_price_auc"] = calculated_cpc
                        advert_by_date[adv_date]["click_price_ark"] = calculated_cpc
                except Exception as e:
                    logger.debug(f"Ошибка обработки рекламных данных: {str(e)}")
            
            # Агрегируем данные воронки продаж по датам
            funnel_by_date = defaultdict(lambda: {
                "card_views": 0,
                "baskets": 0,
                "orders": 0
            })
            
            for funnel in funnel_data:
                try:
                    # Воронка продаж возвращает дату в формате YYYY-MM-DD
                    fun_date_str = funnel.get("date", "")
                    if fun_date_str:
                        fun_date = datetime.strptime(fun_date_str, "%Y-%m-%d").strftime("%d.%m.%Y")
                    else:
                        continue
                    
                    funnel_by_date[fun_date]["card_views"] += funnel.get("openCount", 0) or 0  # Показы/перешли в карточку
                    funnel_by_date[fun_date]["baskets"] += funnel.get("cartCount", 0) or 0  # Корзин
                    funnel_by_date[fun_date]["orders"] += funnel.get("orderCount", 0) or 0  # Заказы
                except Exception as e:
                    logger.debug(f"Ошибка обработки данных воронки: {str(e)}")
            
            # Обрабатываем данные по датам
            processed_count = 0
            new_count = 0
            details = []
            
            # Обрабатываем только текущий день
            today_str = today.strftime("%d.%m.%Y")
            
            # Фильтруем данные только за сегодня
            if today_str not in data_by_date:
                await self._safe_reply(update, f"⚠️ Нет данных за {today_str}")
                return
            
            date_data = data_by_date[today_str]
            logger.info(f"Обработка даты {today_str}: {len(date_data['sales'])} продаж, артикулов: {len(date_data['articles'])}, сумма: {date_data['total_price']}")
            
            # Формируем агрегированные данные для даты
            wb_row_data = {
                "К день": date_data["total_price"],
                "Касса день": date_data["total_price"],
                "Касса месяц": month_total,  # Сумма продаж за весь месяц
            }
            
            # Количество заказов из orders_data
            if today_str in orders_by_date:
                wb_row_data["заказы"] = orders_by_date[today_str]["count"]
            else:
                # Если нет данных о заказах, используем количество продаж
                wb_row_data["заказы"] = date_data["total_quantity"]
            
            # Берем среднюю цену товара или первую найденную
            if date_data["sales"]:
                avg_price = sum(s.get("priceWithDisc", 0) for s in date_data["sales"]) / len(date_data["sales"])
                wb_row_data["ЦЕНА товара"] = avg_price
            
            # Находим остатки для всех артикулов этой даты
            total_stock = 0
            for article in date_data["articles"]:
                stock_info = next((s for s in stocks_data if s.get("nmId") == article), None)
                if stock_info:
                    # В stocks может быть quantity или amount
                    stock_qty = stock_info.get("quantity", 0) or stock_info.get("amount", 0) or 0
                    total_stock += stock_qty
            
            if total_stock > 0:
                wb_row_data["остаток товара на складе"] = total_stock
                
                # Вычисляем "хран день" - остаток умножить на 0.15
                storage_days = total_stock * 0.15
                wb_row_data["хран день"] = round(storage_days, 2)  # Округляем до 2 знаков после запятой
            
            # Добавляем данные о рекламе
            if today_str in advert_by_date:
                adv = advert_by_date[today_str]
                wb_row_data["показы"] = adv["impressions"]
                wb_row_data["клики"] = adv["clicks"]
                if adv["clicks"] > 0 and adv["impressions"] > 0:
                    wb_row_data["CTR"] = (adv["clicks"] / adv["impressions"]) * 100
                
                # Цена клика АУКЦ и АРК (сначала заполняем их)
                if adv.get("click_price_auc", 0) > 0:
                    wb_row_data["цена клика АУКЦ"] = adv["click_price_auc"]
                elif adv["clicks"] > 0 and adv["cost_auc"] > 0:
                    wb_row_data["цена клика АУКЦ"] = adv["cost_auc"] / adv["clicks"]
                
                if adv.get("click_price_ark", 0) > 0:
                    wb_row_data["цена клика АРК"] = adv["click_price_ark"]
                elif adv["clicks"] > 0 and adv["cost_ark"] > 0:
                    wb_row_data["цена клика АРК"] = adv["cost_ark"] / adv["clicks"]
                
                # Цена клика (общая) - используем АУКЦ, если нет - то АРК, если нет - вычисляем
                if adv["clicks"] > 0:
                    # Сначала пробуем использовать цену клика АУКЦ
                    if adv.get("click_price_auc", 0) > 0:
                        wb_row_data["цена клика"] = adv["click_price_auc"]
                    # Если нет АУКЦ, используем АРК
                    elif adv.get("click_price_ark", 0) > 0:
                        wb_row_data["цена клика"] = adv["click_price_ark"]
                    # Если нет сохраненных цен, вычисляем из расхода АУКЦ
                    elif adv["cost_auc"] > 0:
                        wb_row_data["цена клика"] = adv["cost_auc"] / adv["clicks"]
                    # Если нет расхода АУКЦ, вычисляем из расхода АРК
                    elif adv["cost_ark"] > 0:
                        wb_row_data["цена клика"] = adv["cost_ark"] / adv["clicks"]
                
                wb_row_data["расход АУКЦ"] = adv["cost_auc"]
                wb_row_data["расход АРК"] = adv["cost_ark"]
            
            # Добавляем данные воронки продаж
            if today_str in funnel_by_date:
                fun = funnel_by_date[today_str]
                wb_row_data["перешли в карточку"] = fun["card_views"]
                wb_row_data["корзин"] = fun["baskets"]
                # Заказы уже есть из sales_data, но можем обновить из воронки
                if fun["orders"] > 0:
                    wb_row_data["заказы"] = fun["orders"]
            
            # Добавляем данные в таблицу (всегда новая строка, так как файл новый)
            excel_handler.add_daily_data(today_str, wb_row_data)
            
            new_count = 1
            processed_count = len(date_data["sales"])
            details = [f"📅 {today_str}: {len(date_data['sales'])} продаж, {len(date_data['articles'])} артикулов, сумма: {date_data['total_price']:.2f} руб."]
            
            # Формируем детальный отчет
            report_lines = [
                f"✅ Парсинг завершен!",
                f"",
                f"📊 Статистика:",
                f"• Всего обработано: {processed_count}",
                f"• Добавлено записей: {new_count}",
                f"",
                f"📁 Файл: {excel_file_path}"
            ]
            
            # Добавляем детали (первые 10 записей)
            if details:
                report_lines.append(f"")
                report_lines.append(f"📋 Детали (первые 10):")
                for detail in details[:10]:
                    report_lines.append(detail)
                if len(details) > 10:
                    report_lines.append(f"... и еще {len(details) - 10} записей")
            
            # Отправляем текстовый отчет
            await self._safe_reply(update, "\n".join(report_lines))
            
            # Отправляем Excel файл
            if os.path.exists(excel_file_path) and os.path.getsize(excel_file_path) > 0:
                try:
                    logger.info(f"Отправка файла {excel_file_path} в Telegram (размер: {os.path.getsize(excel_file_path)} байт)")
                    # Используем _safe_reply для отправки файла с повторными попытками
                    max_attempts = 3
                    for attempt in range(max_attempts):
                        try:
                            with open(excel_file_path, 'rb') as file:
                                await update.message.reply_document(
                                    document=file,
                                    filename="wb_data.xlsx",
                                    caption="📊 Таблица с данными Wildberries"
                                )
                            logger.info("Файл успешно отправлен")
                            break
                        except (TimedOut, NetworkError, TelegramError) as e:
                            if attempt < max_attempts - 1:
                                wait_time = (2 ** attempt) * 2
                                logger.warning(f"Ошибка отправки файла (попытка {attempt + 1}/{max_attempts}): {str(e)}. Повтор через {wait_time} сек")
                                await asyncio.sleep(wait_time)
                                continue
                            else:
                                raise
                except Exception as e:
                    logger.error(f"Ошибка при отправке файла: {str(e)}", exc_info=True)
                    await self._safe_reply(update, f"⚠️ Не удалось отправить файл: {str(e)}")
            else:
                logger.warning(f"Файл {excel_file_path} не существует или пустой")
                await self._safe_reply(update, "⚠️ Файл не найден или пустой")
            
        except ValueError as e:
            await self._safe_reply(update, f"❌ Ошибка формата даты: {str(e)}")
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
            await self._safe_reply(update, error_msg)
        except requests.exceptions.Timeout:
            error_msg = (
                "⏱️ Превышено время ожидания ответа от API.\n"
                "Попробуйте позже или проверьте интернет-соединение."
            )
            logger.error("Таймаут при запросе к API", exc_info=True)
            await self._safe_reply(update, error_msg)
        except (TimedOut, NetworkError) as e:
            error_msg = (
                "⏱️ Проблемы с соединением к Telegram API.\n"
                "Попробуйте позже."
            )
            logger.error(f"Ошибка соединения с Telegram: {str(e)}", exc_info=True)
            # Не пытаемся отправить сообщение, так как соединение не работает
            logger.error("Не удалось отправить сообщение об ошибке пользователю")
        except Exception as e:
            logger.error(f"Ошибка при парсинге: {str(e)}", exc_info=True)
            try:
                await self._safe_reply(
                    update,
                    f"❌ Произошла ошибка: {str(e)}\n\n"
                    f"Проверьте логи для подробностей."
                )
            except Exception as send_error:
                logger.error(f"Не удалось отправить сообщение об ошибке: {str(send_error)}")
    
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

