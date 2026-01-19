"""Клиент для работы с API Wildberries"""
import requests
import os
import time
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class WBAPIClient:
    """Клиент для взаимодействия с API Wildberries"""
    
    BASE_URL = "https://statistics-api.wildberries.ru/api/v1/supplier"
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Инициализация клиента
        
        Args:
            api_key: API ключ Wildberries. Если не указан, берется из переменной окружения WB_API_KEY
        """
        self.api_key = api_key or os.getenv("WB_API_KEY")
        if not self.api_key:
            raise ValueError("API ключ не найден. Укажите его в параметре или в переменной окружения WB_API_KEY")
        
        # WB API использует токен напрямую в заголовке Authorization
        self.headers = {
            "Authorization": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None, max_retries: int = 5) -> List[Dict]:
        """
        Выполняет запрос к API с повторными попытками при ошибках
        
        Args:
            endpoint: Конечная точка API
            params: Параметры запроса
            max_retries: Максимальное количество повторных попыток
            
        Returns:
            Ответ от API в виде словаря
        """
        url = f"{self.BASE_URL}/{endpoint}"
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.headers, params=params, timeout=30)
                
                # Обработка ошибки 429 (Too Many Requests)
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    
                    logger.warning(
                        f"Получен код 429 (Too Many Requests). "
                        f"Ожидание {retry_after} секунд перед повторной попыткой "
                        f"({attempt + 1}/{max_retries})"
                    )
                    
                    if attempt < max_retries - 1:
                        time.sleep(retry_after)
                        continue
                    else:
                        # Если все попытки исчерпаны, выбрасываем исключение
                        raise requests.exceptions.HTTPError(
                            f"429 Client Error: Too Many Requests после {max_retries} попыток. "
                            f"Попробуйте позже.",
                            response=response
                        )
                
                # Обработка ошибки 400 (Bad Request) - не повторяем, сразу выбрасываем
                if response.status_code == 400:
                    error_text = response.text
                    logger.error(f"400 Bad Request для {endpoint}: {error_text}")
                    raise requests.exceptions.HTTPError(
                        f"400 Bad Request: {error_text}",
                        response=response
                    )
                
                # Обработка других ошибок
                response.raise_for_status()
                
                # WB API возвращает список или словарь
                result = response.json()
                if isinstance(result, list):
                    return result
                elif isinstance(result, dict):
                    return [result]
                else:
                    return []
                
            except requests.exceptions.Timeout:
                wait_time = (2 ** attempt) * 5  # Экспоненциальная задержка: 5, 10, 20, 40, 80 сек
                if attempt < max_retries - 1:
                    logger.warning(f"Таймаут запроса. Повтор через {wait_time} сек ({attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    raise
            
            except requests.exceptions.HTTPError as e:
                # Ошибки 400 и 401 не повторяем
                if e.response.status_code in (400, 401, 403):
                    raise
                # Для других HTTP ошибок повторяем
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) * 5
                    logger.warning(f"HTTP ошибка {e.response.status_code}: {str(e)}. Повтор через {wait_time} сек ({attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    raise
            
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) * 5
                    logger.warning(f"Ошибка запроса: {str(e)}. Повтор через {wait_time} сек ({attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    raise
        
        # Если все попытки исчерпаны
        raise requests.exceptions.RequestException(f"Не удалось выполнить запрос после {max_retries} попыток")
    
    def get_statistics(self, date_from: str, date_to: str) -> List[Dict]:
        """
        Получает статистику продаж за период
        
        Args:
            date_from: Дата начала периода (формат: YYYY-MM-DD)
            date_to: Дата окончания периода (формат: YYYY-MM-DD)
            
        Returns:
            Список словарей со статистикой
        """
        params = {
            "dateFrom": date_from,
            "dateTo": date_to
        }
        return self._make_request("sales", params)
    
    def ping(self) -> Dict:
        """
        Проверяет подключение и валидность токена
        
        Returns:
            Словарь с информацией о статусе подключения
        """
        url = f"{self.BASE_URL.replace('/supplier', '')}/ping"
        response = requests.get(url, headers=self.headers, timeout=10)
        response.raise_for_status()
        return response.json()
    
    def get_stocks(self, date_from: Optional[str] = None) -> List[Dict]:
        """
        Получает информацию об остатках товаров
        
        Args:
            date_from: Опционально - дата начала периода (формат: YYYY-MM-DD)
                      Если не указана, возвращаются текущие остатки
        
        Returns:
            Список словарей с информацией об остатках
        """
        params = None
        if date_from:
            params = {"dateFrom": date_from}
        return self._make_request("stocks", params)
    
    def get_orders(self, date_from: str, date_to: str) -> List[Dict]:
        """
        Получает информацию о заказах за период
        
        Args:
            date_from: Дата начала периода (формат: YYYY-MM-DD)
            date_to: Дата окончания периода (формат: YYYY-MM-DD)
            
        Returns:
            Список словарей с информацией о заказах
        """
        params = {
            "dateFrom": date_from,
            "dateTo": date_to
        }
        return self._make_request("orders", params)
    
    def get_advert_statistics(self, date_from: str, date_to: str) -> List[Dict]:
        """
        Получает статистику по рекламе за период
        
        Args:
            date_from: Дата начала периода (формат: YYYY-MM-DD)
            date_to: Дата окончания периода (формат: YYYY-MM-DD)
            
        Returns:
            Список словарей со статистикой рекламы
        """
        params = {
            "dateFrom": date_from,
            "dateTo": date_to
        }
        return self._make_request("advert", params)

