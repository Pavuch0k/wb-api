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
        
        self.headers = {
            "Authorization": self.api_key,
            "Content-Type": "application/json"
        }
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None, max_retries: int = 5) -> Dict:
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
                    wait_time = retry_after if attempt < max_retries - 1 else retry_after
                    
                    logger.warning(
                        f"Получен код 429 (Too Many Requests). "
                        f"Ожидание {wait_time} секунд перед повторной попыткой "
                        f"({attempt + 1}/{max_retries})"
                    )
                    
                    if attempt < max_retries - 1:
                        time.sleep(wait_time)
                        continue
                    else:
                        response.raise_for_status()
                
                # Обработка других ошибок
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.Timeout:
                wait_time = (2 ** attempt) * 5  # Экспоненциальная задержка: 5, 10, 20, 40, 80 сек
                if attempt < max_retries - 1:
                    logger.warning(f"Таймаут запроса. Повтор через {wait_time} сек ({attempt + 1}/{max_retries})")
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
    
    def get_stocks(self) -> List[Dict]:
        """
        Получает информацию об остатках товаров
        
        Returns:
            Список словарей с информацией об остатках
        """
        return self._make_request("stocks")
    
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

