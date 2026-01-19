"""Клиент для работы с API Wildberries"""
import requests
import os
from typing import Dict, List, Optional
from datetime import datetime, timedelta


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
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Выполняет запрос к API
        
        Args:
            endpoint: Конечная точка API
            params: Параметры запроса
            
        Returns:
            Ответ от API в виде словаря
        """
        url = f"{self.BASE_URL}/{endpoint}"
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()
    
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

