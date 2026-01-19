"""Модуль для работы с Excel таблицами"""
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
import os


class ExcelHandler:
    """Класс для работы с Excel файлами"""
    
    def __init__(self, file_path: str = "wb_data.xlsx"):
        """
        Инициализация обработчика Excel
        
        Args:
            file_path: Путь к файлу Excel
        """
        self.file_path = file_path
        self._ensure_file_exists()
    
    def _ensure_file_exists(self):
        """Создает файл Excel с базовой структурой, если его нет"""
        if not os.path.exists(self.file_path):
            # Создаем пустой DataFrame с базовыми колонками
            df = pd.DataFrame(columns=[
                "акк/дата",
                "ставка/реклама",
                "комментарий",
                "К месяц/Артикул",
                "Касса месяц",
                "К день",
                "Касса день",
                "показы",
                "клики",
                "CTR",
                "цена клика АУКЦ",
                "расход АУКЦ",
                "цена клика АРК",
                "расход АРК",
                "перешли в карточку",
                "корзин",
                "заказы",
                "хран день",
                "М с 1 шт наши данные",
                "итого прибыль",
                "ЦЕНА товара",
                "остаток товар"
            ])
            df.to_excel(self.file_path, index=False, engine='openpyxl')
    
    def read_data(self) -> pd.DataFrame:
        """
        Читает данные из Excel файла
        
        Returns:
            DataFrame с данными
        """
        return pd.read_excel(self.file_path, engine='openpyxl')
    
    def write_data(self, df: pd.DataFrame):
        """
        Записывает данные в Excel файл
        
        Args:
            df: DataFrame для записи
        """
        df.to_excel(self.file_path, index=False, engine='openpyxl')
    
    def add_daily_data(self, date: str, wb_data: Dict, manual_data: Optional[Dict] = None):
        """
        Добавляет дневные данные в таблицу
        
        Args:
            date: Дата в формате DD.MM.YYYY
            wb_data: Данные из API Wildberries
            manual_data: Данные для ручного ввода (зеленые поля)
        """
        df = self.read_data()
        
        # Подготавливаем данные для новой строки
        new_row = {
            "акк/дата": date,
            "ставка/реклама": manual_data.get("ставка/реклама", "") if manual_data else "",
            "комментарий": manual_data.get("комментарий", "") if manual_data else "",
            "К месяц/Артикул": manual_data.get("К месяц/Артикул", "") if manual_data else "",
            "Касса месяц": manual_data.get("Касса месяц", "") if manual_data else "",
            "К день": wb_data.get("К день", ""),
            "Касса день": wb_data.get("Касса день", ""),
            "показы": wb_data.get("показы", ""),
            "клики": wb_data.get("клики", ""),
            "CTR": wb_data.get("CTR", ""),
            "цена клика АУКЦ": wb_data.get("цена клика АУКЦ", ""),
            "расход АУКЦ": wb_data.get("расход АУКЦ", ""),
            "цена клика АРК": wb_data.get("цена клика АРК", ""),
            "расход АРК": wb_data.get("расход АРК", ""),
            "перешли в карточку": wb_data.get("перешли в карточку", ""),
            "корзин": wb_data.get("корзин", ""),
            "заказы": wb_data.get("заказы", ""),
            "хран день": wb_data.get("хран день", ""),
            "М с 1 шт наши данные": manual_data.get("М с 1 шт наши данные", "") if manual_data else "",
            "итого прибыль": wb_data.get("итого прибыль", ""),
            "ЦЕНА товара": wb_data.get("ЦЕНА товара", ""),
            "остаток товар": wb_data.get("остаток товар", "")
        }
        
        # Добавляем новую строку
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        self.write_data(df)
    
    def update_row(self, date: str, updates: Dict):
        """
        Обновляет строку с указанной датой
        
        Args:
            date: Дата для поиска строки
            updates: Словарь с обновлениями
        """
        df = self.read_data()
        mask = df["акк/дата"] == date
        if mask.any():
            for key, value in updates.items():
                if key in df.columns:
                    df.loc[mask, key] = value
            self.write_data(df)
        else:
            raise ValueError(f"Строка с датой {date} не найдена")

