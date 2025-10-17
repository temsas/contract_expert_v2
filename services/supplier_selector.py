# services/supplier_selector.py
from database.db_connection import Database
import logging
from services.data_parser import GosZakupParser
from typing import List, Dict

logger = logging.getLogger(__name__)

class SupplierSelector:
    def __init__(self):
        self.db = Database()
        self.parser = GosZakupParser()
        self._ensure_suppliers_table()

    def _ensure_suppliers_table(self):
        """Создает таблицу поставщиков если её нет"""
        conn = self.db.get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS suppliers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                purchase_method TEXT NOT NULL,
                rating REAL NOT NULL,
                contracts_count INTEGER NOT NULL,
                total_sum REAL NOT NULL,
                is_real_time BOOLEAN DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.close()
        conn.close()

    def get_real_time_suppliers(self, purchase_method: str, category: str, limit: int = 20) -> List[Dict]:
        """Получает актуальных поставщиков в реальном времени"""
        try:
            logger.info(f"🕒 Получение реальных поставщиков для {purchase_method}/{category}")

            real_suppliers = self.parser.parse_real_time_suppliers(purchase_method, category, limit)

            if real_suppliers:
                logger.info(f"✅ Получено {len(real_suppliers)} актуальных поставщиков")
                return real_suppliers
            else:
                logger.warning("⚠️ Не удалось получить актуальных поставщиков, используем локальные данные")
                return self.get_cached_suppliers(purchase_method, category, limit)

        except Exception as e:
            logger.error(f"❌ Ошибка получения реальных поставщиков: {e}")
            return self.get_cached_suppliers(purchase_method, category, limit)

    def get_cached_suppliers(self, purchase_method: str, category: str, limit: int = 20) -> List[Dict]:
        """Получает поставщиков из локальной базы"""
        conn = self.db.get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT name, category, purchase_method, rating, contracts_count, total_sum
            FROM suppliers 
            WHERE purchase_method = ? AND category = ?
            ORDER BY rating DESC, contracts_count DESC
            LIMIT ?
        ''', (purchase_method, category, limit))

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        suppliers = []
        for row in rows:
            suppliers.append({
                'name': row[0],
                'category': row[1],
                'purchase_method': row[2],
                'rating': row[3],
                'contracts_count': row[4],
                'total_sum': row[5],
                'is_real_time': False
            })

        return suppliers

    def cache_suppliers(self, suppliers: List[Dict]):
        """Сохраняет поставщиков в локальную базу"""
        if not suppliers:
            return

        conn = self.db.get_connection()
        cursor = conn.cursor()

        try:
            for supplier in suppliers:
                cursor.execute('''
                    INSERT OR REPLACE INTO suppliers 
                    (name, category, purchase_method, rating, contracts_count, total_sum, is_real_time, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    supplier['name'],
                    supplier['category'],
                    supplier['purchase_method'],
                    supplier['rating'],
                    supplier['contracts_count'],
                    supplier['total_sum'],
                    supplier.get('is_real_time', False)
                ))

            conn.commit()
            logger.info(f"💾 Сохранено {len(suppliers)} поставщиков в кэш")

        except Exception as e:
            logger.error(f"❌ Ошибка сохранения в кэш: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

    def get_top_suppliers(self, purchase_method: str, category: str, limit: int = 20, use_cache: bool = True) -> List[Dict]:
        """Основной метод получения поставщиков"""
        real_suppliers = self.get_real_time_suppliers(purchase_method, category, limit)

        if real_suppliers:
            if use_cache:
                self.cache_suppliers(real_suppliers)
            return real_suppliers
        else:
            cached_suppliers = self.get_cached_suppliers(purchase_method, category, limit)
            if cached_suppliers:
                logger.info("📦 Используем кэшированных поставщиков")
                return cached_suppliers
            else:
                logger.warning("📭 Нет данных в кэше")
                return []

    def get_all_purchase_methods(self):
        """Возвращает все способы закупок"""
        return self.parser.get_available_purchase_methods()

    def get_all_categories(self):
        """Возвращает все категории"""
        return self.parser.get_available_categories()

    def search_categories(self, query: str):
        """Поиск категорий"""
        all_categories = self.get_all_categories()
        return [cat for cat in all_categories if query.lower() in cat.lower()][:10]

    def search_purchase_methods(self, query: str):
        """Поиск способов закупки"""
        all_methods = self.get_all_purchase_methods()
        return [method for method in all_methods if query.lower() in method.lower()][:10]

    def get_suppliers_stats(self):
        """Статистика поставщиков"""
        conn = self.db.get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT 
                COUNT(*) as total_suppliers,
                COUNT(DISTINCT purchase_method) as unique_methods,
                COUNT(DISTINCT category) as unique_categories
            FROM suppliers
        ''')

        stats = dict(cursor.fetchone())
        cursor.close()
        conn.close()


        stats['unique_methods'] = 3
        stats['unique_categories'] = 3

        return stats