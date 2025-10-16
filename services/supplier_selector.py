# services/supplier_selector.py
from database.db_connection import Database
import logging

logger = logging.getLogger(__name__)


class SupplierSelector:
    def __init__(self):
        self.db = Database()
        self._ensure_suppliers_table()
        self._ensure_initial_data()

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
                total_sum REAL NOT NULL
            )
        ''')

        cursor.close()
        conn.close()

    def _ensure_initial_data(self):
        """Проверяет, есть ли данные в базе, если нет - загружает демо"""
        conn = self.db.get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM suppliers")
        count = cursor.fetchone()[0]

        if count == 0:
            logger.info("🔄 База поставщиков пуста, загружаем демо-данные...")
            self._load_demo_suppliers()
        else:
            logger.info(f"✅ В базе уже есть {count} поставщиков")

        cursor.close()
        conn.close()

    def _load_demo_suppliers(self):
        """Загружает демо-данные"""
        conn = self.db.get_connection()
        cursor = conn.cursor()

        demo_suppliers = [
            ("ТОО КазСтройПром", "Работа", "Из одного источника путем прямого заключения договора", 4.8, 122,
             1200000000),
            ("ТОО ЭнергоСнаб", "Товар", "Аукцион (с 2022)", 4.6, 98, 900000000),
            ("ТОО МедСнаб", "Услуга", "Запрос ценовых предложений", 4.7, 150, 1050000000),
            ("ТОО СофтЛайн", "Услуга", "Электронный магазин", 4.9, 75, 850000000),
            ("ТОО ТрансЛогистик", "Услуга", "Открытый конкурс", 4.5, 110, 970000000),
            ("ТОО КазХим", "Товар", "Конкурс с использованием рейтингово-балльной системы", 4.4, 85, 800000000),
            ("ТОО CleanCity", "Услуга", "Запрос ценовых предложений", 4.2, 120, 450000000),
            ("ТОО АстанаСтрой", "Работа", "Конкурс по строительству «под ключ»", 4.9, 130, 1500000000),
            ("ТОО AgroLine", "Товар", "Через товарные биржи", 4.3, 115, 770000000),
            ("ТОО MegaFood", "Товар", "Аукцион (с 2022)", 4.6, 160, 1300000000),
        ]

        cursor.executemany('''
            INSERT INTO suppliers (name, category, purchase_method, rating, contracts_count, total_sum)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', demo_suppliers)

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"✅ Загружено {len(demo_suppliers)} демо-поставщиков")

    def get_top_suppliers(self, purchase_method: str, category: str, limit: int = 50):
        """Возвращает ТОП-N поставщиков по заданным параметрам"""
        return self.get_filtered_suppliers(purchase_method, category, limit)

    def get_all_purchase_methods(self):
        """Возвращает все уникальные способы закупок из базы"""
        conn = self.db.get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT DISTINCT purchase_method 
            FROM suppliers 
            ORDER BY purchase_method
        ''')

        methods = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()

        # Если в базе мало методов, добавляем стандартные
        if len(methods) < 5:
            methods.extend([
                "Из одного источника путем прямого заключения договора",
                "Аукцион (с 2022)",
                "Запрос ценовых предложений",
                "Открытый конкурс",
                "Электронный магазин"
            ])
            methods = list(set(methods))  # Убираем дубликаты
            methods.sort()

        return methods

    def get_filtered_suppliers(self, purchase_method: str = None, category: str = None, limit: int = 50):
        """Возвращает поставщиков с фильтрацией (поддерживает частичные совпадения)"""
        conn = self.db.get_connection()
        cursor = conn.cursor()

        query = '''
            SELECT name, category, purchase_method, rating, contracts_count, total_sum
            FROM suppliers
            WHERE 1=1
        '''
        params = []

        if purchase_method:
            query += ' AND purchase_method LIKE ?'
            params.append(f'%{purchase_method}%')

        if category:
            query += ' AND category LIKE ?'
            params.append(f'%{category}%')

        query += ' ORDER BY rating DESC, contracts_count DESC, total_sum DESC LIMIT ?'
        params.append(limit)

        cursor.execute(query, params)
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
                'total_sum': row[5]
            })

        logger.info(f"🔍 Найдено {len(suppliers)} поставщиков для {purchase_method}/{category}")
        return suppliers



    def get_all_categories(self):
        """Возвращает все уникальные категории из базы"""
        conn = self.db.get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT DISTINCT category 
            FROM suppliers 
            ORDER BY category
        ''')

        categories = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()

        # Если в базе мало категорий, добавляем стандартные
        if len(categories) < 3:
            categories.extend(["Товар", "Работа", "Услуга"])
            categories = list(set(categories))
            categories.sort()

        return categories

    def search_categories(self, query: str):
        """Поиск категорий по частичному совпадению"""
        all_categories = self.get_all_categories()
        return [cat for cat in all_categories if query.lower() in cat.lower()][:10]

    def search_purchase_methods(self, query: str):
        """Поиск способов закупки по частичному совпадению"""
        all_methods = self.get_all_purchase_methods()
        return [method for method in all_methods if query.lower() in method.lower()][:10]

    def get_suppliers_stats(self):
        """Возвращает статистику по поставщикам"""
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

        return stats