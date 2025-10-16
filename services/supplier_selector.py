# services/supplier_selector.py
from database.db_connection import Database

class SupplierSelector:
    def __init__(self):
        self.db = Database()
        self._ensure_demo_suppliers()

    def _ensure_demo_suppliers(self):
        """Создает таблицу и демо-данные поставщиков (однократно при старте)"""
        conn = self.db.get_connection()
        cursor = conn.cursor()

        # ✅ Создаём таблицу, если её ещё нет
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

        # ✅ Проверяем, есть ли уже данные
        cursor.execute("SELECT COUNT(*) FROM suppliers")
        count = cursor.fetchone()[0]

        # ✅ Если таблица пустая — вставляем демо-данные
        if count == 0:
            demo_suppliers = [
                ("ТОО КазСтройПром", "строительные работы", "конкурс", 4.8, 122, 1200000000),
                ("ТОО ЭнергоСнаб", "электрооборудование", "аукцион", 4.6, 98, 900000000),
                ("ТОО МедСнаб", "медицинское оборудование", "запрос ценовых предложений", 4.7, 150, 1050000000),
                ("ТОО СофтЛайн", "IT-услуги", "аукцион", 4.9, 75, 850000000),
                ("ТОО ТрансЛогистик", "транспортные услуги", "аукцион", 4.5, 110, 970000000),
                ("ТОО КазХим", "химические реагенты", "конкурс", 4.4, 85, 800000000),
                ("ТОО CleanCity", "уборочные услуги", "запрос ценовых предложений", 4.2, 120, 450000000),
                ("ТОО АстанаСтрой", "строительные работы", "аукцион", 4.9, 130, 1500000000),
                ("ТОО AgroLine", "сельхозпродукция", "конкурс", 4.3, 115, 770000000),
                ("ТОО MegaFood", "поставка продуктов", "аукцион", 4.6, 160, 1300000000),
                ("ТОО PrintLab", "типографские услуги", "запрос ценовых предложений", 4.1, 80, 250000000),
                ("ТОО SmartEnergy", "энергетические услуги", "аукцион", 4.8, 95, 1100000000),
                ("ТОО SafeNet", "IT-услуги", "конкурс", 4.7, 105, 950000000),
                ("ТОО MedFarm", "медицинское оборудование", "аукцион", 4.9, 130, 1400000000),
                ("ТОО ТехСнаб", "промышленное оборудование", "конкурс", 4.5, 90, 880000000),
                ("ТОО ЭкспертПоставка", "оборудование", "аукцион", 4.6, 102, 960000000),
                ("ТОО GreenBuild", "строительные работы", "аукцион", 4.7, 85, 650000000),
                ("ТОО МедТехСнаб", "медицинское оборудование", "конкурс", 4.9, 140, 1550000000),
                ("ТОО ITProService", "IT-услуги", "аукцион", 4.8, 112, 1240000000),
                ("ТОО ЭкоЧист", "уборочные услуги", "запрос ценовых предложений", 4.4, 88, 500000000),
                ("ТОО AgroExport", "сельхозпродукция", "конкурс", 4.5, 145, 990000000),
                ("ТОО AutoLogistic", "транспортные услуги", "аукцион", 4.7, 155, 1360000000),
                ("ТОО CityPrint", "типографские услуги", "запрос ценовых предложений", 4.3, 92, 400000000),
                ("ТОО TechSystems", "электрооборудование", "конкурс", 4.8, 108, 1020000000),
                ("ТОО PowerEnergy", "энергетические услуги", "аукцион", 4.9, 145, 1490000000),
            ]

            cursor.executemany('''
                INSERT INTO suppliers (name, category, purchase_method, rating, contracts_count, total_sum)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', demo_suppliers)
            conn.commit()
            print(f"✅ Добавлено {len(demo_suppliers)} поставщиков в базу данных.")

        cursor.close()
        conn.close()

    def get_top_suppliers(self, purchase_method: str, category: str, limit: int = 5):
        """Возвращает ТОП-N поставщиков по заданным параметрам (без учёта регистра)"""
        conn = self.db.get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT name, category, purchase_method, rating, contracts_count, total_sum
            FROM suppliers
            WHERE LOWER(purchase_method) LIKE LOWER(?) AND LOWER(category) LIKE LOWER(?)
            ORDER BY rating DESC, total_sum DESC
            LIMIT ?
        ''', (f"%{purchase_method}%", f"%{category}%", limit))

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        return [dict(row) for row in rows]

    # ✅ НОВЫЕ МЕТОДЫ ДЛЯ ВЫПАДАЮЩИХ СПИСКОВ
    def get_all_purchase_methods(self):
        """Возвращает все уникальные способы закупок"""
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

        return methods

    def get_all_categories(self):
        """Возвращает все уникальные категории"""
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

        return categories

    def search_categories(self, query: str):
        """Поиск категорий по частичному совпадению"""
        conn = self.db.get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT DISTINCT category 
            FROM suppliers 
            WHERE LOWER(category) LIKE LOWER(?)
            ORDER BY category
            LIMIT 10
        ''', (f"%{query}%",))

        categories = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()

        return categories

    def search_purchase_methods(self, query: str):
        """Поиск способов закупки по частичному совпадению"""
        conn = self.db.get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT DISTINCT purchase_method 
            FROM suppliers 
            WHERE LOWER(purchase_method) LIKE LOWER(?)
            ORDER BY purchase_method
            LIMIT 10
        ''', (f"%{query}%",))

        methods = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()

        return methods