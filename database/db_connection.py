import sqlite3
import os
from config import Config


class Database:
    def __init__(self):
        self.db_path = Config.SQLITE_DATABASE

    def get_connection(self):

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Чтобы результаты были как словари
        return conn

    def init_db(self):

        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # Таблица для статей законов
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS law_articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    law_type TEXT NOT NULL,
                    article_number TEXT NOT NULL,
                    title TEXT,
                    content TEXT,
                    keywords TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(law_type, article_number)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS contract_analysis (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contract_text TEXT,
                    law_type TEXT,
                    compliance_result TEXT,
                    issues_found TEXT,
                    recommendations TEXT,
                    analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
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

            conn.commit()
            print("✅ SQLite база данных инициализирована успешно")

        except Exception as e:
            print(f"❌ Ошибка инициализации БД: {e}")
            raise
        finally:
            cursor.close()
            conn.close()



if __name__ == "__main__":
    db = Database()
    db.init_db()