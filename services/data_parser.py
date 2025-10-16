# services/data_parser.py
import requests
from bs4 import BeautifulSoup
import logging
from database.db_connection import Database
import time
import random
import urllib.parse

logger = logging.getLogger(__name__)


class GosZakupParser:
    def __init__(self):
        self.db = Database()
        self.base_url = "https://www.goszakup.gov.kz"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    def parse_all_suppliers(self):
        """Парсит поставщиков для ВСЕХ комбинаций способов закупки и видов предметов"""
        all_purchase_methods = self.get_available_purchase_methods()
        all_subject_types = self.get_available_subject_types()

        all_suppliers = []

        for purchase_method in all_purchase_methods:
            for subject_type in all_subject_types:
                try:
                    logger.info(f"🕸️ Парсинг для {purchase_method} / {subject_type}")
                    suppliers = self.parse_suppliers_by_params(purchase_method, subject_type)
                    all_suppliers.extend(suppliers)

                    # Пауза между запросами чтобы не перегружать сервер
                    time.sleep(2)

                except Exception as e:
                    logger.error(f"❌ Ошибка парсинга для {purchase_method}/{subject_type}: {e}")
                    continue

        logger.info(f"✅ Всего спаршено {len(all_suppliers)} поставщиков")
        return all_suppliers

    def parse_suppliers_by_params(self, purchase_method, subject_type):
        """Парсит поставщиков по конкретному способу закупки и виду предмета"""
        try:
            # Кодируем параметры для URL
            encoded_method = urllib.parse.quote(purchase_method)
            encoded_subject = urllib.parse.quote(subject_type)

            # Формируем URL с параметрами
            url = f"https://www.goszakup.gov.kz/ru/top/suppliers?purchase_method={encoded_method}&subject_type={encoded_subject}&sort=count"

            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            # Ищем таблицу с поставщиками
            table = soup.find('table')
            if not table:
                logger.warning(f"⚠️ Таблица не найдена для {purchase_method}/{subject_type}")
                return []

            suppliers = []
            rows = table.find_all('tr')[1:]  # Пропускаем заголовок

            for i, row in enumerate(rows):
                cols = row.find_all('td')
                if len(cols) >= 5:
                    try:
                        supplier_data = self._parse_supplier_row(cols, i)
                        if supplier_data:
                            # Добавляем параметры из запроса
                            supplier_data['purchase_method'] = purchase_method
                            supplier_data['category'] = subject_type
                            suppliers.append(supplier_data)
                    except Exception as e:
                        logger.warning(f"⚠️ Ошибка парсинга строки: {e}")
                        continue

            logger.info(f"✅ Спаршено {len(suppliers)} поставщиков для {purchase_method} / {subject_type}")
            return suppliers

        except Exception as e:
            logger.error(f"❌ Ошибка парсинга {purchase_method}/{subject_type}: {e}")
            return []

    def _parse_supplier_row(self, cols, position):
        """Парсит строку с данными поставщика"""
        try:
            # Место в рейтинге
            rank = position + 1

            # Название поставщика
            name_elem = cols[1].find('a') or cols[1]
            name = name_elem.get_text(strip=True)

            # БИН
            bin_number = cols[2].get_text(strip=True)

            # Количество контрактов
            contracts_text = cols[3].get_text(strip=True).replace(' ', '')
            contracts_count = int(contracts_text) if contracts_text else 0

            # Сумма контрактов
            sum_text = cols[4].get_text(strip=True)
            total_sum = self._parse_sum(sum_text)

            if not name:
                return None

            return {
                'name': name,
                'bin': bin_number,
                'contracts_count': contracts_count,
                'total_sum': total_sum,
                'rank': rank
            }

        except Exception as e:
            logger.warning(f"⚠️ Ошибка парсинга строки поставщика: {e}")
            return None

    def _parse_sum(self, sum_text):
        """Парсит сумму из текста"""
        try:
            if not sum_text:
                return 0

            # Удаляем пробелы и преобразуем в число
            clean_text = sum_text.replace(' ', '').replace('₸', '').replace(',', '.').strip()

            if 'млн' in clean_text.lower():
                number = float(clean_text.lower().replace('млн', '').strip())
                return int(number * 1000000)
            elif 'млрд' in clean_text.lower():
                number = float(clean_text.lower().replace('млрд', '').strip())
                return int(number * 1000000000)
            else:
                return int(float(clean_text))

        except Exception as e:
            logger.warning(f"⚠️ Ошибка парсинга суммы '{sum_text}': {e}")
            return 0

    def update_all_suppliers(self):
        """Обновляет ВСЕХ поставщиков в базе данных"""
        try:
            # Парсим всех поставщиков
            all_suppliers = self.parse_all_suppliers()

            if not all_suppliers:
                logger.error("❌ Не удалось получить данные с сайта")
                return False

            conn = self.db.get_connection()
            cursor = conn.cursor()

            # Очищаем таблицу перед добавлением новых данных
            cursor.execute("DELETE FROM suppliers")

            # Добавляем ВСЕХ поставщиков с уникальными рейтингами
            for supplier in all_suppliers:
                rating = self._calculate_unique_rating(
                    supplier['rank'],
                    supplier['contracts_count'],
                    supplier['total_sum']
                )

                cursor.execute('''
                    INSERT INTO suppliers 
                    (name, category, purchase_method, rating, contracts_count, total_sum)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    supplier['name'],
                    supplier['category'],
                    supplier['purchase_method'],
                    rating,
                    supplier['contracts_count'],
                    supplier['total_sum']
                ))

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"✅ База данных полностью обновлена. Добавлено {len(all_suppliers)} поставщиков")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка обновления базы данных: {e}")
            return False

    def _calculate_unique_rating(self, rank, contracts_count, total_sum):
        """Рассчитывает УНИКАЛЬНЫЙ рейтинг"""
        try:
            # Базовый рейтинг от 4.0 до 5.0 в зависимости от позиции
            position_factor = max(0, 1.0 - (rank / 100))
            base_rating = 4.0 + (position_factor * 1.0)

            # Бонус за количество контрактов
            contracts_bonus = min(contracts_count / 100000, 0.3)

            # Бонус за сумму контрактов
            sum_bonus = min(total_sum / 100000000000, 0.2)

            # Случайная компонента для уникальности
            random_component = (random.random() - 0.5) * 0.2

            rating = base_rating + contracts_bonus + sum_bonus + random_component
            rating = max(4.0, min(rating, 5.0))

            return round(rating, 1)

        except:
            return round(4.0 + random.random() * 0.5, 1)

    def get_available_purchase_methods(self):
        """Возвращает доступные способы закупок"""
        return [
            "Из одного источника путем прямого заключения договора",
            "Через товарные биржи",
            "Открытый конкурс",
            "Запрос ценовых предложений",
            "Из одного источника по несостоявшимся закупкам",
            "Аукцион (до 2022)",
            "Закупка жилища",
            "Электронный магазин",
            "Второй этап конкурса с использованием рамочного соглашения",
            "Из одного источника путем прямого заключения договора по питанию обучающихся",
            "Аукцион (с 2022)",
            "Договор на услуги государственного образовательного заказа",
            "Конкурс с использованием рейтингово-балльной системы",
            "Конкурс по строительству «под ключ»",
            "Конкурс с предварительным квалификационным отбором",
            "Закупка по государственному социальному заказу",
            "Конкурс с применением специального порядка",
            "Конкурс по приобретению услуг по организации питания воспитанников и обучающихся",
            "Конкурс по приобретению товаров, связанных с обеспечением питания воспитанников и обучающихся"
        ]

    def get_available_subject_types(self):
        """Возвращает доступные виды предметов закупок"""
        return ["Товар", "Работа", "Услуга"]