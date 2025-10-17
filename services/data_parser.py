# services/data_parser.py
import requests
from bs4 import BeautifulSoup
import logging
from database.db_connection import Database
import time
import random
import urllib.parse
from typing import List, Dict
import re

logger = logging.getLogger(__name__)


class GosZakupParser:
    def __init__(self):
        self.db = Database()
        self.base_url = "https://www.goszakup.gov.kz"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        })

    def parse_real_time_suppliers(self, purchase_method: str, category: str, limit: int = 20) -> List[Dict]:
        """Парсит актуальных поставщиков в реальном времени с сайта - ОБНОВЛЕННАЯ ВЕРСИЯ"""
        try:
            logger.info(f"🔍 Парсим актуальных поставщиков для {purchase_method} / {category}")

            url = self._build_url(purchase_method, category)

            logger.info(f"🌐 Запрос к: {url}")

            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            if response.status_code != 200:
                logger.error(f"❌ Ошибка HTTP: {response.status_code}")
                return []

            soup = BeautifulSoup(response.content, 'html.parser')

            with open('debug_table.html', 'w', encoding='utf-8') as f:
                f.write(soup.prettify())
            logger.info("💾 Сохранен отладочный HTML в debug_table.html")

            suppliers_table = soup.find('table', class_='table')
            if not suppliers_table:
                logger.warning("⚠️ Таблица поставщиков не найдена на странице")
                all_tables = soup.find_all('table')
                logger.info(f"📊 Найдено таблиц на странице: {len(all_tables)}")
                for i, table in enumerate(all_tables):
                    logger.info(f"📋 Таблица {i}: {len(table.find_all('tr'))} строк")

                if all_tables:
                    suppliers_table = all_tables[0]
                else:
                    return []

            rows = suppliers_table.find_all('tr')
            logger.info(f"📊 Найдено строк в таблице: {len(rows)}")

            if rows:
                first_row = rows[0]
                headers = first_row.find_all('th')
                logger.info(f"📝 Заголовки таблицы: {[h.get_text(strip=True) for h in headers]}")

                if len(rows) > 1:
                    sample_row = rows[1]
                    cells = sample_row.find_all('td')
                    logger.info(f"🔍 Пример данных строки: {[cell.get_text(strip=True) for cell in cells]}")

            suppliers = self._parse_suppliers_table(suppliers_table, purchase_method, category, limit)
            logger.info(f"✅ Спаршено {len(suppliers)} актуальных поставщиков")

            for i, supplier in enumerate(suppliers):
                logger.info(f"🏢 Поставщик {i + 1}: {supplier['name']}")

            return suppliers

        except requests.RequestException as e:
            logger.error(f"❌ Ошибка сети: {e}")
            return []
        except Exception as e:
            logger.error(f"❌ Неожиданная ошибка при парсинге: {e}")
            return []

    def _build_url(self, purchase_method: str, category: str) -> str:
        """Строит URL в зависимости от способа закупки и категории"""
        base_url = "https://www.goszakup.gov.kz/ru/top/suppliers"

        if purchase_method == "Из одного источника путем прямого заключения договора":
            if category == "Товар":
                return f"{base_url}?filter%5Bmethod%5D%5B%5D=23&filter%5Bref_subject_type%5D=1&smb="
            elif category == "Работа":
                return f"{base_url}?filter%5Bmethod%5D%5B%5D=23&filter%5Bref_subject_type%5D=2&smb="
            elif category == "Услуга":
                return f"{base_url}?filter%5Bmethod%5D%5B%5D=23&filter%5Bref_subject_type%5D=3&smb="

        elif purchase_method == "Через товарные биржи":
            if category == "Товар":
                return f"{base_url}?filter%5Bmethod%5D%5B%5D=8&filter%5Bref_subject_type%5D=1&smb="
            elif category == "Работа":
                return f"{base_url}?filter%5Bmethod%5D%5B%5D=8&filter%5Bref_subject_type%5D=2&smb="
            elif category == "Услуга":
                return f"{base_url}?filter%5Bmethod%5D%5B%5D=8&filter%5Bref_subject_type%5D=3&smb="

        elif purchase_method == "Открытый конкурс":
            if category == "Товар":
                return f"{base_url}?filter%5Bmethod%5D%5B%5D=2&filter%5Bref_subject_type%5D=1&smb="
            elif category == "Работа":
                return f"{base_url}?filter%5Bmethod%5D%5B%5D=2&filter%5Bref_subject_type%5D=2&smb="
            elif category == "Услуга":
                return f"{base_url}?filter%5Bmethod%5D%5B%5D=2&filter%5Bref_subject_type%5D=3&smb="

        encoded_method = urllib.parse.quote(purchase_method)
        encoded_category = urllib.parse.quote(category)
        return f"{self.base_url}/ru/top/suppliers?purchase_method={encoded_method}&subject_type={encoded_category}&sort=count"

    def _parse_suppliers_table(self, table, purchase_method: str, category: str, limit: int) -> List[Dict]:
        """Парсит таблицу с поставщиками"""
        suppliers = []
        rows = table.find_all('tr')[1:]

        for i, row in enumerate(rows[:limit]):
            try:
                cols = row.find_all('td')
                if len(cols) >= 3:
                    supplier_data = self._parse_supplier_row(cols, i + 1)
                    if supplier_data:
                        supplier_data.update({
                            'purchase_method': purchase_method,
                            'category': category,
                            'is_real_time': True
                        })
                        suppliers.append(supplier_data)
            except Exception as e:
                logger.warning(f"⚠️ Ошибка парсинга строки {i}: {e}")
                continue

        return suppliers

    def _parse_supplier_row(self, cols, rank: int) -> Dict:
        """Парсит строку с данными поставщика - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
        try:
            if len(cols) < 3:
                return None

            name = self._extract_supplier_name_advanced(cols)

            if not name or name == '-' or len(name) < 2:
                return None

            contracts_elem = cols[3] if len(cols) > 3 else None
            contracts_text = contracts_elem.get_text(strip=True).replace(' ', '') if contracts_elem else "0"
            contracts_count = self._parse_number(contracts_text)

            sum_elem = cols[2] if len(cols) > 2 else None
            sum_text = sum_elem.get_text(strip=True) if sum_elem else "0"
            total_sum = self._parse_sum(sum_text)

            rating = self._calculate_dynamic_rating(rank, contracts_count, total_sum)

            return {
                'name': name,
                'contracts_count': contracts_count,
                'total_sum': total_sum,
                'rating': rating,
                'rank': rank
            }

        except Exception as e:
            logger.warning(f"⚠️ Ошибка парсинга строки поставщика: {e}")
            return None

    def _extract_supplier_name_advanced(self, cols):
        """Расширенное извлечение названия поставщика"""
        try:
            possible_name_columns = [0, 1]

            for col_index in possible_name_columns:
                if col_index < len(cols):
                    name_cell = cols[col_index]

                    link = name_cell.find('a')
                    if link and link.get_text(strip=True):
                        name = link.get_text(strip=True)
                        if self._is_valid_supplier_name(name):
                            return self._clean_supplier_name(name)

                    name = name_cell.get_text(strip=True)
                    if self._is_valid_supplier_name(name):
                        return self._clean_supplier_name(name)

                    for tag in ['span', 'div', 'strong', 'b']:
                        element = name_cell.find(tag)
                        if element and element.get_text(strip=True):
                            name = element.get_text(strip=True)
                            if self._is_valid_supplier_name(name):
                                return self._clean_supplier_name(name)

            return None

        except Exception as e:
            logger.warning(f"⚠️ Ошибка расширенного извлечения названия: {e}")
            return None

    def _is_valid_supplier_name(self, name: str) -> bool:
        """Проверяет, что извлеченный текст похож на название компании"""
        if not name or len(name) < 2:
            return False

        invalid_patterns = [
            r'^\d+$',
            r'^\d+\.\d+$',
            r'^[\d\s,\.]+$',
            r'рейтинг', r'rating', r'ранг',
        ]

        for pattern in invalid_patterns:
            if re.search(pattern, name.lower()):
                return False

        return True

    def _clean_supplier_name(self, name: str) -> str:
        """Очищает название поставщика"""
        # FOR БИН ИНН/ ЕСЛИ удалить может сломаться решение неизвестно
        clean_name = re.sub(r'БИН:\s*\d+', '', name, flags=re.IGNORECASE)
        clean_name = re.sub(r'ИИН:\s*\d+', '', clean_name, flags=re.IGNORECASE)


        clean_name = re.sub(r'\s+', ' ', clean_name).strip()
        clean_name = clean_name.strip(',-–— ')

        return clean_name

    def _find_contracts_count(self, cols) -> int:
        """Находит количество контрактов в разных колонках"""

        possible_columns = [2, 3, 1]

        for col_index in possible_columns:
            if col_index < len(cols):
                text = cols[col_index].get_text(strip=True)
                count = self._parse_number(text)
                if count > 0:
                    return count

        return 0

    def _find_total_sum(self, cols) -> float:
        """Находит общую сумму в разных колонках"""
        possible_columns = [3, 4, 2]

        for col_index in possible_columns:
            if col_index < len(cols):
                text = cols[col_index].get_text(strip=True)
                total_sum = self._parse_sum(text)
                if total_sum > 0:
                    return total_sum

        return 0.0

    def _parse_number(self, text: str) -> int:
        """Парсит число из текста"""
        try:
            clean_text = re.sub(r'[^\d]', '', text)
            return int(clean_text) if clean_text else 0
        except:
            return 0

    def _parse_sum(self, text: str) -> float:
        """Парсит денежную сумму из текста"""
        try:
            if not text:
                return 0.0

            clean_text = text.replace(' ', '').replace(',', '.')

            if 'млрд' in clean_text.lower():
                number = float(re.sub(r'[^\d.]', '', clean_text))
                return number * 1000000000
            elif 'млн' in clean_text.lower():
                number = float(re.sub(r'[^\d.]', '', clean_text))
                return number * 1000000
            else:
                return float(re.sub(r'[^\d.]', '', clean_text))

        except Exception as e:
            logger.warning(f"⚠️ Ошибка парсинга суммы '{text}': {e}")
            return 0.0

    def _calculate_dynamic_rating(self, rank: int, contracts_count: int, total_sum: float) -> float:
        """Рассчитывает динамический рейтинг"""
        try:
            base_rating = 5.0 - (rank * 0.1)
            contracts_bonus = min(contracts_count / 1000, 0.5)
            sum_bonus = min(total_sum / 1000000000, 0.3)
            random_component = random.uniform(-0.2, 0.2)

            rating = base_rating + contracts_bonus + sum_bonus + random_component
            rating = max(3.0, min(rating, 5.0))

            return round(rating, 1)

        except:
            return round(4.0 + random.random(), 1)

    def get_available_purchase_methods(self) -> List[str]:
        """Возвращает доступные способы закупок"""
        return [
            "Из одного источника путем прямого заключения договора",
            "Через товарные биржи",
            "Открытый конкурс"
        ]

    def get_available_categories(self) -> List[str]:
        """Возвращает доступные категории"""
        return ["Товар", "Работа", "Услуга"]