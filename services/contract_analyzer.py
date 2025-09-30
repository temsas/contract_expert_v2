from database.db_connection import Database
from services.gigachat_service import GigaChatService
from utils.file_utils import FileProcessor
import logging

logger = logging.getLogger(__name__)


class ContractAnalyzer:
    def __init__(self):
        self.db = Database()
        try:
            self.gigachat = GigaChatService()
            self.gigachat_available = True
        except Exception as e:
            logger.error(f"❌ GigaChat недоступен: {e}")
            self.gigachat_available = False

    def extract_text_from_contract(self, file_path, filename):
        return FileProcessor.extract_text_from_file(file_path, filename)

    def get_law_articles(self, law_type):

        conn = self.db.get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT article_number, title, content
            FROM law_articles 
            WHERE law_type = ?
            ORDER BY CAST(article_number AS FLOAT)
        ''', (law_type,))

        articles = cursor.fetchall()
        cursor.close()
        conn.close()

        formatted_articles = f"ФЕДЕРАЛЬНЫЙ ЗАКОН {law_type.upper()}\n\n"

        for article in articles:
            formatted_articles += f"СТАТЬЯ {article[0]}\n"
            formatted_articles += f"Заголовок: {article[1]}\n"
            formatted_articles += f"Содержание: {article[2]}\n"
            formatted_articles += "---\n\n"

        return formatted_articles

    def analyze_contract(self, contract_text, law_type, filename):

        if not self.gigachat_available:
            return {
                "compliance_status": "ошибка",
                "issues": [{
                    "article": "системная ошибка",
                    "issue": "GigaChat недоступен",
                    "recommendation": "Проверьте настройки системы"
                }],
                "summary": "GigaChat не подключен"
            }


        law_articles = self.get_law_articles(law_type)

        if len(law_articles) < 50:
            return {
                "compliance_status": "ошибка",
                "issues": [],
                "summary": f"Недостаточно статей в базе данных для {law_type}"
            }


        analysis_result = self.gigachat.analyze_contract(contract_text, law_articles, law_type)


        self._save_analysis_result(contract_text, law_type, analysis_result, filename)

        return analysis_result

    def _save_analysis_result(self, contract_text, law_type, analysis_result, filename):

        conn = self.db.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                INSERT INTO contract_analysis 
                (contract_text, law_type, compliance_result, issues_found, recommendations)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                f"[{filename}] {contract_text[:500]}",
                law_type,
                analysis_result.get('compliance_status', 'не определен'),
                str(analysis_result.get('issues', [])),
                analysis_result.get('summary', '')[:500]
            ))
            conn.commit()
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения анализа: {e}")
        finally:
            cursor.close()
            conn.close()