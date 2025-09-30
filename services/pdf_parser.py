import PyPDF2
import re
from database.db_connection import Database


class LawParser:
    def __init__(self):
        self.db = Database()

    def parse_law_pdf(self, file_path, law_type):

        articles = []

        with open(file_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)

            full_text = ""
            for page in pdf_reader.pages:
                full_text += page.extract_text()


            article_pattern = r'Статья\s+(\d+(?:\.\d+)*)\.?\s*(.*?)(?=Статья\s+\d+|$)'
            matches = re.finditer(article_pattern, full_text, re.DOTALL | re.IGNORECASE)

            for match in matches:
                article_number = match.group(1).strip()
                article_content = match.group(2).strip()


                title = self._extract_title(article_content)
                keywords = self._extract_keywords(article_content)

                article = {
                    'law_type': law_type,
                    'article_number': article_number,
                    'title': title,
                    'content': article_content,
                    'keywords': keywords
                }
                articles.append(article)

                self._save_article_to_db(article)

        return articles

    def _extract_title(self, content):

        sentences = re.split(r'[.!?]', content)
        return sentences[0].strip() if sentences else ""

    def _extract_keywords(self, content):


        stop_words = {'и', 'в', 'во', 'не', 'что', 'он', 'на', 'я', 'с', 'со', 'как', 'а', 'то', 'все', 'она', 'так',
                      'его', 'но', 'да', 'ты', 'к', 'у', 'же', 'вы', 'за', 'бы', 'по', 'только', 'ее', 'мне', 'было',
                      'вот', 'от', 'меня', 'еще', 'нет', 'о', 'из', 'ему', 'теперь', 'когда', 'даже', 'ну', 'вдруг',
                      'ли', 'если', 'уже', 'или', 'ни', 'быть', 'был', 'него', 'до', 'вас', 'нибудь', 'опять', 'уж',
                      'вам', 'ведь', 'там', 'потом', 'себя', 'ничего', 'ей', 'может', 'они', 'тут', 'где', 'есть',
                      'надо', 'ней', 'для', 'мы', 'тебя', 'их', 'чем', 'была', 'сам', 'чтоб', 'без', 'будто', 'чего',
                      'раз', 'тоже', 'себе', 'под', 'будет', 'ж', 'тогда', 'кто', 'этот', 'того', 'потому', 'этого',
                      'какой', 'совсем', 'ним', 'здесь', 'этом', 'один', 'почти', 'мой', 'тем', 'чтобы', 'нее',
                      'сейчас', 'были', 'куда', 'зачем', 'всех', 'никогда', 'можно', 'при', 'наконец', 'два', 'об',
                      'другой', 'хоть', 'после', 'над', 'больше', 'тот', 'через', 'эти', 'нас', 'про', 'всего', 'них',
                      'какая', 'много', 'разве', 'три', 'эту', 'моя', 'впрочем', 'хорошо', 'свою', 'этой', 'перед',
                      'иногда', 'лучше', 'чуть', 'том', 'нельзя', 'такой', 'им', 'более', 'всегда', 'конечно', 'всю',
                      'между'}

        words = re.findall(r'\b[а-яё]{4,}\b', content.lower())
        keywords = [word for word in words if word not in stop_words]

        return ','.join(list(set(keywords))[:10])  # Максимум 10 ключевых слов

    def _save_article_to_db(self, article):

        conn = self.db.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                INSERT INTO law_articles (law_type, article_number, title, content, keywords)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                title = VALUES(title), 
                content = VALUES(content), 
                keywords = VALUES(keywords)
            ''', (article['law_type'], article['article_number'],
                  article['title'], article['content'], article['keywords']))

            conn.commit()
        except Exception as e:
            print(f"Ошибка при сохранении статьи: {e}")
        finally:
            cursor.close()
            conn.close()