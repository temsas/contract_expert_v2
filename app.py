from flask import Flask, render_template, request, jsonify
import os
import re
import PyPDF2
from database.db_connection import Database
from services.contract_analyzer import ContractAnalyzer
from services.supplier_selector import SupplierSelector
from services.data_parser import GosZakupParser

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-here'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['ALLOWED_EXTENSIONS'] = {'pdf', 'doc', 'docx'}

try:
    contract_analyzer = ContractAnalyzer()
    supplier_selector = SupplierSelector()
    AI_AVAILABLE = True
    print("✅ Система инициализирована успешно")
except Exception as e:
    print(f"❌ Ошибка инициализации: {e}")
    AI_AVAILABLE = False
    contract_analyzer = None
    supplier_selector = None


class FileProcessor:
    @staticmethod
    def extract_text_from_file(file_path, filename):
        """Извлекает текст из файла"""
        file_ext = os.path.splitext(filename)[1].lower()

        if file_ext == '.pdf':
            return FileProcessor._extract_from_pdf(file_path)
        elif file_ext in ['.doc', '.docx']:
            return FileProcessor._extract_from_docx_stub(file_path, filename)
        else:
            raise ValueError(f"Неподдерживаемый формат: {file_ext}")

    @staticmethod
    def _extract_from_pdf(file_path):
        """Извлекает текст из PDF"""
        try:
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                text = ""
                for page in pdf_reader.pages:
                    text += page.extract_text()
                return text
        except Exception as e:
            raise Exception(f"Ошибка чтения PDF: {str(e)}")

    @staticmethod
    def _extract_from_docx_stub(file_path, filename):
        try:
            from docx import Document
            doc = Document(file_path)
            text = ""
            for paragraph in doc.paragraphs:
                if paragraph.text.strip():
                    text += paragraph.text + "\n"
            return text
        except ImportError:
            return f"[ВНИМАНИЕ] Файл {filename} загружен. Для полной поддержки DOCX установите: pip install python-docx"
        except Exception as e:
            return f"[ОШИБКА] Не удалось прочитать DOCX файл: {str(e)}"


class LawParser:
    def __init__(self):
        self.db = Database()

    def parse_law_pdf(self, file_path, law_type):
        print(f"📖 Парсим {law_type}...")

        if not os.path.exists(file_path):
            print(f"❌ Файл не найден: {file_path}")
            return 0

        try:
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                full_text = ""
                for page in pdf_reader.pages:
                    full_text += page.extract_text()

                articles_count = self._extract_articles_simple(full_text, law_type)
                return articles_count

        except Exception as e:
            print(f"❌ Ошибка парсинга {law_type}: {e}")
            return 0

    def _extract_articles_simple(self, text, law_type):
        articles_count = 0

        patterns = [
            r'Статья\s+(\d+(?:\.\d+)*)\.?\s*(.*?)(?=Статья\s+\d+|$)',
            r'СТАТЬЯ\s+(\d+(?:\.\d+)*)\.?\s*(.*?)(?=СТАТЬЯ\s+\d+|$)',
            r'ст\.\s*(\d+(?:\.\d+)*)\.?\s*(.*?)(?=ст\.\s*\d+|$)',
        ]

        for pattern in patterns:
            matches = re.finditer(pattern, text, re.DOTALL | re.IGNORECASE)
            for match in matches:
                article_number = match.group(1).strip()
                article_content = match.group(2).strip()

                if len(article_content) > 30 and len(article_content) < 5000:
                    self._save_article(law_type, article_number, article_content[:2000])
                    articles_count += 1

        return articles_count

    def _save_article(self, law_type, article_number, content):
        """Сохраняет статью в базу данных"""
        conn = self.db.get_connection()
        cursor = conn.cursor()

        try:
            title_match = re.split(r'[.!?]', content)
            title = title_match[0].strip() if title_match else content[:100]

            cursor.execute('''
                INSERT OR REPLACE INTO law_articles (law_type, article_number, title, content)
                VALUES (?, ?, ?, ?)
            ''', (law_type, article_number, title, content))

            conn.commit()
        except Exception as e:
            print(f"❌ Ошибка сохранения статьи {article_number}: {e}")
        finally:
            cursor.close()
            conn.close()


def initialize_system():
    print("🚀 Инициализация системы...")

    db = Database()
    db.init_db()

    # Проверяем, есть ли уже статьи в базе
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM law_articles")
    existing_articles = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    if existing_articles > 0:
        print(f"✅ В базе уже есть {existing_articles} статей")
        return existing_articles

    # Загружаем законы если их нет
    print("📚 Загружаем законы...")
    parser = LawParser()

    law_files = {
        '44_fz': 'data/44fz_.pdf',
        '223_fz': 'data/223fz_.pdf'
    }

    total_articles = 0
    for law_type, file_path in law_files.items():
        if os.path.exists(file_path):
            articles_count = parser.parse_law_pdf(file_path, law_type)
            total_articles += articles_count
            if articles_count > 0:
                print(f"✅ {law_type}: {articles_count} статей")
        else:
            print(f"⚠️ Файл не найден: {file_path}")

    print(f"✅ Загружено всего: {total_articles} статей")
    return total_articles


def allowed_file(filename):
    return '.' in filename and \
        filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']


@app.route('/')
def index():
    return render_template('index.html', AI_AVAILABLE=AI_AVAILABLE)


@app.route('/analyze', methods=['POST'])
def analyze_contract():
    if not AI_AVAILABLE:
        return jsonify({'error': 'Система недоступна. Проверьте настройки.'}), 500

    if 'contract_file' not in request.files:
        return jsonify({'error': 'Файл не загружен'}), 400

    file = request.files['contract_file']
    law_type = request.form.get('law_type', '44_fz')

    if file.filename == '':
        return jsonify({'error': 'Файл не выбран'}), 400

    if file and allowed_file(file.filename):
        filename = file.filename
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)

        try:
            contract_text = FileProcessor.extract_text_from_file(file_path, filename)
            result = contract_analyzer.analyze_contract(contract_text, law_type, filename)
            os.remove(file_path)

            return jsonify({
                'status': 'success',
                'law_type': law_type,
                'filename': filename,
                'analysis': result
            })

        except Exception as e:
            if os.path.exists(file_path):
                os.remove(file_path)
            return jsonify({'error': f'Ошибка анализа: {str(e)}'}), 500

    return jsonify({'error': 'Неверный формат файла'}), 400


@app.route('/status')
def system_status():
    """Статус системы"""
    conn = Database().get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM law_articles WHERE law_type = '44_fz'")
    articles_44 = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM law_articles WHERE law_type = '223_fz'")
    articles_223 = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    return jsonify({
        'status': 'running',
        'system_available': AI_AVAILABLE,
        'articles_44_fz': articles_44,
        'articles_223_fz': articles_223,
        'total_articles': articles_44 + articles_223
    })


@app.route('/suppliers', methods=['POST'])
def get_suppliers():
    """Подбор поставщиков по способу закупки и категории (поддерживает пустые значения)"""
    if not supplier_selector:
        return jsonify({'status': 'error', 'message': 'Система подбора поставщиков недоступна'}), 500

    data = request.get_json()
    purchase_method = data.get('purchase_method', '').strip()
    category = data.get('category', '').strip()
    limit = data.get('limit', 50)

    if not purchase_method and not category:
        top_suppliers = supplier_selector.get_filtered_suppliers(limit=limit)
    else:
        top_suppliers = supplier_selector.get_top_suppliers(purchase_method, category, limit)

    return jsonify({
        'status': 'success',
        'count': len(top_suppliers),
        'suppliers': top_suppliers
    })

@app.route('/api/purchase-methods')
def get_purchase_methods():
    if not supplier_selector:
        return jsonify([])
    methods = supplier_selector.get_all_purchase_methods()
    return jsonify(methods)


@app.route('/api/categories')
def get_categories():
    if not supplier_selector:
        return jsonify([])
    categories = supplier_selector.get_all_categories()
    return jsonify(categories)


@app.route('/api/search-categories/<query>')
def search_categories(query):
    if not supplier_selector:
        return jsonify([])
    categories = supplier_selector.search_categories(query)
    return jsonify(categories)


@app.route('/api/search-purchase-methods/<query>')
def search_purchase_methods(query):
    if not supplier_selector:
        return jsonify([])
    methods = supplier_selector.search_purchase_methods(query)
    return jsonify(methods)


@app.route('/api/update-suppliers', methods=['POST'])
def update_suppliers():
    """Обновляет данные поставщиков с сайта goszakup.gov.kz"""
    try:
        parser = GosZakupParser()
        success = parser.update_all_suppliers()

        if success:
            return jsonify({
                'status': 'success',
                'message': 'Данные поставщиков успешно обновлены'
            })
        else:
            return jsonify({
                'status': 'error',
                'message': 'Не удалось обновить данные поставщиков'
            }), 500

    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Ошибка при обновлении: {str(e)}'
        }), 500


@app.route('/api/suppliers-stats')
def get_suppliers_stats():
    """Возвращает статистику по поставщикам в базе"""
    if not supplier_selector:
        return jsonify({})
    stats = supplier_selector.get_suppliers_stats()
    return jsonify(stats)


if __name__ == '__main__':
    os.makedirs('uploads', exist_ok=True)
    os.makedirs('data', exist_ok=True)

    initialize_system()

    print("🚀 Сервер запущен!")
    print("🔍 Интерфейс: http://localhost:5000/")

    app.run(debug=True, host='0.0.0.0', port=5000, use_reloader=False)