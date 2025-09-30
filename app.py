from flask import Flask, render_template, request, jsonify
import os
import re
import PyPDF2
from database.db_connection import Database
from services.contract_analyzer import ContractAnalyzer

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-here'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['ALLOWED_EXTENSIONS'] = {'pdf', 'doc', 'docx'}

# Инициализация анализатора при запуске
try:
    contract_analyzer = ContractAnalyzer()
    AI_AVAILABLE = True
    print("✅ Система инициализирована успешно")
except Exception as e:
    print(f"❌ Ошибка инициализации: {e}")
    AI_AVAILABLE = False
    contract_analyzer = None


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
    return '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Контрактный эксперт</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; background: #f8f9fa; }
            .container { max-width: 800px; margin: 0 auto; }
            .card { background: white; padding: 25px; margin: 20px 0; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
            button { background: #007bff; color: white; padding: 12px 24px; border: none; border-radius: 4px; cursor: pointer; font-size: 16px; }
            button:hover { background: #0056b3; }
            button:disabled { background: #6c757d; cursor: not-allowed; }
            .format-info { background: #e9ecef; padding: 10px; border-radius: 4px; margin: 10px 0; font-size: 14px; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🔍 Контрактный эксперт</h1>
            <p>Проверка контрактов на соответствие 44-ФЗ и 223-ФЗ</p>

            <div class="card">
                <h2>Анализ контракта</h2>

                <div class="format-info">
                    <strong>Поддерживаемые форматы:</strong> PDF, DOC, DOCX
                </div>

                <form id="uploadForm" enctype="multipart/form-data" style="margin-top: 15px;">
                    <div style="margin: 15px 0;">
                        <label for="law_type" style="display: block; margin-bottom: 5px; font-weight: bold;">Выберите закон для проверки:</label>
                        <select id="law_type" name="law_type" required style="padding: 10px; width: 100%; border: 1px solid #ddd; border-radius: 4px;">
                            <option value="44_fz">44-ФЗ</option>
                            <option value="223_fz">223-ФЗ</option>
                        </select>
                    </div>

                    <div style="margin: 15px 0;">
                        <label for="contract_file" style="display: block; margin-bottom: 5px; font-weight: bold;">Загрузите контракт:</label>
                        <input type="file" id="contract_file" name="contract_file" 
                               accept=".pdf,.doc,.docx" required style="padding: 8px; width: 100%;">
                    </div>

                    <button type="submit" ''' + ('disabled' if not AI_AVAILABLE else '') + '''>
                        ''' + ("🔍 Проанализировать контракт" if AI_AVAILABLE else "❌ Система недоступна") + '''
                    </button>
                </form>
            </div>

            <div id="result" class="card" style="display: none;">
                <h2>Результат анализа</h2>
                <div id="analysisResult"></div>
            </div>
        </div>

        <script>
            document.getElementById('uploadForm').addEventListener('submit', async function(e) {
                e.preventDefault();

                const formData = new FormData(this);
                const resultDiv = document.getElementById('analysisResult');
                const resultCard = document.getElementById('result');

                resultCard.style.display = 'block';
                resultDiv.innerHTML = '<p>⏳ Анализ контракта...</p>';

                try {
                    const response = await fetch('/analyze', {
                        method: 'POST',
                        body: formData
                    });

                    const data = await response.json();

                    if (data.status === 'success') {
                        let html = `
                            <h3>Проверка по ${data.law_type.toUpperCase()}</h3>
                            <p><strong>Файл:</strong> ${data.filename}</p>
                            <p><strong>Статус соответствия:</strong> <span style="font-weight: bold; color: ${
                                data.analysis.compliance_status === 'соответствует' ? 'green' : 
                                data.analysis.compliance_status === 'частично соответствует' ? 'orange' : 'red'
                            }">${data.analysis.compliance_status}</span></p>
                            <p><strong>Заключение:</strong> ${data.analysis.summary}</p>
                        `;

                        if (data.analysis.issues && data.analysis.issues.length > 0) {
                            html += '<h4>Выявленные проблемы:</h4><ul>';
                            data.analysis.issues.forEach(issue => {
                                html += `
                                    <li style="margin-bottom: 15px; padding: 10px; background: #f8f9fa; border-left: 4px solid #e74c3c;">
                                        <strong>${issue.article}:</strong> ${issue.issue}
                                        <br><em>Рекомендация:</em> ${issue.recommendation}
                                    </li>
                                `;
                            });
                            html += '</ul>';
                        } else {
                            html += '<p style="color: green; font-weight: bold;">✅ Нарушений не выявлено</p>';
                        }

                        resultDiv.innerHTML = html;
                    } else {
                        resultDiv.innerHTML = `<p style="color: red;">❌ Ошибка: ${data.error}</p>`;
                    }
                } catch (error) {
                    resultDiv.innerHTML = `<p style="color: red;">❌ Ошибка: ${error}</p>`;
                }
            });
        </script>
    </body>
    </html>
    '''


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


if __name__ == '__main__':
    os.makedirs('uploads', exist_ok=True)
    os.makedirs('data', exist_ok=True)

    initialize_system()

    print("🚀 Сервер запущен!")
    print("🔍 Интерфейс: http://localhost:5000/")

    app.run(debug=True, host='0.0.0.0', port=5000, use_reloader=False)