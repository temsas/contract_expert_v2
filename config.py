import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # SQLite
    DATABASE_TYPE = 'sqlite'
    SQLITE_DATABASE = 'laws.db'

    # GigaChat
    GIGACHAT_CREDENTIALS = os.getenv('GIGACHAT_CREDENTIALS')
    GIGACHAT_MODEL = os.getenv('GIGACHAT_MODEL', 'GigaChat-2')

    # Files
    UPLOAD_FOLDER = 'uploads'
    ALLOWED_EXTENSIONS = {'pdf', 'doc', 'docx'}