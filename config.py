import os
from dotenv import load_dotenv

load_dotenv()

# Учетные данные
ADMIN_LOGIN = os.getenv('ADMIN_LOGIN', '')
ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD', '')

# URL'ы
BASE_URL = 'https://crm.it-datamaster.ru'
LOGIN_URL = f'{BASE_URL}/admin'
ACCOUNTS_URL = f'{BASE_URL}/admin/visit/rt-admin'
TOKEN_API_URL = f'{BASE_URL}/admin/user/create-token'

# Настройки парсинга
ACCOUNTS_PER_PAGE = 200
PHONES_PER_PAGE = 50
DELAY_BETWEEN_REQUESTS = (2, 5)
DELAY_BETWEEN_ACCOUNTS = (10, 15)
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5

# Параллелизация
MAX_WORKERS = 3
WORKER_DELAY = (5, 10)

# База данных
DB_PATH = 'data/phones.db'
BACKUP_DIR = 'data/backups'
BACKUP_INTERVAL = 100

# Отчет
REPORT_PATH = 'data/report.xlsx'

# Браузер
HEADLESS = False
BROWSER_TIMEOUT = 120000  # УВЕЛИЧЕНО: 120 секунд (2 минуты) для долгих страниц
PAGE_LOAD_TIMEOUT = 120000  # Таймаут для загрузки страниц
