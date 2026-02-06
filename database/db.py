import sqlite3
import shutil
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
import config


class Database:
    def __init__(self, db_path: str = config.DB_PATH):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        """Инициализация БД со схемой"""
        schema_path = Path(__file__).parent / 'schema.sql'
        with sqlite3.connect(self.db_path) as conn:
            with open(schema_path, 'r', encoding='utf-8') as f:
                conn.executescript(f.read())

    def add_account(self, account_id: str, username: str, token_url: str):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT INTO accounts (account_id, username, token_url, status)
                VALUES (?, ?, ?, 'pending')
                ON CONFLICT(account_id) DO UPDATE SET
                    username = excluded.username,
                    token_url = excluded.token_url,
                    status = CASE 
                        WHEN status = 'completed' THEN 'completed'
                        ELSE 'pending'
                    END
            ''', (account_id, username, token_url))

    def update_account_token(self, account_id: str, token_url: str):
        """Обновить токен-ссылку"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                UPDATE accounts 
                SET token_url = ?, updated_at = CURRENT_TIMESTAMP
                WHERE account_id = ?
            ''', (token_url, account_id))

    def update_account_status(self, account_id: str, status: str, last_page: int = None):
        """Обновить статус аккаунта"""
        with sqlite3.connect(self.db_path) as conn:
            if last_page is not None:
                conn.execute('''
                    UPDATE accounts 
                    SET status = ?, last_page = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE account_id = ?
                ''', (status, last_page, account_id))
            else:
                conn.execute('''
                    UPDATE accounts 
                    SET status = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE account_id = ?
                ''', (status, account_id))

    def add_phones(self, account_id: str, phone_numbers: List[str]):
        """Добавить номера (с дедупликацией)"""
        with sqlite3.connect(self.db_path) as conn:
            added = 0
            for phone in phone_numbers:
                try:
                    conn.execute('''
                        INSERT INTO phones (account_id, phone_number)
                        VALUES (?, ?)
                    ''', (account_id, phone))
                    added += 1
                except sqlite3.IntegrityError:
                    # Номер уже есть в БД
                    pass

            # Обновить счетчик
            conn.execute('''
                UPDATE accounts 
                SET phones_count = (
                    SELECT COUNT(*) FROM phones WHERE account_id = ?
                )
                WHERE account_id = ?
            ''', (account_id, account_id))

            return added

    def get_accounts_by_status(self, status: str) -> List[Dict]:
        """Получить аккаунты по статусу"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('''
                SELECT * FROM accounts WHERE status = ?
                ORDER BY id
            ''', (status,))
            return [dict(row) for row in cursor.fetchall()]

    def get_account(self, account_id: str) -> Optional[Dict]:
        """Получить аккаунт по ID"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('''
                SELECT * FROM accounts WHERE account_id = ?
            ''', (account_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_all_accounts_summary(self) -> List[Dict]:
        """Получить сводку по всем аккаунтам"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('''
                SELECT account_id, username, status, phones_count
                FROM accounts
                ORDER BY id
            ''')
            return [dict(row) for row in cursor.fetchall()]

    def get_total_phones(self) -> int:
        """Получить общее количество уникальных номеров"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT COUNT(*) FROM phones')
            return cursor.fetchone()[0]

    def backup(self):
        """Создать резервную копию БД"""
        Path(config.BACKUP_DIR).mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = f"{config.BACKUP_DIR}/phones_backup_{timestamp}.db"
        shutil.copy2(self.db_path, backup_path)
        return backup_path

    # НОВЫЕ МЕТОДЫ ДЛЯ ПАРАЛЛЕЛИЗАЦИИ

    def acquire_account_for_processing(self) -> Optional[Dict]:
        """
        Атомарно получить следующий аккаунт для обработки
        (thread-safe операция для мультипроцессинга)
        """
        with sqlite3.connect(self.db_path, timeout=30) as conn:
            conn.row_factory = sqlite3.Row

            # Начинаем транзакцию
            conn.execute('BEGIN IMMEDIATE')

            try:
                # Ищем аккаунт со статусом pending или in_progress
                cursor = conn.execute('''
                    SELECT * FROM accounts 
                    WHERE status IN ('pending', 'in_progress')
                    ORDER BY 
                        CASE status 
                            WHEN 'in_progress' THEN 1 
                            WHEN 'pending' THEN 2 
                        END,
                        id
                    LIMIT 1
                ''')

                account = cursor.fetchone()

                if account:
                    # Помечаем как обрабатываемый
                    account_id = account['account_id']
                    conn.execute('''
                        UPDATE accounts 
                        SET status = 'in_progress', updated_at = CURRENT_TIMESTAMP
                        WHERE account_id = ?
                    ''', (account_id,))

                    conn.commit()
                    return dict(account)
                else:
                    conn.rollback()
                    return None

            except Exception as e:
                conn.rollback()
                raise e

    def get_pending_count(self) -> int:
        """Получить количество необработанных аккаунтов"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('''
                SELECT COUNT(*) FROM accounts 
                WHERE status IN ('pending', 'in_progress')
            ''')
            return cursor.fetchone()[0]
    
