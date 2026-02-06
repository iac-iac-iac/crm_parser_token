import sys
import time
import random
import signal
import sqlite3
from pathlib import Path
from argparse import ArgumentParser
import config
from database.db import Database
from scraper.browser import BrowserManager
from scraper.auth import login_to_admin
from scraper.harvester import AccountHarvester
from scraper.phone_scraper import PhoneScraper
from utils.report import generate_excel_report
from utils.logger import logger
from scraper.parallel_scraper import ParallelScraper


class ScraperOrchestrator:
    """Главный оркестратор процесса парсинга"""

    def __init__(self):
        self.db = Database()
        self.interrupted = False
        self.accounts_processed = 0

        # Обработка Ctrl+C
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, sig, frame):
        """Обработчик прерывания"""
        logger.warning(
            "\n⚠️ Получен сигнал остановки. Завершаем текущую операцию...")
        self.interrupted = True

    def run_harvest(self):
        """Фаза 1: Сбор аккаунтов и генерация токенов"""
        logger.info("=" * 60)
        logger.info("🌾 ФАЗА 1: Сбор аккаунтов и генерация токенов")
        logger.info("=" * 60)

        with BrowserManager() as browser:
            page = browser.new_page()

            # Авторизация
            if not login_to_admin(page):
                logger.error(
                    "❌ Не удалось авторизоваться. Проверьте credentials в .env")
                return False

            # Сбор аккаунтов
            harvester = AccountHarvester(page, self.db)
            harvester.harvest_all_accounts()

        return True

    def run_scrape(self):
        """Фаза 2: Парсинг номеров из аккаунтов"""
        logger.info("=" * 60)
        logger.info("📞 ФАЗА 2: Парсинг номеров из аккаунтов")
        logger.info("=" * 60)

        # Получаем аккаунты для обработки
        pending_accounts = self.db.get_accounts_by_status('pending')
        in_progress_accounts = self.db.get_accounts_by_status('in_progress')

        accounts_to_process = in_progress_accounts + pending_accounts
        total = len(accounts_to_process)

        if total == 0:
            logger.info("✅ Все аккаунты уже обработаны!")
            return True

        logger.info(f"📋 Аккаунтов к обработке: {total}")
        logger.info(f"   • В процессе: {len(in_progress_accounts)}")
        logger.info(f"   • Ожидают: {len(pending_accounts)}")

        with BrowserManager() as browser:
            page = browser.new_page()
            scraper = PhoneScraper(page, self.db)

            for idx, account in enumerate(accounts_to_process, 1):
                if self.interrupted:
                    logger.warning("⏸️ Парсинг приостановлен пользователем")
                    break

                account_id = account['account_id']
                username = account['username']
                token_url = account['token_url']
                last_page = account['last_page']

                logger.info(
                    f"\n[{idx}/{total}] 🔄 Обработка: {username} (ID: {account_id})")

                if not token_url:
                    logger.error(
                        f"❌ Нет токен-ссылки для аккаунта {account_id}")
                    self.db.update_account_status(account_id, 'failed')
                    continue

                # Парсинг номеров
                start_page = last_page + 1 if last_page > 0 else 1
                scraper.scrape_account(account_id, token_url, start_page)

                self.accounts_processed += 1

                # Резервное копирование
                if self.accounts_processed % config.BACKUP_INTERVAL == 0:
                    backup_path = self.db.backup()
                    logger.info(f"💾 Создан бэкап: {backup_path}")

                # Задержка между аккаунтами
                if idx < total:
                    delay = random.uniform(*config.DELAY_BETWEEN_ACCOUNTS)
                    logger.info(
                        f"⏳ Ожидание {delay:.1f}сек перед следующим аккаунтом...")
                    time.sleep(delay)

        # Финальный бэкап
        if self.accounts_processed > 0:
            backup_path = self.db.backup()
            logger.info(f"💾 Финальный бэкап: {backup_path}")

        return True

    def run_full(self):
        """Полный цикл: сбор + парсинг"""
        logger.info("🚀 ЗАПУСК ПОЛНОГО ЦИКЛА ПАРСИНГА")

        # Фаза 1
        if not self.run_harvest():
            return False

        if self.interrupted:
            return False

        # Пауза между фазами
        logger.info("\n⏳ Пауза 5 секунд перед началом парсинга номеров...")
        time.sleep(5)

        # Фаза 2
        return self.run_scrape()

    def resume(self):
        """Возобновление прерванной работы"""
        logger.info("🔄 ВОЗОБНОВЛЕНИЕ ПАРСИНГА")
        return self.run_scrape()

    def generate_report(self):
        """Генерация отчета"""
        generate_excel_report(self.db)

    # ✅ ИСПРАВЛЕНИЕ (в main.py, строки 172-192)
    @staticmethod
    def show_stats():
        db = Database()
        
        logger.info("📊 Статистика аккаунтов:")
        
        # Получаем статистику через SQL
        with sqlite3.connect(config.DB_PATH) as conn:
            cursor = conn.execute('''
                SELECT status, COUNT(*) as count 
                FROM accounts 
                GROUP BY status
            ''')
            for row in cursor:
                logger.info(f"   {row[0]}: {row[1]}")
        
        # Общие данные через методы Database
        logger.info(f"\n📋 Всего аккаунтов: {len(db.get_all_accounts_summary())}")
        logger.info(f"📞 Всего номеров: {db.get_total_phones()}")


def main():
    # ИСПРАВЛЕНИЕ: Создаем parser перед использованием
    parser = ArgumentParser(
        description='CRM Scraper - Автоматизация парсинга номеров')
    parser.add_argument(
        '--mode',
        choices=['full', 'harvest', 'scrape', 'report',
                 'parallel', 'clear'],  # ДОБАВЛЕНО clear
        default='full',
        help='Режим работы'
    )
    parser.add_argument(
        '--clear',
        choices=['tokens', 'accounts', 'phones',
                 'all', 'reset-failed', 'reset-progress'],
        help='Тип очистки (используется с --mode clear)'
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=config.MAX_WORKERS,
        help=f'Количество параллельных воркеров (по умолчанию: {config.MAX_WORKERS})'
    )
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Возобновить прерванную работу'
    )
    parser.add_argument(
        '--headless',
        action='store_true',
        default=True,
        help='Запуск браузера в headless режиме'
    )

    args = parser.parse_args()

    # Проверка credentials
    if not config.ADMIN_LOGIN or not config.ADMIN_PASSWORD:
        logger.error("❌ Не заданы ADMIN_LOGIN и ADMIN_PASSWORD в файле .env")
        sys.exit(1)

    # Установка headless режима
    config.HEADLESS = args.headless

    # Запуск
    orchestrator = ScraperOrchestrator()

    try:
        if args.resume:
            orchestrator.resume()
        elif args.mode == 'full':
            orchestrator.run_full()
        elif args.mode == 'harvest':
            orchestrator.run_harvest()
        elif args.mode == 'scrape':
            orchestrator.run_scrape()
        elif args.mode == 'parallel':
            parallel_scraper = ParallelScraper(max_workers=args.workers)
            parallel_scraper.run()
        elif args.mode == 'report':
            orchestrator.generate_report()
        elif args.mode == 'clear':
            if not args.clear:
                logger.error(
                    "❌ Укажите тип очистки: --clear <tokens|accounts|phones|all>")
                sys.exit(1)

            # Подтверждение
            confirm = input(
                f"⚠️ Подтверждаете очистку '{args.clear}'? (yes/no): ")
            if confirm.lower() not in ['yes', 'y', 'да']:
                logger.info("❌ Отменено")
                return

            import sqlite3

            if args.clear == 'tokens':
                with sqlite3.connect(config.DB_PATH) as conn:
                    conn.execute(
                        'UPDATE accounts SET token_url = NULL, status = "pending"')
                logger.info("✅ Токены очищены")

            elif args.clear == 'accounts':
                with sqlite3.connect(config.DB_PATH) as conn:
                    conn.execute('DELETE FROM accounts')
                logger.info("✅ Аккаунты удалены")

            elif args.clear == 'phones':
                with sqlite3.connect(config.DB_PATH) as conn:
                    conn.execute('DELETE FROM phones')
                    conn.execute('UPDATE accounts SET phones_count = 0')
                logger.info("✅ Номера удалены")

            elif args.clear == 'all':
                with sqlite3.connect(config.DB_PATH) as conn:
                    conn.execute('DELETE FROM phones')
                    conn.execute('DELETE FROM accounts')
                logger.info("✅ БД очищена")

            elif args.clear == 'reset-failed':
                with sqlite3.connect(config.DB_PATH) as conn:
                    cursor = conn.execute(
                        'UPDATE accounts SET status = "pending" WHERE status = "failed"')
                    logger.info(f"✅ Сброшено {cursor.rowcount} аккаунтов")

            elif args.clear == 'reset-progress':
                with sqlite3.connect(config.DB_PATH) as conn:
                    cursor = conn.execute(
                        'UPDATE accounts SET status = "pending" WHERE status = "in_progress"')
                    logger.info(f"✅ Сброшено {cursor.rowcount} аккаунтов")

            return

        # Генерация отчета в конце
        logger.info("\n" + "=" * 60)
        orchestrator.generate_report()
        logger.info("=" * 60)
        logger.info("🎉 ПАРСИНГ ЗАВЕРШЕН!")

    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
