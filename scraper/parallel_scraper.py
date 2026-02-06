import time
import random
import multiprocessing as mp
from typing import Optional
from pathlib import Path
import config
from database.db import Database
from scraper.browser import BrowserManager
from scraper.phone_scraper import PhoneScraper
from utils.logger import logger


def worker_process(worker_id: int, total_workers: int):
    """
    Воркер процесс для параллельной обработки аккаунтов

    Args:
        worker_id: ID воркера (1, 2, 3...)
        total_workers: Общее количество воркеров
    """
    # Создаем свою БД для каждого процесса
    db = Database()

    # Настройка логгера для воркера
    from utils.logger import setup_logger
    worker_logger = setup_logger(f'Worker-{worker_id}')

    worker_logger.info(f"🚀 Воркер #{worker_id} запущен")

    # Задержка перед стартом (чтобы не все воркеры стартовали одновременно)
    if worker_id > 1:
        delay = random.uniform(*config.WORKER_DELAY)
        worker_logger.info(f"⏳ Ожидание {delay:.1f}сек перед стартом...")
        time.sleep(delay)

    processed_count = 0

    try:
        # Открываем браузер один раз для всех аккаунтов этого воркера
        with BrowserManager(headless=config.HEADLESS) as browser:
            page = browser.new_page()
            scraper = PhoneScraper(page, db)

            while True:
                # Атомарно получаем следующий аккаунт
                account = db.acquire_account_for_processing()

                if not account:
                    worker_logger.info("📭 Нет больше аккаунтов для обработки")
                    break

                account_id = account['account_id']
                username = account['username']
                token_url = account['token_url']
                last_page = account['last_page']

                worker_logger.info(
                    f"🔄 Обработка: {username} (ID: {account_id})")

                if not token_url:
                    worker_logger.error(f"❌ Нет токен-ссылки для {account_id}")
                    db.update_account_status(account_id, 'failed')
                    continue

                # Парсим аккаунт
                start_page = last_page + 1 if last_page > 0 else 1
                phones_count = scraper.scrape_account(
                    account_id, token_url, start_page)

                processed_count += 1
                worker_logger.info(f"✅ Обработано: {phones_count} номеров")

                # Задержка между аккаунтами
                delay = random.uniform(*config.DELAY_BETWEEN_ACCOUNTS)
                worker_logger.info(f"⏳ Пауза {delay:.1f}сек...")
                time.sleep(delay)

    except KeyboardInterrupt:
        worker_logger.warning("⚠️ Воркер остановлен пользователем")
    except Exception as e:
        worker_logger.error(
            f"❌ Критическая ошибка в воркере: {e}", exc_info=True)
    finally:
        worker_logger.info(
            f"🏁 Воркер #{worker_id} завершен. Обработано: {processed_count} аккаунтов")
        return processed_count


class ParallelScraper:
    """Оркестратор параллельной обработки"""

    def __init__(self, max_workers: int = config.MAX_WORKERS):
        self.max_workers = max_workers
        self.db = Database()

    def run(self):
        """Запустить параллельную обработку"""
        pending_count = self.db.get_pending_count()

        if pending_count == 0:
            logger.info("✅ Все аккаунты уже обработаны!")
            return

        logger.info("=" * 60)
        logger.info(f"🚀 ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА: {self.max_workers} воркеров")
        logger.info(f"📋 Аккаунтов к обработке: {pending_count}")
        logger.info("=" * 60)

        # Оптимизируем количество воркеров
        actual_workers = min(self.max_workers, pending_count)
        logger.info(f"🔢 Запускаю {actual_workers} воркеров...")

        start_time = time.time()

        try:
            # Создаем пул процессов
            with mp.Pool(processes=actual_workers) as pool:
                # Запускаем воркеры
                results = []
                for worker_id in range(1, actual_workers + 1):
                    result = pool.apply_async(
                        worker_process,
                        args=(worker_id, actual_workers)
                    )
                    results.append(result)

                # Ждем завершения всех воркеров
                pool.close()
                pool.join()

                # Собираем результаты
                total_processed = sum([r.get() for r in results])

        except KeyboardInterrupt:
            logger.warning(
                "\n⚠️ Прерывание пользователем. Останавливаю воркеры...")
            pool.terminate()
            pool.join()
            total_processed = 0

        # Финальная статистика
        elapsed_time = time.time() - start_time
        logger.info("\n" + "=" * 60)
        logger.info(f"🎉 ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА ЗАВЕРШЕНА")
        logger.info(f"⏱️ Время выполнения: {elapsed_time/60:.1f} минут")
        logger.info(f"📊 Обработано аккаунтов: {total_processed}")
        if total_processed > 0:
            logger.info(
                f"⚡ Скорость: {elapsed_time/total_processed:.1f} сек/аккаунт")
        logger.info("=" * 60)

        # Создаем бэкап
        backup_path = self.db.backup()
        logger.info(f"💾 Бэкап создан: {backup_path}")
