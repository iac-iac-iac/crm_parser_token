import time
import random
import re
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeout
from typing import List, Dict
import config
from database.db import Database
from utils.logger import logger


class AccountHarvester:
    def __init__(self, page: Page, db: Database):
        self.page = page
        self.db = db

    def harvest_all_accounts(self):
        """Собрать все аккаунты со всех страниц"""
        logger.info("🌾 Начало сбора аккаунтов...")

        # Переход на страницу с retry логикой
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(
                    f"⏳ Загрузка страницы (попытка {attempt}/{max_retries})...")
                logger.info(
                    "   (Страница может грузиться до 2 минут - это нормально)")

                self.page.goto(config.ACCOUNTS_URL,
                               timeout=config.PAGE_LOAD_TIMEOUT)

                # Ждем загрузки
                logger.info("⏳ Ожидание полной загрузки контента...")
                time.sleep(5)

                break  # Успешно загрузилось

            except PlaywrightTimeout:
                if attempt < max_retries:
                    logger.warning(
                        f"⚠️ Таймаут загрузки. Повтор через {config.RETRY_DELAY} сек...")
                    time.sleep(config.RETRY_DELAY)
                else:
                    logger.error(
                        "❌ Не удалось загрузить страницу после всех попыток")
                    raise

        current_page = 1
        total_accounts = 0

        while True:
            logger.info(f"📄 Обработка страницы {current_page}...")

            # Ждем загрузки контента (динамическая таблица)
            time.sleep(3)

            # Парсим аккаунты на текущей странице
            accounts = self._parse_accounts_on_page()

            if not accounts:
                logger.warning("⚠️ Аккаунты не найдены на странице")

                # Отладка
                if current_page == 1:
                    self.page.screenshot(path='debug_screenshot.png')
                    logger.info("📸 Скриншот: debug_screenshot.png")

                    html_content = self.page.content()
                    with open('debug_page.html', 'w', encoding='utf-8') as f:
                        f.write(html_content)
                    logger.info("📄 HTML сохранен: debug_page.html")

                    all_tr = self.page.query_selector_all('tr')
                    logger.info(f"   Всего <tr> элементов: {len(all_tr)}")
                break

            # Генерируем токены для каждого аккаунта
            for idx, account in enumerate(accounts, 1):
                logger.info(
                    f"   [{idx}/{len(accounts)}] Обработка: {account['username']}")

                token_url = self._generate_token(account['account_id'])

                if token_url:
                    self.db.add_account(
                        account_id=account['account_id'],
                        username=account['username'],
                        token_url=token_url
                    )
                    total_accounts += 1
                    logger.info(f"   ✅ Токен получен")
                else:
                    logger.error(f"   ❌ Не удалось получить токен")

                time.sleep(random.uniform(*config.DELAY_BETWEEN_REQUESTS))

            # Проверяем следующую страницу
            if not self._has_next_page():
                logger.info("📭 Достигнута последняя страница")
                break

            # Переход на следующую страницу
            self._go_to_next_page()
            current_page += 1
            time.sleep(random.uniform(3, 5))

        logger.info(f"🎉 Сбор завершен! Всего аккаунтов: {total_accounts}")

    def _parse_accounts_on_page(self) -> List[Dict]:
        """Парсинг аккаунтов на текущей странице"""
        accounts = []

        try:
            # ИСПРАВЛЕНИЕ: Разные варианты селекторов для таблицы
            selectors = [
                'table tbody tr',           # Стандартная таблица
                'table tr',                 # Без tbody
                'div[role="row"]',          # Grid/DataTable
                'tr[data-key]',             # Yii2 GridView
                '.grid-view tbody tr',      # Yii2 с классом
            ]

            rows = []
            for selector in selectors:
                rows = self.page.query_selector_all(selector)
                if len(rows) > 0:
                    logger.info(
                        f"   ✓ Найдено {len(rows)} строк с селектором: {selector}")
                    break

            if len(rows) == 0:
                logger.error(
                    "   ✗ Не найдено ни одной строки с любым селектором")
                return []

            # Парсим каждую строку
            for idx, row in enumerate(rows):
                try:
                    # Получаем весь текст строки
                    row_text = row.inner_text()

                    # Пропускаем заголовки и пустые строки
                    if not row_text or 'Пользователь' in row_text:
                        continue

                    # Проверяем наличие ID и username (не полагаемся на слово "клиент")
                    id_match = re.search(r'#(\d+)', row_text)
                    username_match = re.search(r'@([\w\-\.]+)', row_text)

                    if not id_match or not username_match:
                        continue  # Это не строка с аккаунтом


                    id_match = re.search(r'#(\d+)', row_text)
                    username_match = re.search(r'@([\w\-\.]+)', row_text)

                    if not id_match or not username_match:
                        continue

                    account_id = id_match.group(1)
                    username = username_match.group(1)  # ← Используем уже найденный match

                    accounts.append({
                        'account_id': account_id,
                        'username': username
                    })


                    username = username_match.group(1)

                    accounts.append({
                        'account_id': account_id,
                        'username': username
                    })

                    if idx < 3:  # Логируем первые 3 для проверки
                        logger.debug(
                            f"   Найден: ID={account_id}, User={username}")

                except Exception as e:
                    logger.debug(f"   Ошибка парсинга строки {idx}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Критическая ошибка парсинга: {e}")

        return accounts

    def _generate_token(self, account_id: str) -> str:
        """Генерация токена через клик на кнопку"""
        try:
            # Ищем строку с нужным account_id
            row_xpath = f'//tr[contains(., "#{account_id}")]'
            row = self.page.query_selector(row_xpath)

            if not row:
                logger.error(f"   Строка с ID {account_id} не найдена")
                return None

            # Ищем кнопку генерации токена
            button = row.query_selector('a[onclick*="create-token"]')

            if not button:
                button = row.query_selector('[data-url*="create-token"]')

            if not button:
                button = row.query_selector('a[title*="ссылк"]')

            if not button:
                links = row.query_selector_all('a')
                if len(links) > 0:
                    button = links[0]

            if not button:
                logger.error(
                    f"   Кнопка токена не найдена для ID {account_id}")
                return None

            # СПОСОБ 1: Перехватываем network request
            token_url = None
            network_intercepted = False


            # СПОСОБ 2: Перехватываем dialog
            dialog_appeared = False

            def handle_dialog(dialog):
                nonlocal token_url, dialog_appeared
                dialog_appeared = True
                message = dialog.message
                logger.debug(f"   Dialog: {message[:100]}...")

                # Извлекаем токен из сообщения
                if 'signin?token=' in message:
                    import re
                    match = re.search(r'(http[s]?://[^\s]+)', message)
                    if match:
                        token_url = match.group(1)

                dialog.accept()

            self.page.on('dialog', handle_dialog)

            # СПОСОБ 3: Читаем буфер обмена (после клика токен копируется туда)
            # Для этого нужно дать разрешение на чтение clipboard

            # Кликаем на кнопку
            try:
                button.click(timeout=5000)
            except Exception as e:
                logger.error(f"   Ошибка клика: {e}")

            # Ждем результата
            time.sleep(2)

            # Пробуем прочитать из буфера обмена
            if not token_url:
                try:
                    # Выполняем JS для чтения clipboard
                    clipboard_text = self.page.evaluate(
                        '() => navigator.clipboard.readText()')
                    if clipboard_text and 'signin?token=' in clipboard_text:
                        token_url = clipboard_text
                        logger.debug(
                            f"   Токен из буфера: {clipboard_text[:50]}...")
                except Exception as e:
                    logger.debug(f"   Не удалось прочитать буфер: {e}")

            # Ищем toast/notification на странице
            if not token_url:
                try:
                    # Ищем уведомление с токеном
                    notification_selectors = [
                        '.alert:has-text("http")',
                        '.notification:has-text("signin")',
                        '[role="alert"]:has-text("token")',
                        '.toast:has-text("http")',
                        'div:has-text("signin?token=")',
                    ]

                    for selector in notification_selectors:
                        notification = self.page.query_selector(selector)
                        if notification:
                            text = notification.inner_text()
                            if 'signin?token=' in text:
                                import re
                                match = re.search(r'(http[s]?://[^\s]+)', text)
                                if match:
                                    token_url = match.group(1)
                                    logger.debug(
                                        f"   Токен из уведомления: {token_url[:50]}...")
                                    break
                except:
                    pass

            # Убираем listeners
            self.page.remove_listener('dialog', handle_dialog)

            if token_url:
                # Очищаем токен от мусора
                if '?' in token_url:
                    # Убираем пробелы и лишнее
                    token_url = token_url.split()[0]
                return token_url

            logger.error(f"   Dialog не появился или токен не найден")

            # ОТЛАДКА: Сохраняем скриншот
            self.page.screenshot(path=f'debug_token_{account_id}.png')
            logger.info(f"   📸 Скриншот: debug_token_{account_id}.png")

            return None

        except Exception as e:
            logger.error(f"   Ошибка генерации токена: {e}")
            return None

    def _has_next_page(self) -> bool:
        """Проверка наличия следующей страницы"""
        try:
            # Ищем активную кнопку "следующая"
            next_selectors = [
                'li.next:not(.disabled) a',
                'a[data-page]:not(.disabled)',
                '.pagination .next:not(.disabled)',
                'li:not(.disabled) > a[rel="next"]',
            ]

            for selector in next_selectors:
                next_button = self.page.query_selector(selector)
                if next_button:
                    return True

            return False
        except:
            return False

    def _go_to_next_page(self):
        """Переход на следующую страницу"""
        try:
            next_selectors = [
                'li.next:not(.disabled) a',
                'a[rel="next"]:not(.disabled)',
                '.pagination .next:not(.disabled) a',
            ]
            for selector in next_selectors:
                next_button = self.page.query_selector(selector)
                if next_button:
                    next_button.click()
                    time.sleep(3)  # Ждем загрузки
                    return

            logger.error("Кнопка 'Следующая страница' не найдена")

        except Exception as e:
            logger.error(f"Ошибка перехода на следующую страницу: {e}")
