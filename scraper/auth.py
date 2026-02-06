import time
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeout
import config
from utils.logger import logger


def login_to_admin(page: Page) -> bool:
    """Авторизация в админке"""
    try:
        logger.info("🔐 Авторизация в админке...")

        # Переход на страницу входа с увеличенным таймаутом
        page.goto(config.LOGIN_URL, timeout=config.PAGE_LOAD_TIMEOUT)
        time.sleep(2)

        # Проверяем что мы на странице входа
        current_url = page.url
        logger.debug(f"   Текущий URL: {current_url}")

        # Если уже авторизованы (есть активная сессия)
        if '/admin' in current_url and '/login' not in current_url.lower() and '/signin' not in current_url.lower():
            logger.info("✅ Уже авторизован (активная сессия)")
            return True

        # Ищем поля формы входа
        login_selectors = [
            'input[name="LoginForm[username]"]',
            'input[name="username"]',
            'input[type="text"]',
            '#loginform-username',
        ]

        password_selectors = [
            'input[name="LoginForm[password]"]',
            'input[name="password"]',
            'input[type="password"]',
            '#loginform-password',
        ]

        # Находим поле логина
        login_input = None
        for selector in login_selectors:
            login_input = page.query_selector(selector)
            if login_input:
                logger.debug(f"   Найдено поле логина: {selector}")
                break

        if not login_input:
            logger.error("❌ Поле логина не найдено")
            page.screenshot(path='debug_login_page.png')
            logger.info("📸 Скриншот сохранен: debug_login_page.png")
            return False

        # Находим поле пароля
        password_input = None
        for selector in password_selectors:
            password_input = page.query_selector(selector)
            if password_input:
                logger.debug(f"   Найдено поле пароля: {selector}")
                break

        if not password_input:
            logger.error("❌ Поле пароля не найдено")
            return False

        # Заполняем форму
        logger.info("   Заполнение формы...")
        login_input.fill(config.ADMIN_LOGIN)
        time.sleep(0.5)
        password_input.fill(config.ADMIN_PASSWORD)
        time.sleep(0.5)

        # Ищем кнопку входа
        button_selectors = [
            'button[type="submit"]',
            'input[type="submit"]',
            'button:has-text("Вход")',
            '.btn-primary',
        ]

        submit_button = None
        for selector in button_selectors:
            submit_button = page.query_selector(selector)
            if submit_button:
                logger.debug(f"   Найдена кнопка входа: {selector}")
                break

        if not submit_button:
            logger.error("❌ Кнопка входа не найдена")
            return False

        # Нажимаем кнопку входа
        logger.info("   Нажатие кнопки входа...")
        submit_button.click()

        # Ждем навигации (с большим таймаутом)
        try:
            # Вариант 1: Ждем изменения URL
            page.wait_for_url('**/admin/**', timeout=30000)
            logger.info("✅ Успешная авторизация")
            return True

        except PlaywrightTimeout:
            # Вариант 2: Проверяем текущий URL после задержки
            logger.debug("   Таймаут wait_for_url, проверяю текущий URL...")
            time.sleep(3)

            current_url = page.url
            logger.debug(f"   URL после входа: {current_url}")

            # Проверяем что мы не на странице входа
            if '/admin' in current_url and '/login' not in current_url.lower() and '/signin' not in current_url.lower():
                logger.info("✅ Успешная авторизация")
                return True

            # Вариант 3: Проверяем наличие элементов админки
            admin_elements = page.query_selector_all(
                '.main-header, .navbar, [class*="admin"]')
            if len(admin_elements) > 0:
                logger.info(
                    "✅ Успешная авторизация (обнаружены элементы админки)")
                return True

            # Проверяем ошибки на странице
            error_messages = page.query_selector_all(
                '.alert-danger, .error, [class*="error"]')
            if error_messages:
                error_text = error_messages[0].inner_text()
                logger.error(f"❌ Ошибка входа: {error_text}")
            else:
                logger.error("❌ Не удалось войти (неизвестная причина)")

            page.screenshot(path='debug_login_failed.png')
            logger.info("📸 Скриншот: debug_login_failed.png")

            return False

    except Exception as e:
        logger.error(f"❌ Ошибка авторизации: {e}")

        # Отладочная информация
        try:
            page.screenshot(path='debug_login_error.png')
            logger.info("📸 Скриншот ошибки: debug_login_error.png")
        except:
            pass

        return False
