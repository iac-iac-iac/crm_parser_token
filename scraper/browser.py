from playwright.sync_api import sync_playwright, Browser, Page, TimeoutError
import config


class BrowserManager:
    def __init__(self, headless: bool = config.HEADLESS):
        self.headless = headless
        self.playwright = None
        self.browser = None
        self.context = None

    def __enter__(self):
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=self.headless,
            args=['--disable-blink-features=AutomationControlled']
        )
        self.context = self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            # ДОБАВЛЕНО: разрешение на clipboard
            permissions=['clipboard-read', 'clipboard-write']
        )
        self.context.set_default_timeout(config.BROWSER_TIMEOUT)
        self.context.set_default_navigation_timeout(config.PAGE_LOAD_TIMEOUT)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()

    def new_page(self) -> Page:
        """Создать новую страницу"""
        page = self.context.new_page()
        # Устанавливаем увеличенный таймаут для страницы
        page.set_default_timeout(config.BROWSER_TIMEOUT)
        page.set_default_navigation_timeout(config.PAGE_LOAD_TIMEOUT)
        return page
