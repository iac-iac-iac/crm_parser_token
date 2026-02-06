import time
import random
import re
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeout
from typing import List
import config
from database.db import Database
from utils.logger import logger

class PhoneScraper:
    def __init__(self, page: Page, db: Database):
        self.page = page
        self.db = db
    
    def scrape_account(self, account_id: str, token_url: str, start_page: int = 1):
        """Парсинг всех номеров из аккаунта"""
        try:
            logger.info(f"📞 Парсинг аккаунта {account_id}...")
            
            # Переход по токен-ссылке
            self.page.goto(token_url)
            
            # Ждем загрузки страницы
            time.sleep(5)
            
            # НОВОЕ: Устанавливаем 50 записей на странице
            self._set_page_size(50)
            
            # Обновляем статус
            self.db.update_account_status(account_id, 'in_progress')
            
            current_page = start_page
            total_phones = 0
            
            while True:
                logger.info(f"  📄 Страница {current_page}...")
                
                # Если не первая страница, переходим на нужную
                if current_page > 1:
                    self._go_to_page(current_page)
                    time.sleep(3)
                
                # Парсим номера на текущей странице
                phones = self._parse_phones_on_page()
                
                if phones:
                    added = self.db.add_phones(account_id, phones)
                    total_phones += added
                    logger.info(f"  ✅ Добавлено {added} номеров (всего: {total_phones})")
                else:
                    logger.info(f"  ℹ️ Номеров не найдено на странице {current_page}")
                
                # Сохраняем прогресс
                self.db.update_account_status(account_id, 'in_progress', current_page)
                
                # Проверяем наличие следующей страницы
                if not self._has_next_page():
                    logger.info(f"  📭 Достигнута последняя страница")
                    break
                
                # Переход на следующую страницу
                current_page += 1
                time.sleep(random.uniform(*config.DELAY_BETWEEN_REQUESTS))
            
            # Завершаем обработку аккаунта
            self.db.update_account_status(account_id, 'completed')
            logger.info(f"✅ Аккаунт {account_id} обработан: {total_phones} номеров")
            
            return total_phones
            
        except Exception as e:
            logger.error(f"❌ Ошибка парсинга аккаунта {account_id}: {e}")
            self.db.update_account_status(account_id, 'failed')
            return 0
    
    def _set_page_size(self, size: int = 50):
        """Установить количество записей на странице"""
        try:
            logger.info(f"  ⚙️ Устанавливаю {size} записей на странице...")
            
            # Ищем кнопку dropdown "Длина страницы"
            dropdown_selectors = [
                '//button[contains(., "Длина страницы")]',
                'button[data-toggle="dropdown"]',
                '.btn-group button.dropdown-toggle',
            ]
            
            dropdown_button = None
            for selector in dropdown_selectors:
                dropdown_button = self.page.query_selector(selector)
                if dropdown_button:
                    logger.debug(f"    Найдена кнопка dropdown: {selector}")
                    break
            
            if not dropdown_button:
                logger.warning(f"  ⚠️ Кнопка dropdown не найдена")
                return
            
            # Кликаем на кнопку чтобы открыть меню
            dropdown_button.click()
            time.sleep(0.5)
            
            # Ищем ссылку с нужным размером
            # Вариант 1: По точному href
            link_selector = f'a[href*="updatepagesize?pageSize={size}"]'
            size_link = self.page.query_selector(link_selector)
            
            # Вариант 2: По тексту
            if not size_link:
                size_link = self.page.query_selector(f'ul.dropdown-menu a:has-text("{size}")')
            
            # Вариант 3: XPath
            if not size_link:
                size_link = self.page.query_selector(f'//ul[contains(@class, "dropdown-menu")]//a[text()="{size}"]')
            
            if size_link:
                # Проверяем что это не активная опция
                parent_li = self.page.query_selector(f'//a[contains(@href, "pageSize={size}")]/parent::li')
                
                if parent_li and 'active' in parent_li.get_attribute('class'):
                    logger.info(f"  ✅ Уже установлено {size} записей")
                    # Закрываем меню
                    self.page.keyboard.press('Escape')
                    return
                
                # Кликаем на ссылку
                size_link.click()
                time.sleep(3)  # Ждем перезагрузки страницы
                logger.info(f"  ✅ Установлено {size} записей")
            else:
                logger.warning(f"  ⚠️ Опция {size} не найдена в меню")
                # Закрываем меню
                self.page.keyboard.press('Escape')
            
        except Exception as e:
            logger.warning(f"  ⚠️ Не удалось установить размер страницы: {e}")
    
    def _parse_phones_on_page(self) -> List[str]:
        """Парсинг номеров на текущей странице"""
        phones = []
        
        try:
            # Ждем появления таблицы
            time.sleep(2)
            
            selectors = [
                'table tbody tr',
                'table tr',
                'tr[data-key]',
                '.grid-view tbody tr',
                'div[role="row"]',
            ]
            
            rows = []
            for selector in selectors:
                rows = self.page.query_selector_all(selector)
                if len(rows) > 0:
                    logger.debug(f"   ✓ Найдено {len(rows)} строк (селектор: {selector})")
                    break
            
            if len(rows) == 0:
                logger.warning("   ✗ Таблица не найдена")
                self.page.screenshot(path='debug_phones_page.png')
                logger.info("   📸 Скриншот: debug_phones_page.png")
                return []
            
            # Парсим каждую строку
            for idx, row in enumerate(rows):
                try:
                    row_text = row.inner_text()
                    
                    # Пропускаем заголовки
                    if 'ТЕЛЕФОН' in row_text or 'ПРОЕКТ' in row_text:
                        continue
                    
                    # ВАРИАНТ 1: Regex поиск 11-значных номеров
                    phone_matches = re.findall(r'\b(7\d{10})\b', row_text)
                    
                    if phone_matches:
                        for phone in phone_matches:
                            if phone not in phones:
                                phones.append(phone)
                    else:
                        # ВАРИАНТ 2: Поиск по ячейкам
                        phone_cells = row.query_selector_all('td')
                        
                        for cell in phone_cells:
                            cell_text = cell.inner_text().strip()
                            
                            if cell_text.isdigit() and len(cell_text) == 11 and cell_text.startswith('7'):
                                if cell_text not in phones:
                                    phones.append(cell_text)
                
                except Exception as e:
                    logger.debug(f"   Ошибка парсинга строки {idx}: {e}")
                    continue
            
            # Дедупликация
            phones = list(set(phones))
            
        except Exception as e:
            logger.error(f"Ошибка парсинга номеров: {e}")
        
        return phones
    
    def _has_next_page(self) -> bool:
        """Проверка наличия следующей страницы"""
        try:
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
    
    def _go_to_page(self, page_num: int):
        """Переход на указанную страницу"""
        try:
            current_url = self.page.url
            
            # Добавляем/обновляем параметр page
            if '?' in current_url:
                base_url = current_url.split('?')[0]
                params = current_url.split('?')[1]
                
                params_list = [p for p in params.split('&') if not p.startswith('page=')]
                params_list.append(f'page={page_num}')
                
                new_url = f"{base_url}?{'&'.join(params_list)}"
            else:
                new_url = f"{current_url}?page={page_num}"
            
            self.page.goto(new_url)
            time.sleep(3)
            
        except Exception as e:
            logger.error(f"Ошибка перехода на страницу {page_num}: {e}")
