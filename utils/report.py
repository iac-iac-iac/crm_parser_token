import pandas as pd
from pathlib import Path
from database.db import Database
import config
from utils.logger import logger

def generate_excel_report(db: Database):
    """Генерация Excel-отчета"""
    try:
        logger.info("📊 Генерация отчета...")
        
        # Получаем данные
        accounts = db.get_all_accounts_summary()
        total_phones = db.get_total_phones()
        
        # ИСПРАВЛЕНИЕ: Проверка на пустые данные
        if not accounts:
            logger.warning("⚠️ Нет данных для отчета. БД пуста.")
            
            # Создаем пустой отчет
            Path(config.REPORT_PATH).parent.mkdir(parents=True, exist_ok=True)
            
            with pd.ExcelWriter(config.REPORT_PATH, engine='openpyxl') as writer:
                empty_df = pd.DataFrame({
                    'Сообщение': ['Данные отсутствуют. Запустите парсинг.']
                })
                empty_df.to_excel(writer, sheet_name='Информация', index=False)
            
            logger.info(f"✅ Пустой отчет сохранен: {config.REPORT_PATH}")
            return
        
        # Создаем DataFrame
        df = pd.DataFrame(accounts)
        
        # Переименовываем колонки
        df.rename(columns={
            'username': 'Название аккаунта',
            'account_id': 'ID аккаунта',
            'phones_count': 'Количество номеров',
            'status': 'Статус'
        }, inplace=True)
        
        # Переводим статусы
        status_map = {
            'pending': 'Ожидает',
            'in_progress': 'В процессе',
            'completed': 'Завершен',
            'failed': 'Ошибка'
        }
        df['Статус'] = df['Статус'].map(status_map)
        
        # Создаем Excel-файл
        Path(config.REPORT_PATH).parent.mkdir(parents=True, exist_ok=True)
        
        with pd.ExcelWriter(config.REPORT_PATH, engine='openpyxl') as writer:
            # Основная таблица
            df.to_excel(writer, sheet_name='Отчет по аккаунтам', index=False)
            
            # Итоговая статистика
            summary_df = pd.DataFrame({
                'Показатель': [
                    'Всего аккаунтов',
                    'Завершено',
                    'В процессе',
                    'Ожидает',
                    'Ошибок',
                    'Всего уникальных номеров'
                ],
                'Значение': [
                    len(accounts),
                    len([a for a in accounts if a['status'] == 'completed']),
                    len([a for a in accounts if a['status'] == 'in_progress']),
                    len([a for a in accounts if a['status'] == 'pending']),
                    len([a for a in accounts if a['status'] == 'failed']),
                    total_phones
                ]
            })
            summary_df.to_excel(writer, sheet_name='Статистика', index=False)
            
            # Автоширина колонок
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
        
        logger.info(f"✅ Отчет сохранен: {config.REPORT_PATH}")
        logger.info(f"📊 Всего номеров: {total_phones}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка генерации отчета: {e}", exc_info=True)
