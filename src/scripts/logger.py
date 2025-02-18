import logging
import sys

class LoggerConfig:
    """
    Класс для настройки логирования
    """
    @staticmethod
    def get_logger(name: str, level=logging.INFO):
        logger = logging.getLogger(name)
        if not logger.hasHandlers():  # Проверяем, есть ли уже обработчики, чтобы избежать дублирования
            logger.setLevel(level)
            
            # Создаем обработчик вывода в консоль
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(level)
            
            # Формат логов
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            console_handler.setFormatter(formatter)
            
            # Добавляем обработчик
            logger.addHandler(console_handler)
        
        return logger