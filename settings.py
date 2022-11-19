import os
import sys
from pathlib import Path

from dotenv import load_dotenv


dotenv_path = Path(".env")
load_dotenv(dotenv_path=dotenv_path)

# флаг включен / выключен боевой режим
PRODUCTION = os.getenv("PRODUCTION")
if PRODUCTION:
    current_dir = os.path.dirname(os.path.realpath(__file__))
    base_path = os.path.dirname(current_dir)
    sys.path.append(base_path)
    sys.path.append("/home/manage_report")

# путь к драйверу Chrome
CHROME_DRIVER_PATH = os.getenv("CHROME_DRIVER_PATH")

# путь к папке с cookies
COOKIES_PATH = os.getenv("COOKIES_PATH")

# путь к файлу с логами
LOG_FILE_PATH = os.getenv("LOG_FILE_PATH")

# прокси
PROXIES = {
    "http": os.getenv("PROXIES_HTTP"),
    "https": os.getenv("PROXIES_HTTPS"),
}

# заголовки запроса
HEADERS = {
    "accept": os.getenv("HEADERS_ACCEPT"),
    "user_agent": os.getenv("HEADERS_USER_AGENT"),
}
