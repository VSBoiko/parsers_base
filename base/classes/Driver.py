from selenium import webdriver

import settings


class Driver:
    """Класс для работы с вебдрайверами Selenium."""

    def __init__(self):
        """Инициализировать объект класса Driver."""
        self.__chromedriver_path = settings.CHROME_DRIVER_PATH
        self.__cookies_path = settings.COOKIES_PATH
        self.__driver = self.__initialize_driver()

    def __enter__(self):
        """Для менеджера контекста (with Driver() as *)"""
        return self.__driver

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Для менеджера контекста (with Driver() as *)"""
        self.__driver.close()
        self.__driver.quit()

    def __initialize_driver(self) -> webdriver:
        """Инициализировать драйвер с готовыми настройками.

        :return: драйвер с готовыми настройками.
        """
        options = webdriver.ChromeOptions()

        options.add_experimental_option("prefs", {
            "profile.default_content_setting_values.notifications": 2,
        })
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--no-sandbox")
        options.add_argument("--headless")
        options.add_argument("--disable-dev-shm-usage")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        options.add_argument(f"user-data-dir={self.__cookies_path}")

        driver: webdriver = webdriver.Chrome(executable_path=self.__chromedriver_path, options=options)

        return driver
