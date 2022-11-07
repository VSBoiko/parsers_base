import random
from time import sleep

import requests

import settings


class Requests:
    """Класс для запросов."""

    def __init__(self):
        """Инициализировать объект класса Requests."""
        # Заголовки запроса
        self.__headers = settings.HEADERS

        # Прокси
        self.__proxies = settings.PROXIES

    def get_headers(self) -> dict:
        """Получить заголовки запроса.

        :return: заголовки запроса.
        """
        return self.__headers

    def get_proxies(self) -> dict:
        """Получить прокси.

        :return: прокси.
        """
        return self.__proxies

    def get(self, url: str, use_proxies: bool = False):
        """Запрос через библиотеку requests.

        :param url: адрес запроса;
        :param use_proxies: флаг использовать / не использовать прокси.

        :return: результат запроса.
        """
        params = {
            "url": url,
            "headers": self.get_headers(),
            "timeout": 10,
        }
        if use_proxies:
            params.update({
                "proxies": self.get_proxies(),
            })
        response = requests.get(**params)
        return response

    def request_by_webdriver(self, url: str, driver) -> str:
        """Запрос с использованием драйвера.

        :param url: адрес запроса;
        :param driver: драйвер.

        :return: результат запроса.
        """
        driver.get(url)
        sleep(random.randrange(3, 5))
        return driver.page_source

    def set_headers(self, headers: dict):
        """Установить заголовки запроса."""
        self.__headers = headers

    def set_proxies(self, proxies: dict):
        """Установить прокси."""
        self.__proxies = proxies
