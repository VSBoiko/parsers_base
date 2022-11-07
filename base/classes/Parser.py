import random
from time import sleep


from base.logger import logging

import settings


class Parser:
    def __init__(self,
                 parser_name: str,
                 is_sleeping: bool = True,
                 is_sending_orders: bool = True,
                 is_updating_order: bool = True,
                 is_parsing_site: bool = True):
        """Инициализировать объект класса Parser.

        :param parser_name: имя парсера;
        :param is_sleeping: флаг засыпать / не засыпать на несколько секунд после запроса;
        :param is_sending_orders: флаг отправлять / не отправлять заказы по API;
        :param is_updating_order: флаг обновлять / не обновлять запись в БД после отправки;
        :param is_parsing_site: флаг включить / выключить парсинг сайта;
        """
        self.parser_name = parser_name
        self.is_sleeping = is_sleeping
        self.is_sending_orders = is_sending_orders
        self.is_updating_order = is_updating_order
        self.is_parsing_site = is_parsing_site

    def _get_parser_name(self):
        return self.parser_name

    def _send_order(self, order: list) -> bool:
        data = {
            "name": self._get_parser_name(),
            "data": order,
        }
        try:
            if self.is_sending_orders and settings.PRODUCTION:
                from Send_report.Utils import send_to_api
                settings.send_to_api(data)
            return True
        except Exception as err:
            logging.error(f"Ошибка при отправке заказа по API - {err}")
            return False

    def _to_sleep(self, start: int = 2, stop: int = 4):
        if self.is_sleeping:
            sleep(random.randrange(start, stop))
