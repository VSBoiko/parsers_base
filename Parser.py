from Db import Db

from base.classes.FileManager import FileManager
from base.classes.Parser import Parser as BaseParser
from base.classes.Requests import Requests


class Parser(BaseParser):
    """Класс парсера сайта."""
    def __init__(self,
                 parser_name: str,
                 is_sleeping: bool = True,
                 is_sending_orders: bool = True,
                 is_updating_order: bool = True,
                 is_parsing_site: bool = True):
        """Инициализировать объект класса Parser.

        :param parser_name:
        :param is_sleeping:
        :param is_sending_orders:
        :param is_updating_order:
        :param is_parsing_site:
        """
        super().__init__(
            parser_name=parser_name,
            is_sleeping=is_sleeping,
            is_sending_orders=is_sending_orders,
            is_updating_order=is_updating_order,
            is_parsing_site=is_parsing_site,
        )

        # аттрибут для работы с запросами
        self.requests = Requests()

        # аттрибут для работы с файлами
        self.file_manager = FileManager()

        # создание БД с двумя таблицами
        self.db = Db(f"{parser_name}.db")

    def run(self):
        """Метод парсит сайт, сохраняет данные в БД и отправляет по API."""
        pass
