from base.classes.Db import Db as BaseDb


class Db(BaseDb):
    """Класс для работы с БД парсера."""

    def __init__(self, db_name: str):
        """Инициализировать объект класса Db.

        :param db_name: название файла с БД (например, "basedb.db").
        """
        super().__init__(db_name)
