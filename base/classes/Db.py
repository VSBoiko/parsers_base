import sqlite3


class Db:
    """Класс для работы с БД SQLite3."""

    def __init__(self, db_name: str):
        """Инициализировать объект класса Db.

        :param db_name: название файла с БД (например, "basedb.db").
        """
        # Название файла БД
        self.__db_name = db_name

        # Подключение к БД
        self.__connection = sqlite3.connect(self.__db_name)

    def execute(self, query: str):
        """Запрос к БД.

        :param query: текст запроса.
        """
        self.__connection.cursor().execute(query)

    def query(self, query: str) -> list:
        """Запрос к БД с получением результата этого запроса (через fetchall).

        :param query: текст запроса.

        :return: результат запроса.
        """
        return self.__connection.execute(query).fetchall()

    def get_db_name(self) -> str:
        """Получить название БД."""
        return self.__db_name

    def insert(self, query: str, data: list):
        """Записать в БД.

        :param query: текст запроса;
        :param data: данные, которые требуется записать.
        """
        with self.__connection as connection:
            connection.executemany(query, data)
