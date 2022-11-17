import datetime

from base.classes.Db import Db as BaseDb
from base.logger import logging


class Db(BaseDb):
    """Класс для работы с БД парсера."""

    def __init__(self, db_name: str):
        """Инициализировать объект класса Db.

        :param db_name: название файла с БД (например, "basedb.db").
        """
        super().__init__(db_name)

    def add_customer(self, url: str, customer_id: str, customer_data: str) -> bool:
        """Добавить заказчика в БД.

        :param url: ссылка на страницу заказчика;
        :param customer_id: ID заказчика;
        :param customer_data: данные о заказчике в формате json-строки.

        :return: флаг успешно / не успешно прошла запись.
        """
        query = "INSERT INTO " \
                "customers(url, customer_id, customer_data) " \
                "VALUES (?, ?, ?)"
        try:
            self.insert(query, [(url, customer_id, customer_data)])
        except Exception as err:
            logging.error(f"Ошибка при добавлении в таблицу 'customers' - {err}")
            return False
        else:
            return True

    def add_order(self, url: str, order_type: str, order_id: str,
                  order_data: str, order_detail: str, customer_id: str,
                  was_send: int = 0) -> bool:
        """Добавить заказ в БД.

        :param url: ссылка на страницу заказа;
        :param order_type: тип заказа;
        :param order_id: ID заказа;
        :param order_data: данные о заказе в формате json-строки (со стр-цы со всеми заказами);
        :param order_detail: детальные данные о заказе в формате json-строки (со стр-цы заказа);
        :param customer_id: ID заказчика;
        :param was_send: заказ отправлен / не отправлен по API.

        :return: флаг успешно / не успешно прошла запись.
        """
        query = "INSERT INTO " \
                "orders(url, order_type, order_id, order_data, order_detail, " \
                "customer_id, was_send) " \
                "VALUES (?, ?, ?, ?, ?, ?, ?)"
        values = [(url, order_type, order_id, order_data, order_detail, customer_id, was_send)]

        try:
            self.insert(query, values)
        except Exception as err:
            logging.error(f"Ошибка при добавлении в таблицу 'orders' - {err}")
            return False
        else:
            return True

    def create_table_customers(self):
        """Создать таблицу для заказчиков."""
        self.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                url TEXT DEFAULT "",
                customer_id TEXT DEFAULT "",
                customer_data TEXT DEFAULT ""
            )
        """)

    def create_table_orders(self):
        """Создать таблицу для заказов."""
        self.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                url TEXT DEFAULT "",
                order_type TEXT DEFAULT "",
                order_id TEXT DEFAULT "",
                order_data TEXT DEFAULT "",
                order_detail TEXT DEFAULT "",
                customer_id TEXT DEFAULT "",
                was_send BOOLEAN DEFAULT 0
            )
        """)

    def formatted_customer(self, customer_row: list) -> dict:
        """Отформатировать информацию о заказчике.

        :param customer_row: список с информацией о заказчике.

        :return: словарь с информацией о заказчике.
        """
        return {
            "created_at": self.get_date_from_str(customer_row[0]),
            "url": customer_row[1],
            "customer_id": customer_row[2],
            "customer_data": customer_row[3],
        }

    def formatted_order(self, order_row: list) -> dict:
        """Отформатировать информацию о заказе.

        :param order_row: список с информацией о заказе.

        :return: словарь с информацией о заказе.
        """
        return {
            "created_at": self.get_date_from_str(order_row[0]),
            "url": order_row[1],
            "order_type": order_row[2],
            "order_id": order_row[3],
            "order_data": order_row[4],
            "order_detail": order_row[5],
            "customer_id": order_row[6],
            "was_send": order_row[7],
        }

    def get_all_customers(self) -> dict:
        """Получить все данные из таблицы с заказчиками.

        :return: словарь, где ключи = ID заказчиков, а значения = сами заказчики.
        """
        query = "SELECT * FROM customers"
        rows = self.query(query)
        return {row[2]: self.formatted_customer(row) for row in rows}

    def get_all_orders(self) -> dict:
        """Получить все данные из таблицы с заказами.

        :return: словарь, где ключи = ID заказов, а значения = сами заказы.
        """
        query = "SELECT * FROM orders"
        rows = self.query(query)
        return {row[3]: self.formatted_order(row) for row in rows}

    def get_all_orders_ids(self) -> list:
        """Получить список со всеми ID заказов.

        :return: список со всеми ID заказов.
        """
        query = "SELECT order_id FROM orders"
        rows = self.query(query)
        return [row[0] for row in rows]

    def get_customer_by_id(self, customer_id: str) -> dict:
        """Получить информацию о заказчике по ID заказчика (customer_id)

        :param customer_id: ID заказчика.

        :return: словарь с данными о заказчике.
        """
        query = f"SELECT * FROM customers WHERE customer_id={customer_id}"
        rows = self.query(query)
        customers = [self.formatted_customer(row) for row in rows]
        if customers:
            return customers.pop()
        else:
            return {}

    def get_order_by_order_id(self, order_id: str) -> dict:
        """Получить информацию о заказе по ID заказа (order_id)

        :param order_id: ID заказа.

        :return: словарь с данными о заказе.
        """
        query = f"SELECT * FROM orders WHERE order_id={order_id}"
        rows = self.query(query)
        orders = [self.formatted_order(row) for row in rows]
        if orders:
            return orders.pop()
        else:
            return {}

    def get_unsent_orders(self) -> list:
        """Получить список заказов, которые не отправлялись по API.

        :return: список заказов, которые не отправлялись по API.
        """
        query = "SELECT * FROM orders WHERE was_send = 0"
        rows = self.query(query)
        return [self.formatted_order(row) for row in rows]

    def successful_send(self, order_id: str) -> bool:
        """Отметить в таблице, что заказ отправлен по API.

        :param order_id: ID заказа.

        :return: флаг успешно / неуспешно прошло обновление записи.
        """
        query = "UPDATE orders SET was_send=1, order_data=NULL, order_detail=NULL WHERE order_id=?"
        return self.insert(query, [(order_id,)])

    @staticmethod
    def get_date_from_str(str_date: str, str_date_format: str = "%Y-%m-%d %H:%M:%S") -> datetime:
        """Получить дату из строки.

        :param str_date: строка с датой;
        :param str_date_format: формат даты в строке str_date.

        :return: дата.
        """
        return datetime.datetime.strptime(str_date, str_date_format)
