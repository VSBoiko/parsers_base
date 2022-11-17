import json

from Db import Db

from base.classes.FileManager import FileManager
from base.classes.Parser import Parser as BaseParser
from base.classes.Requests import Requests
from base.logger import logging


class Parser(BaseParser):
    """Класс для работы парсера."""

    # Ссылка на страницу со списком заказов
    orders_list_url = "https://old.zakupki.mos.ru/api/Cssp/Purchase/Query?queryDto=%7B%22filter%22%3A%7B%22" \
                      "auctionSpecificFilter%22%3A%7B%22stateIdIn%22%3A%5B19000002%5D%7D%2C%22" \
                      "needSpecificFilter%22%3A%7B%22stateIdIn%22%3A%5B20000002%5D%7D%2C%22" \
                      "tenderSpecificFilter%22%3A%7B%22stateIdIn%22%3A%5B5%5D%7D%7D%2C%22" \
                      "order%22%3A%5B%7B%22field%22%3A%22relevance%22%2C%22desc%22%3Atrue%7D%5D%2C%22" \
                      "withCount%22%3Atrue%2C%22skip%22%3A0%7D"

    # Типы заказов на сайте
    auction_type = "auction"
    need_type = "need"
    tender_type = "tender"

    # Список ID заказов, которые не отправлять по API
    dont_send_ids = [
        "4122001",  # test order
    ]

    # Список статусов заказа, которые отправлять по API
    current_state_ids = [
        20000002,
        19000002,
    ]

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
        self.db.create_table_orders()
        self.db.create_table_customers()

    def run(self):
        """Метод парсит сайт, сохраняет данные в БД и отправляет по API."""
        logging.info("Парсер начал работу")

        if self.is_parsing_site:
            orders = {}
            try:
                orders = self.__request(self.orders_list_url)
            except Exception as err:
                logging.error(f"Ошибка при запросе на получении списка заказов - {err}")

            self.__add_orders_to_db(orders, self.db)

        result = self.__send_orders_from_db(self.db)

        logging.info(f"Парсер закончил работу. "
                     f"{result.get('new_orders')} новых заказов отправлено. "
                     f"{result.get('errors')} заказов с ошибкой.")

    @staticmethod
    def __get_auction_lot_api_url(lot_id: str) -> str:
        """Получить URL на API страницу лота аукциона.

        :param lot_id: ID лота.

        :return: URL на API страницу лота аукциона.
        """
        return f"https://zakupki.mos.ru/newapi/api/Auction/GetAuctionItemAdditionalInfo?itemId={lot_id}"

    @staticmethod
    def __get_customer_api_url(customer_id: str) -> str:
        """Получить URL на API страницу заказчика.

        :param customer_id: ID заказчика.

        :return: URL на API страницу заказчика.
        """
        return f"https://zakupki.mos.ru/newapi/api/CompanyProfile/" \
               f"GetByCompanyId?companyId={customer_id}"

    @staticmethod
    def __get_customer_url(customer_id: str) -> str:
        """Получить URL на страницу заказчика.

        :param customer_id: ID заказчика.

        :return: URL на страницу заказчика.
        """
        return f"https://zakupki.mos.ru/companyProfile/customer/{customer_id}"

    @staticmethod
    def __get_document_url(document_id: str) -> str:
        """Получить URL на документ из заказа.

        :param document_id: ID документа.

        :return: URL на документ из заказа.
        """
        return f"https://zakupki.mos.ru/newapi/api/FileStorage/Download?id={document_id}"

    def __add_customer_to_db(self, customer: dict, db: Db) -> bool:
        """Добавить заказчика в БД.

        :param customer: данные заказчика;
        :param db: база данных.

        :return: флаг успешно / неуспешно прошло добавление.
        """
        customer_id = customer.get('id')
        if db.get_customer_by_id(customer_id):
            logging.debug(f"Заказчик уже существует в БД - ({customer_id})")
            return False

        api_url = self.__get_customer_api_url(customer_id)
        try:
            customer_data = self.__request(api_url)
        except Exception as err:
            logging.error(f"Ошибка при запросе инф-ции о заказчике - {err}")
            return False

        customer_url = self.__get_customer_url(customer_id)
        if customer_data.get("httpStatusCode") == 404:
            logging.error(f"Ошибка при получении заказчика: {customer_url}")
            return False

        success = db.add_customer(
            url=customer_url,
            customer_id=customer_id,
            customer_data=json.dumps(customer_data, ensure_ascii=False),
        )
        if not success:
            logging.debug(f"Заказчик {customer_id} не добавлен в БД")
            return False

        logging.debug(f"Заказчик {customer_id} успешно добавлен в БД")
        return True

    def __add_orders_to_db(self, orders: dict, db: Db):
        """Добавить заказы в БД.

        :param orders: список с заказами;
        :param db: база данных.
        """
        count_all_item = orders["count"]
        count = 0
        logging.info("Начало добавления заказов в БД")
        db_orders = db.get_all_orders()
        db_customers = db.get_all_customers()
        for order in orders["items"]:
            count += 1
            order_type = self.__get_order_type(order)
            order_id = self.__get_order_id(order_type, order)
            if order_id in db_orders:
                continue

            logging.debug(f"#{count} / {count_all_item}: Заказ ({order.get('number')}) {order.get('name')}")

            if not self.__check_order(order):
                continue

            customer = order.get('customers')[0]
            if customer.get("id") not in db_customers:
                self.__add_customer_to_db(customer, db)

            self.__add_order_to_db(db, order_type, order_id, order, customer.get("id"))
        logging.info("Конец добавления заказов в БД")

    def __add_order_to_db(self, db: Db, order_type: str, order_id: str,
                          order_data: dict, customer_id: str) -> bool:
        """Добавить заказ в БД.

        :param db: база данных;
        :param order_type: тип заказа;
        :param order_id: ID заказа;
        :param order_data: данные заказа;
        :param customer_id: ID заказчика.

        :return: флаг успешно / неуспешно прошло добавление.
        """
        db_order = db.get_order_by_order_id(order_id)
        if db_order:
            logging.debug(f"Заказ уже существует в БД - ({order_id})")
            return False

        order_api_url = self.__get_order_api_url(order_type, order_id)
        try:
            order_detail = self.__request(order_api_url)
        except Exception as err:
            logging.error(f"Ошибка при запросе детальной инф-ции о заказе - {err}")
            return False

        order_url = self.__get_order_url(order_type, order_id)
        if not self.__check_order_state(order_detail.get("state").get("id")):
            logging.error(f"Заказ имеет некорректный статус: {order_url}")
            db.add_order(
                url=order_url,
                order_type=order_type,
                order_id=order_id,
                order_data="Заказ имеет некорректный статус",
                order_detail=f"Статус - {order_detail.get('state').get('name')}",
                customer_id=customer_id,
                was_send=1,
            )
            return False

        if order_detail.get("httpStatusCode") == 404:
            logging.error(f"Ошибка при получении детальной инф-ции о заказе: {order_url}")
            return False

        success = db.add_order(
            url=order_url,
            order_type=order_type,
            order_id=order_id,
            order_data=json.dumps(order_data, ensure_ascii=False),
            order_detail=json.dumps(order_detail, ensure_ascii=False),
            customer_id=customer_id,
        )
        if not success:
            logging.debug(f"Заказ {order_url} не добавлен в БД")
            return False

        logging.debug(f"Заказ {order_id} успешно добавлен в БД")
        return True

    def __check_order(self, order) -> bool:
        """Проверить корректный ли заказ.

        :param order: данные заказа.

        :return: флаг корректный / не корректный заказ.
        """
        if order.get("number") in self.dont_send_ids:
            return False
        elif len(order.get('customers')) == 0:
            logging.error(f"Заказ не имеет заказчика: number = '{order.get('number')}'")
            return False

        return True

    def __check_order_state(self, state_id: int) -> bool:
        """Проверить корректный ли статус у заказа.

        :param state_id: ID статуса заказа.

        :return: флаг корректный / не корректный статус у заказа.
        """
        if state_id in self.current_state_ids:
            return True
        else:
            return False

    def __request(self, url: str) -> dict:
        """Запрос.

        :param url: адрес запроса.

        :return: словарь с результатом запроса.
        """
        response = self.requests.get(url)
        self._to_sleep()
        return response.json()

    def __formatted_order_need(self, order, customer) -> dict:
        """Получить отформатированные данные заказа типа Need.

        :param order: данные заказа;
        :param customer: данные заказчика.

        :return: отформатированные данные заказа типа Need.
        """
        order_data = json.loads(order.get("order_data"))
        order_detail = json.loads(order.get("order_detail"))
        order_url = order.get("url")

        customer_detail = json.loads(customer.get("customer_data"))
        result = {
            "fz": "ЗМО",
            "purchaseNumber": order_data.get("number"),
            "url": order_url,
            "title": order_data.get("name"),
            "purchaseType": "Закупка по потребности",
            "procedureInfo": {
                "endDate": order_data.get("endDate"),
            },
            "lots": [{
                "price": order_detail.get("nmck"),
                "customerRequirements": [{
                    "kladrPlaces": [{
                        "deliveryPlace": order_detail.get("deliveryPlace"),
                    }],
                }],
            }],
            "ETP": {
                "name": "zakupki.mos.ru",
            },
            "attachments": [{
                "docDescription": doc.get("name"),
                "url": self.__get_document_url(doc.get('id')),
            } for doc in order_detail.get("files")],
            "type": 2,
        }

        # customer
        if customer_detail.get("company").get("factAddress"):
            if "customer" not in result:
                result.update({"customer": {}})
            result["customer"].update({"factAddress": customer_detail.get("company").get("factAddress")})

        if customer_detail.get("company").get("inn"):
            if "customer" not in result:
                result.update({"customer": {}})
            result["customer"].update({"inn": customer_detail.get("company").get("inn")})

        if customer_detail.get("company").get("kpp"):
            if "customer" not in result:
                result.update({"customer": {}})
            result["customer"].update({"kpp": customer_detail.get("company").get("kpp")})

        # contactPerson
        contact_name = order_detail.get("contactPerson").split() if order_detail.get("contactPerson") else ["", ""]
        if len(contact_name) == 1:
            contact_name.append("")

        added_email = False
        if order_detail.get("contactEmail"):
            if "contactPerson" not in result:
                result.update({"contactPerson": {}})
            result["contactPerson"].update({"contactEMail": order_detail.get("contactEmail")})
            added_email = True

        added_phone = False
        if order_detail.get("contactPhone"):
            if "contactPerson" not in result:
                result.update({"contactPerson": {}})
            result["contactPerson"].update({"contactPhone": order_detail.get("contactPhone")})
            added_phone = True

        if added_email or added_phone:
            if contact_name[0]:
                if "contactPerson" not in result:
                    result.update({"contactPerson": {}})
                result["contactPerson"].update({"lastName": contact_name[0]})

            if contact_name[1]:
                if "contactPerson" not in result:
                    result.update({"contactPerson": {}})
                result["contactPerson"].update({"firstName": contact_name[1]})

        okpd_codes = [{"code": item.get("okpd").get("code")} for item in order_detail.get("items")]
        if okpd_codes:
            result.get("lots")[0].update({"lotItems": okpd_codes})

        return result

    def __formatted_order_auction(self, order: dict, customer: dict) -> dict:
        """Получить отформатированные данные заказа типа Auction.

        :param order: данные заказа;
        :param customer: данные заказчика.

        :return: отформатированные данные заказа типа Auction.
        """
        order_data = json.loads(order["order_data"])
        order_detail = json.loads(order["order_detail"])
        order_url = order.get("url")
        customer_data = json.loads(customer.get("customer_data"))

        deliveries = order_detail.get("deliveries")[0]

        result = {
            "fz": "ЗМО",
            "purchaseNumber": order_data.get("number"),
            "url": order_url,
            "title": order_data.get("name"),
            "purchaseType": "Котировочная сессия",
            "procedureInfo": {
                "endDate": order_data.get("endDate"),
            },
            "lots": [{
                "price": order_detail.get("startCost"),
                "customerRequirements": [{
                    "kladrPlaces": deliveries.get("deliveryPlace"),
                    "obesp_i": order_detail.get("contractGuaranteeAmount"),
                }],
            }],
            "ETP": {
                "name": "zakupki.mos.ru",
            },
            "attachments": [{
                "docDescription": doc.get("name"),
                "url": self.__get_document_url(doc.get('id')),
            } for doc in order_detail.get("files")],
            "type": 2,
        }

        # customer
        if customer_data.get("company").get("factAddress"):
            if "customer" not in result:
                result.update({"customer": {}})
            result["customer"].update({"factAddress": customer_data.get("company").get("factAddress")})

        if customer_data.get("company").get("inn"):
            if "customer" not in result:
                result.update({"customer": {}})
            result["customer"].update({"inn": customer_data.get("company").get("inn")})

        if customer_data.get("company").get("kpp"):
            if "customer" not in result:
                result.update({"customer": {}})
            result["customer"].update({"kpp": customer_data.get("company").get("kpp")})

        lots = []
        for item in order_detail.get("items"):
            lot = self.__get_auction_lot(item.get("id"))
            lots.append(lot)

        okpd_codes = [{"code": lot.get("okpd").get("code")} for lot in lots]
        if okpd_codes:
            result.get("lots")[0].update({"lotItems": okpd_codes})

        return result

    def __formatted_order_tender(self):
        """Получить отформатированные данные заказа типа Tender.

        [Метод не написан, т.к. заказов этого типа очень мало и их сложно парсить]
        """
        pass

    def __get_auction_lot(self, lot_id: str) -> dict:
        """Получить данные о лоте аукциона.

        :param lot_id: ID лота аукциона.

        :return: словарь с данными о лоте аукциона.
        """
        url = self.__get_auction_lot_api_url(lot_id)
        try:
            return self.__request(url)
        except Exception as err:
            logging.error(f"Ошибка при запросе детальной инф-ции о лоте аукциона - {err}")
            return {}

    def __get_formatted_order(self, order_type: str, order: dict, customer: dict) -> dict:
        """Получить отформатированные данные заказа для отправки по API.

        :param order_type: тип заказа;
        :param order: данные заказа;
        :param customer: данные заказчика.

        :return: отформатированные данные заказа для отправки по API.
        """
        formatted_order = {}
        if order_type == self.auction_type:
            formatted_order = self.__formatted_order_auction(order, customer)
        elif order_type == self.need_type:
            formatted_order = self.__formatted_order_need(order, customer)
        elif order_type == self.tender_type:
            formatted_order = {}

        return formatted_order

    def __get_order_api_url(self, order_type: str, order_id: str) -> str:
        """Получить URL на API страницу заказа.

        :param order_type: тип заказа;
        :param order_id: ID заказа.

        :return: URL на API страницу заказа.
        """
        if order_type == self.auction_type:
            return f"https://zakupki.mos.ru/newapi/api/" \
                   f"{order_type.capitalize()}/Get?{order_type}Id={order_id}"
        elif order_type == self.need_type:
            return f"https://zakupki.mos.ru/newapi/api/" \
                   f"{order_type.capitalize()}/Get?{order_type}Id={order_id}"
        elif order_type == self.tender_type:
            return f"https://old.zakupki.mos.ru/api/Cssp/" \
                   f"{order_type.capitalize()}/GetEntity?id={order_id}"

    def __get_order_id(self, order_type: str, order: dict) -> str:
        """Получить ID заказа.

        :param order_type: тип заказа;
        :param order: данные заказа.

        :return: ID заказа.
        """
        if order_type == self.auction_type:
            return str(order['auctionId'])
        elif order_type == self.need_type:
            return str(order['needId'])
        elif order_type == self.tender_type:
            return str(order['tenderId'])

    def __get_order_type(self, order: dict) -> str:
        """Получить тип заказа.

        :param order: данные заказа.

        :return: тип заказа.
        """
        if order.get("auctionId"):
            return self.auction_type
        elif order.get("needId"):
            return self.need_type
        elif order.get("tenderId"):
            return self.tender_type

    def __get_order_url(self, order_type: str, order_id: str) -> str:
        """Получить URL на страницу заказа.

        :param order_type: тип заказа;
        :param order_id: ID заказа.

        :return: URL на страницу заказа.
        """
        if order_type == self.auction_type:
            return f"https://zakupki.mos.ru/auction/{order_id}"
        elif order_type == self.need_type:
            return f"https://zakupki.mos.ru/need/{order_id}"
        elif order_type == self.tender_type:
            return f"https://old.zakupki.mos.ru/#/tenders/{order_id}"

    def __send_orders_from_db(self, db: Db):
        """Отправить заказы из БД по API.

        :param db: база данных.

        :return: словарь с результатами отправки.
        """
        orders = db.get_unsent_orders()
        count_all_orders = len(orders)
        if count_all_orders == 0:
            logging.info("Новых заказов нет")

        count, count_send, count_send_error = 0, 0, 0
        sent_orders = []
        for order in orders:
            count += 1
            iter_info = f"{count} / {count_all_orders}"
            order_type = order.get("order_type")
            customer = db.get_customer_by_id(order.get("customer_id"))

            if not customer:
                logging.error(f"{iter_info} По заказу {order.get('order_id')} в БД "
                              f"нет информации о заказчике {order.get('customer_id')}")
                count_send_error += 1
                continue

            try:
                formatted_order = self.__get_formatted_order(order_type, order, customer)
            except Exception as err:
                logging.error(f"{iter_info} Ошибка при формировании заказа для отправки по API: {err}")
                count_send_error += 1
                continue

            if not formatted_order:
                logging.debug(f"{iter_info} Заказ пустой: {order.get('url')}")
                count_send_error += 1
                continue

            if not self._send_order([formatted_order]):
                logging.debug(f"{iter_info} Заказ не отправлен по API: {order.get('url')}")
                count_send_error += 1
                continue

            sent_orders.append(formatted_order)

            if self.is_updating_order:
                db.successful_send(order.get("order_id"))
            logging.debug(f"{iter_info} Заказ успешно отправлен по API: {order.get('url')}")
            count_send += 1

        self.file_manager.write_json_file("last_result.json", sent_orders)

        return {
            "new_orders": count_send,
            "errors": count_send_error,
        }
