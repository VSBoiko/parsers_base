from datetime import datetime, timedelta
import json
import re

from Db import Db

from base.classes.FileManager import FileManager
from base.classes.Parser import Parser as BaseParser
from base.classes.Requests import Requests
from base.logger import logging


class Parser(BaseParser):
    data_filepath = "./data.json"

    tender_customer = "tenders_organizer"
    tender_end_date = "tenders_end_date_accepting_offers"
    tender_status = "tenders_status"
    tender_type = "tenders_tender_type"
    tender_number = "tenders_tender_oebs_number"
    tender_number_2 = "tenders_number_tenders_on_tenders"
    tender_contact = "tender_responsible"
    tender_category = "tenders_category"
    tender_region = "tenders_region"
    tender_docs = "tenders_attachments"
    current_statuses_codes = [
        "tenders_open_for_proposals",  # Прием предложений
        # "tenders_closed_for_proposals",              # Закрыт прием предложений
        # "tenders_acceptance_of_offers_suspended",    # Приостановлен прием предложений
        # "tenders_completed",                         # Завершена
        # "tenders_canceled"                           # Отменена
    ]
    current_customers = [
        {
            "code": "84",
            "value": "ПАО МТС",
            "factAddress": "",
            "inn": "7740000076",
            "kpp": "770901001",
        },
        {
            "code": "54629",
            "value": "ПАО МГТС",
            "factAddress": "",
            "inn": "7710016640",
            "kpp": "770501001",
        },
        {
            "code": "83",
            "value": "Корпоративный центр",
            "factAddress": "",
            "inn": "7733631345",
            "kpp": "772901001",
        },
        {
            "code": "35329",
            "value": "АО РТК",
            "factAddress": "",
            "inn": "7709356049",
            "kpp": "770901001",
        },
        {
            "code": "OOO MTS DIDZHITAL",
            "value": "ООО МТС ДИДЖИТАЛ",
            "factAddress": "",
            "inn": "7707767501",
            "kpp": "772501001",
        },
        {
            "code": "52513",
            "value": "ООО Спутниковое ТВ",
            "factAddress": "",
            "inn": "7709909783",
            "kpp": "770201001",
        },
        {
            "code": "120785",
            "value": "ООО МТС МЕДИА",
            "factAddress": "",
            "inn": "7707434100",
            "kpp": "770701001",
        },
        {
            "code": "125060",
            "value": "ООО МТС Энтертейнмент",
            "factAddress": "",
            "inn": "9709058198",
            "kpp": "770901001",
        },
        {
            "code": "135500",
            "value": "ООО Прикладная Техника",
            "factAddress": "",
            "inn": "9725039570",
            "kpp": "772501001",
        },
        {
            "code": "120784",
            "value": "ООО МТС ИИ",
            "factAddress": "",
            "inn": "9725021438",
            "kpp": "772501001",
        },
        {
            "code": "73894",
            "value": "ООО МТС ИТ",
            "factAddress": "",
            "inn": "7707767501",
            "kpp": "772501001",
        },
    ]

    location_file = "matches.json"
    default_region = "Москва"

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
        
        # заполнить таблицу с заказчиками
        for customer in self.current_customers:
            self.__add_customer_to_db(customer, self.db)

    def run(self):
        """Метод парсит сайт, сохраняет данные в БД и отправляет по API."""
        logging.info("Парсер начал работу")

        count = 0
        count_added = 0
        count_errors = 0
        
        if self.is_parsing_site:
            page_size = 10
            count_empty = 0
            count_pages = self._get_count_pages(page_size)

            for page in range(0, count_pages):
                orders = {}
                url = self._get_tenders_list_url(page_size, page)

                try:
                    orders = self.__request(url)
                except Exception as err:
                    logging.error(f"Ошибка при запросе на получении списка заказов - {err}")

                all_items, added, errors = self.__add_orders_to_db(orders["data"], self.db)

                count += all_items
                count_added += added
                count_errors += errors

                if added == 0:
                    count_empty += 1
                else:
                    count_empty = 0

                if count_empty == 20:
                    break

        result = self.__send_orders_from_db(self.db)

        time_finish = datetime.now()
        logging.info(f"[PARSER] Парсер закончил работу в {time_finish.strftime('%d.%m.%Y, %H:%M:%S')}."
                     f"БД: {count} всего обработано заказов, {count_added} новых заказов в БД, "
                     f"{count_errors} заказов с ошибкой."
                     f"Отправка по API: {result.get('new_orders')} новых заказов отправлено, "
                     f"{result.get('errors')} заказов с ошибкой.")

    @staticmethod
    def _get_document_url(document_id: str) -> str:
        return f"https://tenders.mts.ru.ru/newapi/api/FileStorage/Download?id={document_id}"

    @staticmethod
    def _get_tenders_list_url(page_size: int, page: int) -> str:
        return f"https://tenders.mts.ru/api/v1/tenders?" \
               f"pageSize={page_size}&" \
               f"page={page}&" \
               f"attributesForSort=tenders_publication_date%2Cdesc"
    
    def __add_customer_to_db(self, customer: dict, db: Db):
        """Добавить заказчика в БД.

        :param customer: данные заказчика;
        :param db: база данных.
        """
        customer_id = customer.get('code')
        db_customers = db.get_all_customers()
        if customer_id not in db_customers:
            db.add_customer(
                url="",
                customer_id=customer_id,
                customer_data=json.dumps(customer, ensure_ascii=False),
            )

    def __add_orders_to_db(self, orders: dict, db: Db):
        """Добавить заказы в БД.

        :param orders: список с заказами;
        :param db: база данных.
        """
        count_all_item = len(orders)
        count = 0
        count_add = 0
        count_errors = 0
        logging.info("Начало добавления заказов в БД")
        for order in orders:
            count += 1
            order_id = self.__get_order_id(order)

            logging.debug(f"#{count} / {count_all_item}: Заказ {order_id}")

            if not self.__check_order(order):
                count_errors += 1
                continue

            customer = self.__get_tender_customer(order)
            if not customer.get("code"):
                logging.error(f"Заказ {order_id} не имеет заказчика")
                count_errors += 1
                continue

            db.add_order(
                url=self.__get_order_url(order_id),
                order_type=self.__get_order_type(order),
                order_id=order_id,
                order_data=json.dumps(order, ensure_ascii=False),
                order_detail=json.dumps({}, ensure_ascii=False),
                customer_id=customer.get("code"),
            )
            logging.debug(f"Заказ {order_id} успешно добавлен в БД")
            count_add += 1

        logging.info(f"Конец добавления заказов в БД. "
                     f"Обработано {count_all_item} заказов. "
                     f"Добавлено {count_add} заказов. "
                     f"Пропущено {count_errors} заказов.")

        return count_all_item, count_add, count_errors

    def __check_order(self, order) -> bool:
        """Проверить корректный ли заказ.

        :param order: данные заказа.

        :return: флаг корректный / не корректный заказ.
        """
        status = self.__get_tender_status(order)
        tender_number = self.__get_tender_number(order)
        order_id = self.__get_order_id(order)
        end_date = self.__get_tender_end_date(order)

        if status.get("code") not in self.current_statuses_codes:
            logging.debug(f"У заказа {order_id} некорректный статус - {status.get('code')}")
            return False
        elif not tender_number:
            logging.debug(f"У заказа {order_id} нет номера")
            return False
        elif end_date == "" or datetime.today() > datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1):
            logging.debug(f"У заказа {order_id} дата окончания = {end_date}")
            return False

        db_order = self.db.get_order_by_order_id(order_id)
        if db_order:
            logging.debug(f"Заказ уже существует в БД - ({order_id})")
            return False

        return True

    def __define_region(self, text_from_post: str):
        location = ""
        with open(self.location_file, encoding="utf-8") as file:
            matches_file = json.load(file)

        for region, value in matches_file.items():
            for locality in value["matches"]:
                pattern = f"{locality.title()}"
                result = re.search(pattern=pattern, string=text_from_post)
                if result is not None:
                    location = f"{region}, {locality.capitalize()}"
                    return location

        return location

    def __formatted_order(self, order) -> dict:
        order_url = order.get("url")
        order_data = json.loads(order.get("order_data"))
        
        tender_number = self.__get_tender_number(order_data)
        tender_type = self.__get_tender_type(order_data)
        tender_end_date = self.__get_tender_end_date(order_data)
        
        customer_db = self.db.get_customer_by_id(order.get("customer_id"))
        customer = json.loads(customer_db.get("customer_data"))

        name = order_data.get("name")
        categories = self.__get_tender_category(order_data)
        categories_names = [c.get("value") for c in categories]
        if len(categories_names) >= 2:
            full_name = f"{name} [{', '.join(categories_names[0:2])}]"
        elif len(categories_names) == 1:
            full_name = f"{name} [{', '.join(categories_names[0:1])}]"
        else:
            full_name = name
        result = {
            "fz": "Коммерческие",
            "purchaseNumber": tender_number,
            "url": order_url,
            "title": full_name,
            "purchaseType": tender_type.get("value"),
            "procedureInfo": {
                "endDate": tender_end_date,
            },
            "customer": {
                "fullName": customer.get("value"),
                "inn": customer.get("inn"),
                "kpp": customer.get("kpp"),
                # "factAddress": "",
            },
            "ETP": {
                "name": "tenders.mts.ru",
            },
            "type": 2,
        }

        # contactPerson - Иванов Иван Иванович <ivan@test.ru>
        contact = self.__get_tender_contact(order_data)
        if contact.get("value"):
            contact_list = contact.get("value").split("<")
            result.update({"contactPerson": {}})
            if contact_list and contact_list[0] != "":
                contact_name_list = contact_list[0].split()
                result["contactPerson"].update({
                    "lastName": contact_name_list[0],
                    "firstName": contact_name_list[1],
                })

            if len(contact_list) > 1:
                contact_email_list = contact_list[1].split(">")
                result["contactPerson"].update({
                    "contactEMail": contact_email_list[0],
                })

        # docs
        docs = self.__get_tender_docs(order_data)
        if docs:
            attachments = [{
                "docDescription": doc.get("name"),
                "url": doc.get('url'),
            } for doc in docs]
            result.update({"attachment": attachments})

        # region
        tender_region_in_name = self.__define_region(name)
        tender_regions = self.__get_tender_region(order_data)
        delivery_place = ""
        if tender_region_in_name:
            delivery_place = tender_region_in_name
        else:
            if tender_regions:
                for region in tender_regions:
                    delivery_place = self.__define_region(region.get("value"))
                    if delivery_place:
                        break

            if not delivery_place:
                delivery_place = self.__define_region(self.default_region)

        result.update({"lots": [
            {"customerRequirements": [
                {
                    "kladrPlaces": [
                        {
                            "deliveryPlace": delivery_place,
                        },
                    ],
                },
            ]},
        ]})

        return result

    def __get_customer(self, customer_code: dict) -> dict:
        try:
            customer = list(filter(
                lambda x: x.get("code") == customer_code,
                self.current_customers,
            ))[0]
            return customer
        except Exception as err:
            logging.error(f"Не удалось получить заказчика - {err}")
            return {}

    def _get_count_pages(self, page_size: int) -> int:
        url = self._get_tenders_list_url(page_size, 0)
        response = self.__request(url)
        if "totalPages" in response:
            return int(response.get("totalPages"))
        else:
            return -1

    def __get_order_id(self, item: dict) -> str:
        return item.get("id")

    def __get_order_type(self, item: dict) -> str:
        return item.get("attributeCategories")[0].get("code")

    def __get_order_url(self, item_id: str) -> str:
        return f"https://tenders.mts.ru/tenders/{item_id}"

    def __get_tender_category(self, item_detail: dict) -> list:
        try:
            categories = list(filter(
                lambda x: x.get("code") == self.tender_category,
                item_detail.get("attributeCategories")[0].get("attributes"),
            ))[0]

            if isinstance(categories, dict):
                return categories.get("value")
            else:
                return []
        except Exception as err:
            logging.error(f"Не удалось получить категорию заказ - {err}")
            return []

    def __get_tender_contact(self, item_detail: dict) -> dict:
        try:
            result = list(filter(
                lambda x: x.get("code") == self.tender_contact,
                item_detail.get("attributeCategories")[0].get("attributes"),
            ))[0]

            if isinstance(result, dict):
                return result.get("value")
            else:
                return {}
        except Exception as err:
            logging.error(f"Не удалось получить контакт заказ - {err}")
            return {}

    def __get_tender_customer(self, item_detail: dict) -> dict:
        try:
            customer = list(filter(
                lambda x: x.get("code") == self.tender_customer,
                item_detail.get("attributeCategories")[0].get("attributes"),
            ))[0]

            if isinstance(customer, dict):
                return customer.get("value")
            else:
                return {}
        except Exception as err:
            logging.error(f"Не удалось определить заказчика тендера - {err}")
            return {}

    def __get_tender_docs(self, item_detail: dict) -> list:
        try:
            docs = list(filter(
                lambda x: x.get("code") == self.tender_docs,
                item_detail.get("attributeCategories")[0].get("attributes"),
            ))[0]

            if isinstance(docs, dict):
                return docs.get("value")
            else:
                return []
        except Exception as err:
            logging.error(f"Не удалось получить документы заказ - {err}")
            return []

    def __get_tender_end_date(self, item_detail: dict) -> str:
        try:
            result = list(filter(
                lambda x: x.get("code") == self.tender_end_date,
                item_detail.get("attributeCategories")[0].get("attributes"),
            ))[0]

            if isinstance(result, dict):
                return result.get("value")
            else:
                return ""
        except Exception as err:
            logging.error(f"Не удалось определить дату окончания заказа - {err}")
            return ""

    def __get_tender_number(self, item_detail: dict) -> str:
        try:
            result = list(
                filter(
                    lambda x: x.get("code") == self.tender_number or x.get("code") == self.tender_number_2,
                    item_detail.get("attributeCategories")[0].get("attributes"),
                ),
            )[0]

            if isinstance(result, dict):
                return result.get("value")
            else:
                return ""
        except Exception as err:
            logging.error(f"Не удалось определить номер заказа - {err}")
            return ""

    def __get_tender_region(self, item_detail: dict) -> list:
        try:
            regions = list(filter(
                lambda x: x.get("code") == self.tender_region,
                item_detail.get("attributeCategories")[0].get("attributes"),
            ))[0]

            if isinstance(regions, dict):
                return regions.get("value")
            else:
                return []
        except Exception as err:
            logging.error(f"Не удалось определить регион заказа - {err}")
            return []

    def __get_tender_type(self, item_detail: dict) -> dict:
        try:
            result = list(filter(
                lambda x: x.get("code") == self.tender_type,
                item_detail.get("attributeCategories")[0].get("attributes"),
            ))[0]

            if isinstance(result, dict):
                return result.get("value")
            else:
                return {}
        except Exception as err:
            logging.error(f"Не удалось определить тип тендера - {err}")
            return {}

    def __get_tender_status(self, item_detail: dict) -> dict:
        try:
            result = list(filter(
                lambda x: x.get("code") == self.tender_status,
                item_detail.get("attributeCategories")[0].get("attributes"),
            ))[0]

            if isinstance(result, dict):
                return result.get("value")
            else:
                return {}
        except Exception as err:
            logging.error(f"Не удалось определить статуса заказа - {err}")
            return {}

    def __is_current_customer(self, customer: dict) -> bool:
        code = customer.get("code")
        codes = [x.get("code") for x in self.current_customers]
        return True if code in codes else False

    def __request(self, url: str) -> dict:
        """Запрос.

        :param url: адрес запроса.

        :return: словарь с результатом запроса.
        """
        response = self.requests.get(url)
        self._to_sleep()
        return response.json()

    def __send_orders_from_db(self, db: Db):
        orders = db.get_unsent_orders()
        count_all_orders = len(orders)
        if count_all_orders == 0:
            logging.info("Новых заказов нет")

        count, count_send, count_send_error = 0, 0, 0
        sent_orders = []
        for order in orders:
            count += 1
            iter_info = f"{count} / {count_all_orders}"

            db_customer = self.db.get_customer_by_id(order.get("customer_id"))
            tender_customer = json.loads(db_customer.get("customer_data"))
            if not self.__is_current_customer(tender_customer):
                logging.error(f"{iter_info} По заказу {order.get('order_id')} в БД "
                              f"нет информации о заказчике {order.get('customer_id')}")
                count_send_error += 1
                continue

            try:
                formatted_order = self.__formatted_order(order)
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
