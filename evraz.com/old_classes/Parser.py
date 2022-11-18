import datetime
import json
import os
import re

from classes.BaseParser import BaseParser
from classes.ParserDb import ParserDb


class Parser(BaseParser):
    __tender_customer = "tenders_organizer"
    __tender_end_date = "tenders_end_date_accepting_offers"
    __tender_status = "tenders_status"
    __tender_type = "tenders_tender_type"
    __tender_number = "tenders_tender_oebs_number"
    __tender_number_2 = "tenders_number_tenders_on_tenders"
    __tender_contact = "tender_responsible"
    __tender_category = "tenders_category"
    __tender_region = "tenders_region"
    __tender_docs = "tenders_attachments"
    __current_statuses_codes = [
        "tenders_open_for_proposals",                # Прием предложений
        # "tenders_closed_for_proposals",              # Закрыт прием предложений
        # "tenders_acceptance_of_offers_suspended",    # Приостановлен прием предложений
        # "tenders_completed",                         # Завершена
        # "tenders_canceled"                           # Отменена
    ]
    __current_customers = [
        {
            "code": "84",
            "value": "ПАО МТС",
            "factAddress": "",
            "inn": "7740000076",
            "kpp": "770901001"
        },
        {
            "code": "54629",
            "value": "ПАО МГТС",
            "factAddress": "",
            "inn": "7710016640",
            "kpp": "770501001"
        },
        {
            "code": "83",
            "value": "Корпоративный центр",
            "factAddress": "",
            "inn": "7733631345",
            "kpp": "772901001"
        },
        {
            "code": "35329",
            "value": "АО РТК",
            "factAddress": "",
            "inn": "7709356049",
            "kpp": "770901001"
        },
        {
            "code": "OOO MTS DIDZHITAL",
            "value": "ООО МТС ДИДЖИТАЛ",
            "factAddress": "",
            "inn": "7707767501",
            "kpp": "772501001"
        },
        {
            "code": "52513",
            "value": "ООО Спутниковое ТВ",
            "factAddress": "",
            "inn": "7709909783",
            "kpp": "770201001"
        },
        {
            "code": "120785",
            "value": "ООО МТС МЕДИА",
            "factAddress": "",
            "inn": "7707434100",
            "kpp": "770701001"
        },
        {
            "code": "125060",
            "value": "ООО МТС Энтертейнмент",
            "factAddress": "",
            "inn": "9709058198",
            "kpp": "770901001"
        },
        {
            "code": "135500",
            "value": "ООО Прикладная Техника",
            "factAddress": "",
            "inn": "9725039570",
            "kpp": "772501001"
        },
        {
            "code": "120784",
            "value": "ООО МТС ИИ",
            "factAddress": "",
            "inn": "9725021438",
            "kpp": "772501001"
        },
        {
            "code": "73894",
            "value": "ООО МТС ИТ",
            "factAddress": "",
            "inn": "7707767501",
            "kpp": "772501001"
        }
    ]

    __location_file = "matches.json"
    __default_region = "Москва"

    def start(self):
        time_start = datetime.datetime.now()
        print(f"[PARSER] Парсер начал работу в {time_start.strftime('%d.%m.%Y, %H:%M:%S')}")

        self.add_logger_info("Парсер начал работу")

        data_filepath = "./data.json"
        db = ParserDb("tendersmts.db")
        db.create_table_orders()

        if os.path.exists(data_filepath):
            print(f"[ERROR] Файл {data_filepath} существует, новые заказы не могут быть загружены")
            self.add_logger_error(f"Файл {data_filepath} существует, новые заказы не могут быть загружены")
            os.remove(f"{data_filepath}")
            self.add_logger_info(f"{data_filepath} успешно удален")

        count = 0
        count_added = 0
        count_errors = 0
        if self._is_parsing_site:
            page_size = 200
            count_pages = self.__get_count_pages(page_size)

            for page in range(0, count_pages):
                url = self._get_tenders_list_url(page_size, page)

                self.__create_data_file(url, data_filepath)
                all_items, added, errors = self.__add_data_to_db(data_filepath, db)

                count += all_items
                count_added += added
                count_errors += errors

                os.remove(f"{data_filepath}")

        result = self.__send_orders_from_db(db)

        time_finish = datetime.datetime.now()
        print(f"[PARSER] Парсер закончил работу в {time_finish.strftime('%d.%m.%Y, %H:%M:%S')}.\n"
              f"БД\n"
              f"{count} всего обработано заказов. "
              f"{count_added} новых заказов в БД. "
              f"{count_errors} заказов с ошибкой.\n\n"
              f"Отправка по API\n"
              f"{result.get('new_orders')} новых заказов отправлено. "
              f"{result.get('errors')} заказов с ошибкой.")

        self.add_logger_info(f"Парсер закончил работу. "
                             f"{result.get('new_orders')} новых заказов отправлено. "
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

    def __add_data_to_db(self, json_filepath: str, db: ParserDb):
        data = self.read_json_file(json_filepath)
        count_all_item = len(data["data"])
        count = 0
        count_add = 0
        count_errors = 0
        print("[START] Начало добавления заказов в БД")
        for item in data["data"]:
            count += 1
            iter_info = f"#{count} / {count_all_item}"
            item_id = self.__get_item_id(item)
            # print(f"{iter_info}: [ORDER] Заказ ({item_id}) {item.get('name')}")

            # self.add_logger_info(f"Заказ ({item.get('number')}) {item.get('name')}")
            if not self.__check_order(db, item_id, item):
                continue

            item_type = self.__get_item_type(item)
            item_url = self.__get_item_url(item_id)
            customer = self.__get_tender_customer(item)

            if not customer.get("code"):
                print(f"[ERROR] Ошибка при получении детальной инф-ции о заказе: {item_url}")
                self.add_logger_error(f"Ошибка при получении детальной инф-ции о заказе: {item_url}")
                count_errors += 1
            else:
                is_add = self.__add_order_to_db(
                    db=db,
                    url=item_url,
                    order_type=item_type,
                    order_id=item_id,
                    order_data=item,
                    order_detail={},
                    customer=customer
                )
                count_add += 1 if is_add else 0

        print(f"[FINISH] Конец добавления заказов в БД.\n "
              f"Обработано {count_all_item} заказов. "
              f"Добавлено {count_add} заказов. "
              f"С ошибкой {count_errors} заказов.")

        return count_all_item, count_add, count_errors

    def __add_order_to_db(self, db: ParserDb, url: str, order_type: str, order_id: str,
                          order_data: dict, order_detail: dict, customer: dict) -> bool:
        db.add_order(
            url=url,
            order_type=order_type,
            order_id=order_id,
            order_data=json.dumps(order_data, ensure_ascii=False),
            order_detail=json.dumps(order_detail, ensure_ascii=False),
            customer=json.dumps(customer, ensure_ascii=False)
        )
        print(f"[SUCCESS] Заказ {order_id} успешно добавлен в БД")
        self.add_logger_info(f"Заказ {order_id} успешно добавлен в БД")
        return True

    def __check_order(self, db, order_id: str, order) -> bool:
        status = self.__get_tender_status(order)
        tender_number = self.__get_tender_number(order)
        end_date = self.__get_tender_end_date(order)

        if status.get("code") not in self.__current_statuses_codes:
            return False
        elif not tender_number:
            return False
        elif end_date == "" or datetime.datetime.now() > self.__get_date(end_date):
            return False

        db_order = db.get_order_by_order_id(order_id)
        if db_order:
            # print(f"[ALREADY EXIST] Заказ уже существует в БД - ({order_id})")
            # self.add_logger_info(f"Заказ уже существует в БД - ({order_id})")
            return False

        return True

    def __create_data_file(self, url: str, json_filepath: str) -> bool:
        response = self._request(url)
        if response:
            self.write_json_file(json_filepath, response.json())
            print(f"[FILE CREATED] Файл {json_filepath} со списком заказов успешно создан")
            self.add_logger_info(f"Файл {json_filepath} со списком заказов успешно создан")
            return True
        else:
            return False

    def __define_region(self, text_from_post: str):
        location = ""
        with open(self.__location_file, encoding="utf-8") as file:
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
        tender_customer = json.loads(order.get("customer"))
        customer = self.__get_customer(tender_customer.get("code"))

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
                "endDate": tender_end_date
            },
            "customer": {
                "fullName": customer.get("value"),
                "inn": customer.get("inn"),
                "kpp": customer.get("kpp"),
                # "factAddress": "",
            },
            "ETP": {
                "name": "tenders.mts.ru"
            },
            "type": 2
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
                    "firstName": contact_name_list[1]
                })

            if len(contact_list) > 1:
                contact_email_list = contact_list[1].split(">")
                result["contactPerson"].update({
                    "contactEMail": contact_email_list[0]
                })

        # docs
        docs = self.__get_tender_docs(order_data)
        if docs:
            attachments = [{
                "docDescription": doc.get("name"),
                "url": doc.get('url')
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
                delivery_place = self.__define_region(self.__default_region)

        result.update({"lots": [
            {"customerRequirements": [
                {
                    "kladrPlaces": [
                        {
                            "deliveryPlace": delivery_place,
                        }
                    ]
                }
            ]}
        ]})

        return result

    def __get_customer(self, customer_code: dict) -> dict:
        try:
            customer = list(filter(
                    lambda x: x.get("code") == customer_code,
                    self.__current_customers
                ))[0]
            return customer
        except Exception as err:
            return {}

    def __get_count_pages(self, page_size: int) -> int:
        url = self._get_tenders_list_url(page_size, 0)
        response = self._request(url)
        if response:
            result = response.json()
            if "totalPages" in result:
                return int(result.get("totalPages"))
            else:
                return -1
        else:
            return -1

    def __get_date(self, tender_date: str):
        return datetime.datetime.strptime(tender_date, "%Y-%m-%d")

    def __get_item(self, detail_url: str) -> dict:
        response = self._request(detail_url)
        if response:
            return response.json()
        else:
            return {}

    def __get_item_api_url(self, item_id: str) -> str:
        return f"https://tenders.mts.ru/api/v1/tenders/{item_id}"

    def __get_item_id(self, item: dict) -> str:
        return item.get("id")

    def __get_item_type(self, item: dict) -> str:
        return item.get("attributeCategories")[0].get("code")

    def __get_item_url(self, item_id: str) -> str:
        return f"https://tenders.mts.ru/tenders/{item_id}"

    def __get_tender_category(self, item_detail: dict) -> list:
        try:
            customer = list(filter(
                lambda x: x.get("code") == self.__tender_category,
                item_detail.get("attributeCategories")[0].get("attributes")
            ))[0]

            if isinstance(customer, dict):
                return customer.get("value")
            else:
                return []
        except Exception as err:
            return []

    def __get_tender_contact(self, item_detail: dict) -> dict:
        try:
            result = list(filter(
                lambda x: x.get("code") == self.__tender_contact,
                item_detail.get("attributeCategories")[0].get("attributes")
            ))[0]

            if isinstance(result, dict):
                return result.get("value")
            else:
                return {}
        except Exception as err:
            return {}

    def __get_tender_customer(self, item_detail: dict) -> dict:
        try:
            customer = list(filter(
                lambda x: x.get("code") == self.__tender_customer,
                item_detail.get("attributeCategories")[0].get("attributes")
            ))[0]

            if isinstance(customer, dict):
                return customer.get("value")
            else:
                return {}
        except Exception as err:
            return {}

    def __get_tender_docs(self, item_detail: dict) -> list:
        try:
            customer = list(filter(
                lambda x: x.get("code") == self.__tender_docs,
                item_detail.get("attributeCategories")[0].get("attributes")
            ))[0]

            if isinstance(customer, dict):
                return customer.get("value")
            else:
                return []
        except Exception as err:
            return []

    def __get_tender_end_date(self, item_detail: dict) -> str:
        try:
            result = list(filter(
                lambda x: x.get("code") == self.__tender_end_date,
                item_detail.get("attributeCategories")[0].get("attributes")
            ))[0]

            if isinstance(result, dict):
                return result.get("value")
            else:
                return ""
        except Exception as err:
            return ""

    def __get_tender_number(self, item_detail: dict) -> str:
        try:
            result = list(filter(
                lambda x: x.get("code") == self.__tender_number or
                x.get("code") == self.__tender_number_2,
                item_detail.get("attributeCategories")[0].get("attributes")
            ))[0]

            if isinstance(result, dict):
                return result.get("value")
            else:
                return ""
        except Exception as err:
            return ""

    def __get_tender_region(self, item_detail: dict) -> list:
        try:
            customer = list(filter(
                lambda x: x.get("code") == self.__tender_region,
                item_detail.get("attributeCategories")[0].get("attributes")
            ))[0]

            if isinstance(customer, dict):
                return customer.get("value")
            else:
                return []
        except Exception as err:
            return []

    def __get_tender_type(self, item_detail: dict) -> dict:
        try:
            result = list(filter(
                lambda x: x.get("code") == self.__tender_type,
                item_detail.get("attributeCategories")[0].get("attributes")
            ))[0]

            if isinstance(result, dict):
                return result.get("value")
            else:
                return {}
        except Exception as err:
            return {}

    def __get_tender_status(self, item_detail: dict) -> dict:
        try:
            result = list(filter(
                lambda x: x.get("code") == self.__tender_status,
                item_detail.get("attributeCategories")[0].get("attributes")
            ))[0]

            if isinstance(result, dict):
                return result.get("value")
            else:
                return {}
        except Exception as err:
            return {}

    def __is_current_customer(self, customer: dict) -> bool:
        code = customer.get("code")
        codes = [x.get("code") for x in self.__current_customers]
        return True if code in codes else False

    def __send_orders_from_db(self, db: ParserDb):
        orders = db.get_unsent_orders()
        count, count_send, count_send_error = 0, 0, 0
        print("[START] Начало отправки заказов по API")
        if len(orders) == 0:
            print("[INFO] Новых заказов нет")
            self.add_logger_info("Новых заказов нет")
        for order in orders:
            count += 1

            tender_customer = json.loads(order.get("customer"))
            if not self.__is_current_customer(tender_customer):
                print(f"[ERROR] По заказу {order.get('order_id')} "
                      f"нет информации о заказчике {tender_customer.get('value')}")
                self.add_logger_error(f"По заказу {order.get('order_id')} "
                                      f"нет информации о заказчике {tender_customer.get('value')}")
                continue

            formatted_order = {}
            try:
                formatted_order = self.__formatted_order(order)
            except Exception as err:
                print(f"[ERROR] Ошибка при создании заказа для отправки по API: {order.get('url')}")
                self.add_logger_error(f"Ошибка при создании заказа для отправки по API: {order.get('url')}")
                self.add_logger_error(err)
            if formatted_order:
                if self._send_orders([formatted_order]):
                    if self._is_upd_order_after_sending:
                        db.update_send_on_success(order.get("order_id"))
                    print(f"[SUCCESS] Заказ успешно отправлен по API: {order.get('url')}")
                    self.add_logger_info(f"Заказ успешно отправлен по API: {order.get('url')}")
                    count_send += 1
                else:
                    print(f"[ERROR] Заказ не отправлен по API: {order.get('url')}")
                    self.add_logger_error(f"Заказ не отправлен по API: {order.get('url')}")
                    count_send_error += 1
            else:
                print(f"[EMPTY ORDER] Заказ пустой: {order.get('url')}")
                self.add_logger_info(f"Заказ пустой: {order.get('url')}")
                count_send_error += 1
        print("[FINISH] Конец отправки заказов по API\n")

        return {
            "new_orders": count_send,
            "errors": count_send_error,
        }
