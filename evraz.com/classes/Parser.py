import datetime
import json
import os
import re
from pprint import pprint

from bs4 import BeautifulSoup

from classes.BaseParser import BaseParser
from classes.ParserDb import ParserDb


class Parser(BaseParser):
    __current_statuses_codes = [
        "Опубликовано"
    ]

    __current_customers = [
        {
            "value": "Евраз Ванадий Тула АО",
            "factAddress": "",
            "inn": "7105008754",
            "kpp": "710501001"
        },
        {
            "value": "РУК ООО",
            "factAddress": "",
            "inn": "4253029657",
            "kpp": "425301001"
        },
        {
            "value": "ЕВРАЗ НТМК АО",
            "factAddress": "",
            "inn": "6623000680",
            "kpp": "662301001"
        },
        {
            "value": "ЕВРАЗ ЗСМК АО",
            "factAddress": "",
            "inn": "4218000951",
            "kpp": "421801001"
        },
        {
            "value": "ЕВРАЗ КГОК АО",
            "factAddress": "",
            "inn": "6615001962",
            "kpp": "668101001"
        },
        {
            "value": "АО ЕВРАЗ НТМК",
            "factAddress": "",
            "inn": "6623000680",
            "kpp": "662301001"
        },
        {
            "value": "ООО ЕВРАЗ Узловая",
            "factAddress": "",
            "inn": "7117030553",
            "kpp": "711701001"
        }
    ]

    __location_file = "matches.json"
    __default_region = "Москва"

    def start(self):
        time_start = datetime.datetime.now()
        print(f"[PARSER] Парсер начал работу в {time_start.strftime('%d.%m.%Y, %H:%M:%S')}")

        self.add_logger_info("Парсер начал работу")

        data_filepath = "./data.json"
        db = ParserDb("avrazcom.db")
        db.create_table_orders()

        count = 0
        count_added = 0
        count_errors = 0
        if self._is_parsing_site:
            pages_number = self.__get_pages_count()
            for page_number in range(1, pages_number + 1):
                url = self.__get_items_page_url(str(page_number))

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

    def __add_data_to_db(self, json_filepath: str, db: ParserDb):
        data = self.read_json_file(json_filepath)
        count_all_item = len(data["items"])
        count = 0
        count_add = 0
        count_errors = 0
        print("[START] Начало добавления заказов в БД")
        for item in data["items"]:
            count += 1
            iter_info = f"#{count} / {count_all_item}"

            item_url = self.__get_item_url(item)

            if db.get_order_by_url(item_url):
                continue

            item_info = self.__get_item_info(item)

            print(f"{iter_info}: [ORDER] Заказ ({item_info.get('number')}) {item_info.get('name')}")
            # self.add_logger_info(f"Заказ ({item_info.get('number')}) {item_info.get('name')}")
            if not self.__check_order(db, item_info):
                count_errors += 1
                continue
            else:
                is_add = self.__add_order_to_db(
                    db=db,
                    url=item_url,
                    order_type="",
                    order_id=item_info.get("number"),
                    order_data=item,
                    order_detail=item_info,
                    customer={}
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

    def __check_order(self, db, item_info: dict) -> bool:
        number = item_info.get("number")
        status = item_info.get("status")
        customer = item_info.get("customer")

        # формат на сайте - 10.10.2022 09:38, Мск
        end_datetime = item_info.get("date_end")
        if end_datetime:
            end_date_list = end_datetime.split()
            if end_date_list:
                end_date = self.__get_date_from_str(end_date_list.pop(0))
            else:
                return False
        else:
            return False

        if not number:
            print(f"number = {number}")
            return False
        elif status not in self.__current_statuses_codes:
            print(f"status = {status}")
            return False
        elif not customer:
            print(f"customer = {customer}")
            return False
        elif end_date is False or datetime.datetime.now() > end_date:
            print(f"end_date = {end_date}")
            return False

        db_order = db.get_order_by_order_id(number)
        if db_order:
            # print(f"[ALREADY EXIST] Заказ уже существует в БД - ({order_id})")
            # self.add_logger_info(f"Заказ уже существует в БД - ({order_id})")
            return False

        return True

    def __create_data_file(self, url: str, json_filepath: str) -> bool:
        page_source = self._request_by_webdriver(url)
        response = page_source.split('<html><head><meta name="color-scheme" content="light dark">'
                                     '</head><body>'
                                     '<pre style="word-wrap: break-word; white-space: pre-wrap;">')[1]
        response = response.split("</pre></body></html>")[0]
        try:
            response_json = json.loads(response)
            if response_json:
                self.write_json_file(json_filepath, response_json)
                print(f"[FILE CREATED] Файл {json_filepath} со списком заказов успешно создан")
                self.add_logger_info(f"Файл {json_filepath} со списком заказов успешно создан")
                return True
            else:
                return False
        except Exception as err:
            print(f"[ERROR] Ошибка при создании файло со списком заказов.")
            self.add_logger_error(f"Ошибка при создании файло со списком заказов.")
            return False

    def __formatted_order(self, order) -> dict:
        order_detail = json.loads(order.get("order_detail"))
        customer = order_detail.get("customer")

        end_datetime = order_detail.get("date_end")
        if end_datetime:
            end_date_list = end_datetime.split()
            if end_date_list:
                end_date = end_date_list.pop(0)
            else:
                return {}
        else:
            return {}

        result = {
            "fz": "Коммерческие",
            "purchaseNumber": order_detail.get("number"),
            "url": order_detail.get("url"),
            "title": order_detail.get("name"),
            "purchaseType": "Запрос предложений",
            "procedureInfo": {
                "endDate": end_date,
            },
            "customer": {
                "fullName": customer.get("value"),
                "inn": customer.get("inn"),
                "kpp": customer.get("kpp"),
                # "factAddress": "",
            },
            "ETP": {
                "name": "evraz.com"
            },
            "type": 2
        }

        # contactPerson
        contact = order_detail.get("contacts")
        if contact != {}:
            contact_name = contact.get("name").split()
            result.update({"contactPerson": {}})
            if len(contact_name) >= 2:
                result["contactPerson"].update({
                    "lastName": contact_name[0],
                    "firstName": contact_name[1]
                })
            elif len(contact_name) == 1:
                result["contactPerson"].update({
                    "lastName": contact_name[0],
                })

            contact_email = contact.get("email")
            if contact_email:
                result["contactPerson"].update({
                    "contactEMail": contact_email
                })

        # region
        tender_region = order_detail.get("region")
        if tender_region:
            delivery_place = tender_region
        else:
            delivery_place = self.__default_region

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

    def __get_customer(self, customer_name: str) -> dict:
        try:
            customer = list(filter(
                lambda x: x.get("value") == customer_name,
                self.__current_customers
            ))[0]
            return customer
        except Exception as err:
            return {}

    def __get_date_from_str(self, date: str):
        try:
            date_str = datetime.datetime.strptime(date, "%d.%m.%Y")
        except Exception as err:
            return False

        return date_str

    def __get_items_page_url(self, page_number: str) -> str:
        return f"https://www.evraz.com/ru/api/suppliers/?page={page_number}"

    def __get_item_page_source(self, item_url: str) -> str:
        try:
            page_source = self._request_by_webdriver(item_url)
        except Exception as err:
            return ""
        return page_source

    def __get_item_info(self, item: dict):
        item_url = self.__get_item_url(item)
        page_source = self._request_by_webdriver(item_url)
        soup = BeautifulSoup(page_source, "lxml")
        customer = self.__get_item_customer_name(soup)

        return {
            "url": item_url,
            "category": self.__get_item_category(item),
            "number": self.__get_item_number(soup),
            "name": self.__get_item_name(soup),
            "customer": self.__get_customer(customer),
            "organizator_name": self.__get_item_organizator_name(soup),
            "region": self.__get_item_region(soup),
            "date_start": self.__get_item_date_start(soup),
            "date_end": self.__get_item_date_end(soup),
            "contacts": self.__get_item_contacts(soup),
            "status": self.__get_item_status(soup),
            "type": self.__get_item_status(soup),
        }

    def __get_item_status(self, soup: BeautifulSoup) -> str:
        try:
            status = soup.find(
                "td",
                class_="z-table-col z-table-col--body",
                text="Статус на площадке"
            ).findNext("td").text
        except Exception as err:
            return ""

        return status.strip()

    def __get_item_number(self, soup: BeautifulSoup) -> str:
        try:
            item_number = soup.find(
                "div",
                class_="suppliers-detail__caption-item"
            ).find_all("span").pop().text
        except Exception as err:
            return ""

        return item_number

    def __get_item_customer_name(self, soup: BeautifulSoup) -> str:
        try:
            customer_name = soup.find(
                "td",
                class_="z-table-col z-table-col--body",
                text="Заказчик"
            ).findNext("td").text
        except Exception as err:
            return ""

        return customer_name.strip()

    def __get_item_name(self, soup: BeautifulSoup) -> str:
        try:
            name = soup.find(
                "td",
                class_="z-table-col z-table-col--body",
                text="Наименование"
            ).findNext("td").text
        except Exception as err:
            return ""

        return name.strip()

    def __get_item_type(self, soup: BeautifulSoup) -> str:
        try:
            name = soup.find(
                "td",
                class_="z-table-col z-table-col--body",
                text="Наименование"
            ).findNext("td").text
        except Exception as err:
            return ""

        return name.strip()

    def __get_item_organizator_name(self, soup: BeautifulSoup) -> str:
        try:
            organizator_name = soup.find(
                "td",
                class_="z-table-col z-table-col--body",
                text="Организатор"
            ).findNext("td").text
        except Exception as err:
            return ""

        return organizator_name.strip()

    def __get_item_region(self, soup: BeautifulSoup) -> str:
        try:
            region = soup.find(
                "td",
                class_="z-table-col z-table-col--body",
                text="Регион"
            ).findNext("td").text
        except Exception as err:
            return ""

        return region.strip()

    def __get_item_date_start(self, soup: BeautifulSoup) -> str:
        try:
            date_start = soup.find(
                "td",
                class_="z-table-col z-table-col--body suppliers-detail__table-date",
                text="Дата начала подачи заявок"
            ).findNext("td").find("span").text
        except Exception as err:
            return ""

        return date_start.strip()

    def __get_item_date_end(self, soup: BeautifulSoup) -> str:
        try:
            date_end = soup.find(
                "td",
                class_="z-table-col z-table-col--body suppliers-detail__table-date",
                text="Дата окончания подачи заявок"
            ).findNext("td").find("span").text
        except Exception as err:
            return ""

        return date_end.strip()

    def __get_item_contacts(self, soup: BeautifulSoup) -> dict:
        try:
            td = soup.find(
                "td",
                class_="z-table-col z-table-col--body",
                text="Контакты для связи"
            ).findNext("td")
            contacts = {
                "name": td.find("span").find("span").text,
                "email": td.find("a").text,

            }
        except Exception as err:
            return {}

        return contacts

    def __get_item_url(self, item: dict) -> str:
        link = item.get("subject").get("link")
        return f"https://www.evraz.com{link}"

    def __get_item_category(self, item: dict) -> str:
        if "category" in item:
            return item.get("category")
        else:
            return ""

    def __get_pages_count(self) -> int:
        url = self.__get_items_page_url("1")
        page_source = self._request_by_webdriver(url)

        response = page_source.split('<html><head><meta name="color-scheme" content="light dark">'
                                     '</head><body>'
                                     '<pre style="word-wrap: break-word; white-space: pre-wrap;">')[1]
        response = response.split("</pre></body></html>")[0]
        try:
            response_json = json.loads(response)
            if response_json and "nav" in response_json:
                nav = response_json.get("nav")
                return int(nav.get("total")) if "total" in nav else 0
            else:
                return 0
        except Exception as err:
            return 0

    def __is_current_customer(self, customer_name: str) -> bool:
        customers = [x.get("value") for x in self.__current_customers]
        return True if customer_name in customers else False

    def __send_orders_from_db(self, db: ParserDb):
        orders = db.get_unsent_orders()
        count, count_send, count_send_error = 0, 0, 0
        print("[START] Начало отправки заказов по API")
        if len(orders) == 0:
            print("[INFO] Новых заказов нет")
            self.add_logger_info("Новых заказов нет")

        success_send_orders = []

        customer_name = []
        customer_name_empty = 0
        customer_name_not_empty = 0
        region = []
        region_empty = 0
        region_not_empty = 0
        status = []
        status_empty = 0
        status_not_empty = 0
        contacts = []
        contacts_empty = 0
        contacts_not_empty = 0
        for order in orders:
            count += 1

            # order_data = json.loads(order.get("order_data"))
            # order_detail = json.loads(order.get("order_detail"))
            #
            # pprint(order_data)
            # pprint(order_detail)

            # pprint(order_data.get("link"))

            # if order_detail.get("customer_name") not in customer_name:
            #     if order_detail.get("customer_name") == "":
            #         customer_name.append(order_detail.get("url"))
            #         customer_name_empty += 1
            #     else:
            #         customer_name.append(order_detail.get("customer_name"))
            # if order_detail.get("customer_name") != "":
            #     customer_name_not_empty += 1

            # if order_detail.get("region") not in region:
            #     if order_detail.get("region") == "":
            #         region.append(order_detail.get("url"))
            #         region_empty += 1
            #     else:
            #         region.append(order_detail.get("region"))
            # if order_detail.get("region") != "":
            #     region_not_empty += 1

            # if order_detail.get("status") not in status:
            #     if order_detail.get("status") == "":
            #         status.append(order_detail.get("url"))
            #         status_empty += 1
            #     else:
            #         status.append(order_detail.get("status"))
            # if order_detail.get("status") != "":
            #     status_not_empty += 1
            #
            # if order_detail.get("contacts") not in contacts:
            #     if order_detail.get("contacts") == "":
            #         contacts.append(order_detail.get("url"))
            #         contacts_empty += 1
            #     else:
            #         contacts.append(order_detail.get("contacts"))
            # if order_detail.get("contacts") != "":
            #     contacts_not_empty += 1

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
                    success_send_orders.append(formatted_order)
                else:
                    print(f"[ERROR] Заказ не отправлен по API: {order.get('url')}")
                    self.add_logger_error(f"Заказ не отправлен по API: {order.get('url')}")
                    count_send_error += 1
            else:
                print(f"[EMPTY ORDER] Заказ пустой: {order.get('url')}")
                self.add_logger_info(f"Заказ пустой: {order.get('url')}")
                count_send_error += 1

        # pprint(contacts)
        # pprint(f"{contacts_not_empty} / {contacts_empty} / {count}")

        print("[FINISH] Конец отправки заказов по API\n")

        self.write_json_file("last_result.json", success_send_orders)

        return {
            "new_orders": count_send,
            "errors": count_send_error,
        }
