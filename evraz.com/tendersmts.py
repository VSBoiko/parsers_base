# https://www.evraz.com/ru/suppliers/actual-procurement-procedures/
# http://www.tender.pro/api/tenders/list?sid=&company_id=&order=3&tender_type=90&tender_state=1&country=0&basis=0&tender_name=&tender_id=&company_name=&good_name=&dateb=&datee=&dateb2=&datee2=
# https://srm.brusnika.ru/trades?documents=N4IgLgTghgJgpgZTlCBjAFgbQA4Fc3pQDOcRAdCShgLogBcoEpuANmALID289oAblBa449EKhQwQAXwA0IThHgReIAJaS6AJjlhVYFiLohAKCCAGEEBCIKZByAtlDABJVJwB2olBE4B3APoxvLl4S1iDYEKp2EACeAKIAHmGkRKquKsSocC4wqi4A5vQAZoIkcmGc2HAQYFGiAEoxAOIOCAAqMfUAIj4dAIJt0nJwCUxEya5E9Jig6ZnZeYXFcKWeFVU1RvVNre0xXb39sqBlq9WiDh0hM1k5%2BXRFLCRS1FIvQA%3D


from classes.Parser import Parser


if __name__ == "__main__":

    parser = Parser(
        parser_name="tenders.mts.ru",
        is_parsing_site=False,
        is_sending_orders=False,
        is_upd_order_after_sending=False,
        append_base_path=False,
        is_sleeping=True,
        is_logging=False
    )
    parser.start()
