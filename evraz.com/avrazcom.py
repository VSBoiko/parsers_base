from classes.Parser import Parser


if __name__ == "__main__":

    parser = Parser(
        parser_name="avraz.com",
        is_parsing_site=True,
        is_sending_orders=False,
        is_upd_order_after_sending=False,
        append_base_path=False,
        is_sleeping=True,
        is_logging=False
    )
    parser.start()
