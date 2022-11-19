from Parser import Parser


if __name__ == "__main__":
    parser = Parser(
        parser_name="site_name",
        is_sleeping=True,
        is_sending_orders=False,
        is_updating_order=False,
        is_parsing_site=False,
    )

    parser.run()
