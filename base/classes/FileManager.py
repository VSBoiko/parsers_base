import json


class FileManager:
    """Класс для работы с файлами."""

    @staticmethod
    def read_json_file(filename: str):
        """Получить содержимое json-файла.

        :param filename: путь к json-файлу.

        :return: содержимое json-файла.
        """
        with open(filename, "r") as file:
            return json.load(file)

    @staticmethod
    def write_json_file(filename: str, data):
        """Записать данные в json-файл.

        :param filename: путь к json-файлу;
        :param data: данные для записи в json-файл.
        """
        with open(filename, "w") as file:
            json.dump(data, file, indent=4, ensure_ascii=False)
