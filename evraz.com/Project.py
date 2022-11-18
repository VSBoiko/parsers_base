import sys
import os


# todo: Примерный макет архитектуры, к которому придерживаемся, если есть предложение изменить так что бы было еще удобнее,
#  готов послушать Вас
# todo: Работаем в режиме DEBUG всегда, пока не разместим парсер на сервере

DEBUG = import sys
import os


# todo: Примерный макет архитектуры, к которому придерживаемся, если есть предложение изменить так что бы было еще удобнее, готов послушать Вас
# todo: Работаем в режиме DEBUG всегда, пока не разместим парсер на сервере

DEBUG = True
if DEBUG is False:
    currentdir = os.path.dirname(os.path.realpath(__file__))
    base_path = os.path.dirname(currentdir)
    sys.path.append(base_path)
    sys.path.append('/home/manage_report')
    from Send_report.Utils import send_to_api
    DRIVER_PATH = 'home/service/chromedriver'
else:
    DRIVER_PATH = ''  # todo: определить если необходимо, если драйвер не используется в проекте то удалить переменную


class FileManager:
    # все методы для работы с файлами всех форматов, запись, чтение итд
    pass


class DB:
    # все манипуляции с базой данных пишем сюда
    pass


class Driver:
    # методы chromedriver пищем сюда
    pass


class Request:
    # методы библиотеки requests или других, какие предпочитаете
    pass


class Parser(Driver, DB):

    def run(self):
        ads: list[dict] = []  # в этом списке словари с данными
        # в цикле собираем данные, кладем в список и потом вызываем метод send() и передаем этом список ads
        #
        #
        #
        #
        #
        # тут пишем основную логику
        #
        #
        #
        #
        #
        # self.send()
        pass

    def send(self, ads: list[dict]):
        '''
        отправляем данные по api на сервер
        :param ads: список с данными в виде словарей
        :return:
        '''
        # todo: Имя пасрера желательно по url источника например: если источник www.google.com, название парсера будет google_com
        data = {'name': 'Имя парсера',
                'data': ads}
        if DEBUG:
            print(data)

        # send_to_api(data)

    def __get_name(self):
        pass

    def __get_INN(self):
        pass

    def __phone_number(self):
        pass

    def __tender_number(self):
        pass

    # todo: итд


if __name__=="__main__":
    Parser().run()