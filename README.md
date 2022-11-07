### Пример структуры `.env` файла

```
PRODUCTION=False

CHROME_DRIVER_PATH=/path/to/chrome/driver
COOKIES_PATH=/path/to/cookies

LOG_FILE_PATH=/path/to/logfile.log

PROXIES_HTTP=example.proxy.com
PROXIES_HTTPS=example.proxy.com

HEADERS_ACCEPT=text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8
HEADERS_USER_AGENT=Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:104.0) Gecko/20100101 Firefox/104.0
```

* `PRODUCTION` - флаг включен / выключен боевой режим
* `CHROME_DRIVER_PATH` - путь к драйверу Chrome
* `COOKIES_PATH` - путь к папке с cookies
* `LOG_FILE_PATH` - путь к файлу с логами
* `PROXIES_HTTP` - HTTP прокси
* `PROXIES_HTTPS` - HTTPS прокси
* `HEADERS_ACCEPT` - заголовов запроса 'accept'
* `HEADERS_USER_AGENT` - заголовов запроса 'user_agent'

Для удобства разработчика пример продублирован в файле `.env_example` в корне проекта.


### Базовые классы
