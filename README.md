#### Используемые таблицы
- [Открытие месяца](https://docs.google.com/spreadsheets/d/1eM9elG7gj4_KRCpTAGS-dcqbVVjCP7geidP0vzGHAlg/edit#gid=1601388241)

#### .env
```
DB_NAME=""
DB_USER=""
DB_PASSWORD=""
DB_HOST=""
DB_PORT=
```
#### Поставить задачу в крон (выполняется каждый час) время по UTC
```
0 4 * * * python3 ************/exel.py
```
