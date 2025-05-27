# Практическая работа 7. Импорт и экспорт данных в SQL. Работа с внешними источниками данных

## Цель:
Научиться импортировать и экспортировать данные в базу данных SQL. Работа включает в себя загрузку данных из внешних источников в таблицы базы данных, а также экспорт данных из базы данных в различные форматы. Студенты научатся работать с внешними данными, преобразовывать их в нужный формат и интегрировать с существующими таблицами в базе данных.

## Запуск и подключение к базе данных
1. **Подключениек библиотеке**
```
   !pip install psycopg2
```
   !![image](https://github.com/user-attachments/assets/0306c23c-b58f-4487-b1d7-1c6a04ed0c45)

```
     import psycopg2
     from psycopg2 import Error 
 ```
!![image](https://github.com/user-attachments/assets/e8e108ac-f676-4780-8555-de346adf9c1a)

2. **Создадим подключение к базе данных**
 ```
   try:
    # Подключение к существующей базе данных
    connection = psycopg2.connect(user="postgres",
                                  password="Egor",
                                  host="localhost",
                                  port="5432",
                                  database="practika")

    # Создание курсора для выполнения операций с базой данных
    cursor = connection.cursor()
    # Вывод информации о сервере PostgreSQL
    print("Информация о сервере PostgreSQL")
    print(connection.get_dsn_parameters(), "\n")
    # Выполнение SQL-запроса
    cursor.execute("SELECT version();")
    # Получение результата
    record = cursor.fetchone()
    print("Вы подключены к - ", record, "\n")

except (Exception, Error) as error:
    print("Ошибка при подключении к PostgreSQL:", error)
    
finally:
    if (connection):
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто")
```
!![image](https://github.com/user-attachments/assets/766e8071-3f56-4ebc-86c4-2adfb8ca33b3)
   



