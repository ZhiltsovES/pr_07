# Практическая работа 7. Импорт и экспорт данных в SQL. Работа с внешними источниками данных

## Цель:
Научиться импортировать и экспортировать данные в базу данных SQL. Работа включает в себя загрузку данных из внешних источников в таблицы базы данных, а также экспорт данных из базы данных в различные форматы. Студенты научатся работать с внешними данными, преобразовывать их в нужный формат и интегрировать с существующими таблицами в базе данных.

## Запуск и подключение к базе данных
 **Подключениек библиотеке**
```
   !pip install psycopg2
```
   ![image](https://github.com/user-attachments/assets/0306c23c-b58f-4487-b1d7-1c6a04ed0c45)

```
     import psycopg2
     from psycopg2 import Error 
 ```
![image](https://github.com/user-attachments/assets/e8e108ac-f676-4780-8555-de346adf9c1a)

 **Создадим подключение к базе данных**
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
![image](https://github.com/user-attachments/assets/766e8071-3f56-4ebc-86c4-2adfb8ca33b3)
**Создание таблицы**
```
try:
    # Подключение к базе данных
    connection = psycopg2.connect(user="postgres",
                                  password="Egor",
                                  host="localhost",
                                  port="5432",
                                  database="practika")

    cursor = connection.cursor()
    # SQL-запрос для создания новой таблицы
    create_table_query = '''CREATE TABLE mobile
          (ID INT PRIMARY KEY     NOT NULL,
          MODEL           TEXT    NOT NULL,
          PRICE           REAL); '''
    # Выполнение команды: создание новой таблицы
    cursor.execute(create_table_query)
    connection.commit()
    print("Таблица успешно создана в PostgreSQL")

except (Exception, Error) as error:
    print("Ошибка при подключении к PostgreSQL:", error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто")
```
![image](https://github.com/user-attachments/assets/958921c9-6970-4bd0-9c77-fb66c3453b79)
**Выполнение запросов**
```
try:
    # Подключение к базе данных
    connection = psycopg2.connect(user="postgres",
                                  password="Egor",
                                  host="localhost",
                                  port="5432",
                                  database="practika")

    cursor = connection.cursor()
    # Выполнение SQL-запроса для вставки данных в таблицу
    insert_query = """ INSERT INTO mobile (ID, MODEL, PRICE) VALUES (1, 'Iphone12', 1100)"""
    cursor.execute(insert_query)
    connection.commit()
    print("1 запись успешно вставлена")

    # Получение результатов
    cursor.execute("SELECT * from mobile")
    record = cursor.fetchall()
    print("Результат:", record)

    # Выполнение SQL-запроса для обновления данных в таблице
    update_query = """Update mobile set price = 1500 where id = 1"""
    cursor.execute(update_query)
    connection.commit()
    count = cursor.rowcount
    print(count, "запись(ей) обновлено успешно")

    # Получение результатов
    cursor.execute("SELECT * from mobile")
    print("Результат:", cursor.fetchall())

    # Выполнение SQL-запроса для удаления данных из таблицы
    delete_query = """Delete from mobile where id = 1"""
    cursor.execute(delete_query)
    connection.commit()
    count = cursor.rowcount
    print(count, "запись(ей) удалено успешно")

    # Получение результатов
    cursor.execute("SELECT * from mobile")
    print("Результат:", cursor.fetchall())

except (Exception, psycopg2.Error) as error:
    print("Ошибка при подключении к PostgreSQL:", error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто")
```
![image](https://github.com/user-attachments/assets/c88d094a-5e0b-4f00-8706-8d29788b6107)
## Practice 07. Упражнение/мини-проект
Информационная система больницы
## **Упражнение 1.** Подключитесь к серверу базы данных, создайте базу данных **medical_db** и таблицу **Hospital**
**Упражнение 1*.** Создайте таблицу **Doctor** и заполните эту таблицу данными в Postgre SQL
```
import psycopg2

def get_connection(database_name):
    # Функция для получения подключения к базе данных
    connection = psycopg2.connect(user="postgres",
                                  password="Egor",
                                  host="localhost",
                                  port="5432",
                                  database = database_name)
    return connection

def close_connection(connection):
    # Функция для закрытия подключения к базе данных
    if connection:
        connection.close()
        print("Соединение с PostgreSQL закрыто")

try:
    # Создание подключения к базе данных sql_case_bi_mgpu (база, с которой можно создавать другие базы)
    connection = psycopg2.connect(user="postgres",
                                  password="Egor",
                                  host="localhost",
                                  port="5432",
                                  database="practika")
    connection.autocommit = True  # Отключаем транзакцию для команды CREATE DATABASE
    cursor = connection.cursor()

    # Создание базы данных
    cursor.execute("CREATE DATABASE medical_db;")
    print("База данных 'medical_db' успешно создана")

    # Закрытие текущего соединения для подключения к новой базе данных
    close_connection(connection)

    # Подключение к новой базе данных 'medical_db'
    connection = get_connection("medical_db")
    cursor = connection.cursor()

    # Создание таблицы Hospital
    create_table_query = '''
    CREATE TABLE Hospital (
        Hospital_Id serial NOT NULL PRIMARY KEY,
        Hospital_Name VARCHAR (100) NOT NULL,
        Bed_Count serial
    );
    '''
    cursor.execute(create_table_query)
    connection.commit()
    print("Таблица 'Hospital' успешно создана")

    # Вставка данных в таблицу Hospital
    insert_query = '''
    INSERT INTO Hospital (Hospital_Id, Hospital_Name, Bed_Count)
    VALUES
    (1, 'Mayo Clinic', 200),
    (2, 'Cleveland Clinic', 400),
    (3, 'Johns Hopkins', 1000),
    (4, 'UCLA Medical Center', 1500);
    '''
    cursor.execute(insert_query)
    connection.commit()
    print("Данные успешно вставлены в таблицу 'Hospital'")

except (Exception, psycopg2.Error) as error:
    print("Ошибка при подключении или работе с PostgreSQL:", error)

finally:
    # Закрытие подключения к базе данных
    if connection:
        close_connection(connection)
```
![image](https://github.com/user-attachments/assets/32864091-2d06-4255-a4f7-2f31c64f25a3)



   



