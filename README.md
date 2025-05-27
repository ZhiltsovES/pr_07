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
![image](https://github.com/user-attachments/assets/19b5a887-f313-4e5b-9556-726038e6ac26)


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
**Создаем таблицу Doctor**
```
import psycopg2

try:
    # Подключение к базе данных
    connection = psycopg2.connect(
        user="postgres",
        password="Egor",
        host="localhost",
        port="5432",
        database="medical_db"
    )
    
    # Создание курсора
    cursor = connection.cursor()
    
    # SQL-запрос для создания таблицы
    create_table_query = """
        CREATE TABLE Doctor (
            Doctor_Id serial NOT NULL PRIMARY KEY,
            Doctor_Name VARCHAR(100) NOT NULL,
            Hospital_Id serial NOT NULL,
            Joining_Date DATE NOT NULL,
            Speciality VARCHAR(100) NOT NULL,
            Salary INTEGER NOT NULL,
            Experience SMALLINT
        );
    """
    
    # Выполнение SQL-запроса для создания таблицы
    cursor.execute(create_table_query)
    
    # Фиксация изменений
    connection.commit()
    print("Таблица успешно создана.")
    
    # SQL-запрос для вставки данных в таблицу Doctor
    insert_query = '''
        INSERT INTO Doctor (
            Doctor_ID,
            Doctor_Name,
            Hospital_Id,
            Joining_Date,
            Speciality,
            Salary,
            Experience
        )
        VALUES
        ('101', 'David', '1', '2005-02-10', 'Pediatric', 40000, NULL),
('102', 'Michael', '1', '2018-07-23', 'Oncologist', 20000, NULL),
('103', 'Susan', '2', '2016-05-19', 'Garnacologist', 25000, NULL),
('104', 'Robert', '2', '2017-12-28', 'Pediatric', 28000, NULL),
('105', 'Linda', '3', '2004-06-04', 'Garnacologist', 42000, NULL),
('106', 'William', '3', '2012-09-11', 'Dermatologist', 30000, NULL),
('107', 'Richard', '4', '2014-08-21', 'Garnacologist', 32000, NULL),
('108', 'Karen', '4', '2011-10-17', 'Radiologist', 30000, NULL),
('109', 'James', '1', '2022-01-15', 'Cardiologist', 45000, 5),
('110', 'Emily', '1', '2023-04-10', 'Orthopedic Surgeon', 50000, 3),
('111', 'Olivia', '2', '2021-09-05', 'Neurologist', 42000, 4),
('112', 'John', '2', '2024-02-18', 'Surgeon', 60000, 2),
('113', 'Sophia', '3', '2022-07-30', 'Urologist', 38000, 6),
('114', 'Daniel', '3', '2025-03-22', 'Pulmonologist', 47000, 1),
('115', 'Isabella', '4', '2023-11-01', 'Pediatrician', 41000, 3),
('116', 'Liam', '4', '2022-05-25', 'Dermatologist', 35000, 4),
('117', 'Mia', '1', '2024-06-17', 'Gastroenterologist', 53000, 2),
('118', 'Lucas', '2', '2023-01-12', 'Anesthesiologist', 46000, 3);
    '''
    
    # Выполнение SQL-запроса для вставки данных
    cursor.execute(insert_query)
    
    # Фиксация изменений
    connection.commit()
    print("Данные успешно вставлены в таблицу 'Doctor'")
    
except Exception as e:
    print(f"Ошибка: {e}")
finally:
    if connection:
        cursor.close()
        connection.close()
```
![image](https://github.com/user-attachments/assets/9949a547-e4b4-4a71-b8e3-9fce9d2ef923)
## **Упражнение 2.** Подключитесь к серверу базы данных и распечатайте его версию
```
import psycopg2

def get_connection(database_name):
    # Функция для получения подключения к базе данных
    connection = psycopg2.connect(user="postgres",
                                  password="Egor",
                                  host="localhost",
                                  port="5432",
                                  database="medical_db")
    return connection

def close_connection(connection):
    # Функция для закрытия подключения к базе данных
    if connection:
        connection.close()
        print("Соединение с PostgreSQL закрыто")

try:
    # Подключение к базе данных
    database_name = 'medical_db'
    connection = get_connection(database_name)
    cursor = connection.cursor()

    # Выполнение запроса для получения версии базы данных
    cursor.execute("SELECT version();")
    db_version = cursor.fetchone()

    # Вывод версии базы данных
    print("Версия PostgreSQL:", db_version[0])

except (Exception, psycopg2.Error) as error:
    print("Ошибка при подключении или работе с PostgreSQL:", error)

finally:
    # Закрытие подключения
    if connection:
        close_connection(connection)
```
![image](https://github.com/user-attachments/assets/0f7e4476-096b-47c9-ba89-6ad6c74e1755)
## **Упражнение 3.** Получить информацию о больнице и врачах с использованием идентификаторов больницы и врача
```
def get_connection(database_name):
    # Функция для получения подключения к базе данных
    connection = psycopg2.connect(user="postgres",
                                  password="Egor",
                                  host="localhost",
                                  port="5432",
                                  database="medical_db")
    return connection

def close_connection(connection):
    # Функция для закрытия подключения к базе данных
    if connection:
        connection.close()
        print("Соединение с PostgreSQL закрыто")

def get_hospital_detail(hospital_id):
    try:
        # Подключаемся к базе данных medical_db
        database_name = 'medical_db'
        connection = get_connection(database_name)
        cursor = connection.cursor()

        # Запрос для получения информации о больнице
        select_query = """SELECT * FROM Hospital WHERE Hospital_Id = %s"""
        cursor.execute(select_query, (hospital_id,))
        records = cursor.fetchall()

        # Вывод информации о больнице
        print("Печать записи о больнице:")
        for row in records:
            print("Hospital Id:", row[0])
            print("Hospital Name:", row[1])
            print("Bed Count:", row[2])

        # Закрытие подключения
        close_connection(connection)
    except (Exception, psycopg2.Error) as error:
        print("Ошибка при получении данных:", error)

def get_doctor_detail(doctor_id):
    try:
        # Подключаемся к базе данных medical_db
        database_name = 'medical_db'
        connection = get_connection(database_name)
        cursor = connection.cursor()

        # Запрос для получения информации о докторе
        select_query = """SELECT * FROM Doctor WHERE Doctor_Id = %s"""
        cursor.execute(select_query, (doctor_id,))
        records = cursor.fetchall()

        # Вывод информации о докторе
        print("Печать записи о докторе:")
        for row in records:
            print("Doctor Id:", row[0])
            print("Doctor Name:", row[1])
            print("Hospital Id:", row[2])
            print("Joining Date:", row[3])
            print("Specialty:", row[4])
            print("Salary:", row[5])
            print("Experience:", row[6])

        # Закрытие подключения
        close_connection(connection)
    except (Exception, psycopg2.Error) as error:
        print("Ошибка при получении данных:", error)

# Запросить данные о больнице с ID 2 и докторе с ID 106
print("Упражнение 3. Чтение информации о больнице и докторе\n")
get_hospital_detail(2)
print("\n")
get_doctor_detail(106)
```
![image](https://github.com/user-attachments/assets/4bb57f1e-6ddb-463e-8be4-5f28f1a25622)
## **Упражнение 4.** Получить список врачей по заданной специальности и зарплате
```
import psycopg2

def get_connection(database_name):
    # Функция для получения подключения к базе данных
    connection = psycopg2.connect(user="postgres",
                                  password="Egor",
                                  host="localhost",
                                  port="5432",
                                  database="medical_db")
    return connection

def close_connection(connection):
    # Функция для закрытия подключения к базе данных
    if connection:
        connection.close()
        print("Соединение с PostgreSQL закрыто")

# Подключаемся к базе данных medical_db
database_name = 'medical_db'

def get_specialist_doctors_list(speciality, salary):
    try:
        connection = get_connection(database_name)
        cursor = connection.cursor()

        # SQL-запрос для получения списка врачей по специальности и зарплате
        sql_select_query = """SELECT * FROM Doctor WHERE Speciality=%s AND Salary > %s"""
        cursor.execute(sql_select_query, (speciality, salary))
        records = cursor.fetchall()

        # Выводим информацию о врачах с указанной специальностью и зарплатой выше заданной
        print(f"Список врачей со специальностью {speciality} и зарплатой больше {salary}: \n")
        for row in records:
            print(f"Идентификатор врача: {row[0]}")
            print(f"Имя врача: {row[1]}")
            print(f"Идентификатор больницы: {row[2]}")
            print(f"Дата поступления: {row[3]}")
            print(f"Специальность: {row[4]}")
            print(f"Зарплата: {row[5]}")
            print(f"Опыт: {row[6]}\n")

        # Закрытие подключения
        close_connection(connection)
    except (Exception, psycopg2.Error) as error:
        print("Ошибка при получении данных:", error)

# Вопрос 4: Получение списка врачей по заданной специальности и зарплате
print("Вопрос 4: Получить список врачей по заданной специальности и зарплате\n")
get_specialist_doctors_list("Pediatric", 20000)
```
![image](https://github.com/user-attachments/assets/aad79a1f-b6bb-42bf-bf23-20a9e48851e8)
## **Упражнение 5.** Получить список врачей по заданной специальности и зарплате
```
import psycopg2

def get_connection(database_name):
    # Функция для получения подключения к базе данных
    connection = psycopg2.connect(user="postgres",
                                  password="Egor",
                                  host="localhost",
                                  port="5432",
                                  database="medical_db")
    return connection

def close_connection(connection):
    # Функция для закрытия подключения к базе данных
    if connection:
        connection.close()
        print("Соединение с PostgreSQL закрыто")

def get_doctors_by_hospital(hospital_id):
    try:
        # Подключаемся к базе данных medical_db
        database_name = 'medical_db'
        connection = get_connection(database_name)
        cursor = connection.cursor()

        # SQL-запрос для получения всех врачей из указанной больницы
        select_query = """SELECT * FROM Doctor WHERE Hospital_Id = %s"""
        cursor.execute(select_query, (hospital_id,))
        records = cursor.fetchall()

        # Проверка и вывод результатов
        print(f"\nСписок врачей из больницы с ID {hospital_id}:\n")
        if records:
            for row in records:
                print("ID врача:", row[0])
                print("Имя врача:", row[1])
                print("ID больницы:", row[2])
                print("Дата начала работы:", row[3])
                print("Специальность:", row[4])
                print("Зарплата:", row[5])
                print("Опыт:", row[6], "\n")
        else:
            print("Врачи не найдены.")

        # Закрытие подключения
        close_connection(connection)
    except (Exception, psycopg2.Error) as error:
        print("Ошибка при получении данных:", error)

# Вызов функции для больницы с ID 3
print("Упражнение 5. Получение списка врачей из определённой больницы\n")
get_doctors_by_hospital(3)
```
![image](https://github.com/user-attachments/assets/1b538573-f2f9-43a8-a93c-a155b99e91ad)

## **Задание 6**. Обновить стаж врача в годах. Показать информацию до и после обновления.
```
import psycopg2

def get_connection(database_name):
    # Функция для получения подключения к базе данных
    connection = psycopg2.connect(user="postgres",
                                  password="Egor",
                                  host="localhost",
                                  port="5432",
                                  database="medical_db")
    return connection

def close_connection(connection):
    # Функция для закрытия подключения к базе данных
    if connection:
        connection.close()
        print("Соединение с PostgreSQL закрыто")

def update_experience(doctor_id, new_experience):
    try:
        # Подключаемся к базе данных
        database_name = 'medical_db'
        connection = get_connection(database_name)
        cursor = connection.cursor()

        # Обновляем стаж доктора с указанным ID
        update_query = """UPDATE Doctor SET Experience = %s WHERE Doctor_Id = %s"""
        cursor.execute(update_query, (new_experience, doctor_id))
        connection.commit()

        print(f"Стаж врача с ID {doctor_id} успешно обновлен на {new_experience} лет")

        # Печать данных о докторе после обновления
        select_query = """SELECT Doctor_Id, Doctor_Name, Hospital_Id, Joining_Date, Speciality, Salary, Experience
                          FROM Doctor WHERE Doctor_Id = %s"""
        cursor.execute(select_query, (doctor_id,))
        doctor_record = cursor.fetchone()

        if doctor_record:
            print("\nИнформация о докторе после обновления:")
            print(f"Doctor Id: {doctor_record[0]}")
            print(f"Doctor Name: {doctor_record[1]}")
            print(f"Hospital Id: {doctor_record[2]}")
            print(f"Joining Date: {doctor_record[3]}")
            print(f"Speciality: {doctor_record[4]}")
            print(f"Salary: {doctor_record[5]}")
            print(f"Experience: {doctor_record[6]}")

        # Закрытие подключения
        close_connection(connection)

    except (Exception, psycopg2.Error) as error:
        print("Ошибка при обновлении данных:", error)

# Обновим стаж врача с ID 101 на 3 года
print("Задание: Обновить стаж врачу с ID 101 на 3 года\n")
update_experience(101, 3)
```
![image](https://github.com/user-attachments/assets/58fce728-9362-4c9f-9418-e4c4f1b2a41c)
```
import psycopg2
from datetime import datetime

def get_connection(database_name):
    # Функция для получения подключения к базе данных
    connection = psycopg2.connect(user="postgres",
                                  password="Egor",
                                  host="localhost",
                                  port="5432",
                                  database="medical_db")
    return connection

def close_connection(connection):
    # Функция для закрытия подключения к базе данных
    if connection:
        connection.close()
        print("Соединение с PostgreSQL закрыто")

def get_experience_in_years(doctor_id):
    try:
        # Подключаемся к базе данных medical_db
        database_name = 'medical_db'
        connection = get_connection(database_name)
        cursor = connection.cursor()

        # Запрос для получения информации о докторе
        select_query = """SELECT Joining_Date, Experience FROM Doctor WHERE Doctor_Id = %s"""
        cursor.execute(select_query, (doctor_id,))
        result = cursor.fetchone()

        if result:
            # Получаем дату присоединения и текущий опыт
            joining_date, current_experience = result

            # Если опыт уже установлен в базе данных, добавляем новые года
            if current_experience is not None:
                current_date = datetime.now()
                # Рассчитываем, сколько лет прошло с момента последнего обновления стажа
                years_since_last_update = current_date.year - (joining_date.year + current_experience)
                # Учитываем месяц и день, чтобы избежать пересчета в том же году
                if (current_date.month, current_date.day) < (joining_date.month, joining_date.day):
                    years_since_last_update -= 1
                new_experience = current_experience + years_since_last_update
                print(f"Текущий стаж (с учетом прошедших лет): {new_experience} лет")
                return new_experience
            else:
                # Если опыта нет, рассчитываем его с момента присоединения
                current_date = datetime.now()
                experience_years = current_date.year - joining_date.year - ((current_date.month, current_date.day) < (joining_date.month, joining_date.day))
                print(f"Расчитанный стаж: {experience_years} лет")
                return experience_years
        else:
            print("Доктор не найден")
            return None

    except (Exception, psycopg2.Error) as error:
        print("Ошибка при получении данных:", error)
        return None

    finally:
        # Закрытие подключения
        close_connection(connection)

def update_experience(doctor_id):
    try:
        # Получаем стаж до обновления
        experience_before = get_experience_in_years(doctor_id)

        if experience_before is None:
            return

        print(f"Стаж до обновления: {experience_before} лет")

        # Подключаемся к базе данных
        database_name = 'medical_db'
        connection = get_connection(database_name)
        cursor = connection.cursor()

        # Обновляем стаж в базе данных
        update_query = """UPDATE Doctor SET Experience = %s WHERE Doctor_Id = %s"""
        cursor.execute(update_query, (experience_before, doctor_id))
        connection.commit()

        # Получаем стаж после обновления
        experience_after = get_experience_in_years(doctor_id)

        print(f"Стаж после обновления: {experience_after} лет")

        # Закрытие подключения
        close_connection(connection)

    except (Exception, psycopg2.Error) as error:
        print("Ошибка при обновлении данных:", error)

# Вызов функции для обновления стажа врача с ID 101
print("Задание 6. Обновить стаж врача в годах и показать до и после\n")
update_experience(101)
```
![image](https://github.com/user-attachments/assets/fe3422d9-c291-4642-944c-f441bb6ed921)

## Индивидуальные задания:
**Задание 1. Создайте таблицу "Department" с полями "ID", "NAME".**
```
import psycopg2

try:
    # Подключение к базе данных
    connection = psycopg2.connect(
        user="postgres",
        password="Egor",
        host="localhost",
        port="5432",
        database="medical_db"
    )
    
    # Создание курсора
    cursor = connection.cursor()
    
    # SQL-запрос для создания таблицы
    create_table_query = """
        CREATE TABLE Department (
            ID serial NOT NULL PRIMARY KEY,
            Name VARCHAR(100) NOT NULL
        );
    """
    
    # Выполнение SQL-запроса
    cursor.execute(create_table_query)
    
    # Фиксация изменений
    connection.commit()
    print("Таблица успешно создана.")
    
except Exception as e:
    print(f"Ошибка: {e}")
finally:
    if connection:
        cursor.close()
        connection.close()
```
![image](https://github.com/user-attachments/assets/ec3def68-f258-4a22-b966-cc08b508bc60)

## Задание 2. **Вставьте 10 товаров в таблицу "Products".**

**Создадим таблицу Products и добавим 10 товаров**
```
import psycopg2

try:
    # Подключение к базе данных
    connection = psycopg2.connect(
        user="postgres",
        password="Egor",
        host="localhost",
        port="5432",
        database="medical_db"
    )
    
    # Создание курсора
    cursor = connection.cursor()
    
    # SQL-запрос для создания таблицы (если она еще не существует)
    create_table_query = """
        CREATE TABLE IF NOT EXISTS Products (
            Name VARCHAR(100) NOT NULL,
            Price DECIMAL
        );
    """
    
    # Выполнение SQL-запроса для создания таблицы
    cursor.execute(create_table_query)
    print("Таблица 'Products' успешно создана или уже существует.")
    
    # SQL-запрос для вставки 10 товаров
    insert_query = """
        INSERT INTO Products (Name, Price)
        VALUES 
        ('Laptop', 1200.00),
        ('Smartphone', 800.00),
        ('Headphones', 150.00),
        ('Keyboard', 50.00),
        ('Mouse', 30.00),
        ('Monitor', 300.00),
        ('Printer', 200.00),
        ('Scanner', 180.00),
        ('Tablet', 600.00),
        ('Camera', 700.00);
    """
    
    # Выполнение SQL-запроса для вставки данных
    cursor.execute(insert_query)
    connection.commit()
    print("10 товаров успешно вставлены в таблицу 'Products'.")
    
except Exception as e:
    print(f"Ошибка: {e}")
finally:
    if connection:
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто.")
```
![image](https://github.com/user-attachments/assets/5023ccbb-747f-4252-a567-0ef1f9331031)

## Задание 3. **Выведите всех пациентов, получающих лечение от "Orthopedic".**
**Создадим таблицу Patient с полями p_id, p_name, d_id. Также пропишем второстепенный ключ к полю doctor_id**
```
import psycopg2

try:
    # Подключение к базе данных
    connection = psycopg2.connect(
        user="postgres",
        password="Egor",
        host="localhost",
        port="5432",
        database="medical_db"
    )
    
    # Создание курсора
    cursor = connection.cursor()
    
    # SQL-запрос для создания таблицы patient
    create_table_query = """
        CREATE TABLE IF NOT EXISTS patient (
            patient_id SERIAL PRIMARY KEY,
            patient_name VARCHAR(100) NOT NULL,
            doctor_id INTEGER NOT NULL,
            FOREIGN KEY (doctor_id) REFERENCES doctor(doctor_id)
        );
    """
    
    # Выполнение запроса
    cursor.execute(create_table_query)
    connection.commit()
    print("Таблица 'patient' успешно создана.")

except (Exception, psycopg2.Error) as error:
    print("Ошибка при создании таблицы:", error)

finally:
    if connection:
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто.")
```
![image](https://github.com/user-attachments/assets/a28c3ac9-a1b1-467a-b3e8-4f4a9b83b20d)

**Затем внесем несколько пациентов в таблицу patient**
```
import psycopg2

try:
    # Подключение к базе данных
    connection = psycopg2.connect(
        user="postgres",
        password="Egor",
        host="localhost",
        port="5432",
        database="medical_db"
    )
    
    # Создание курсора
    cursor = connection.cursor()

    insert_query = """
        INSERT INTO patient (Patient_id, patient_name, doctor_id)
        VALUES 
       ('1','Alice', 109),
('2','Bob', 110 ),
('3','Charlie', 109 );
    """
    
    # Выполнение SQL-запроса для вставки данных
    cursor.execute(insert_query)
    connection.commit()
    print("данные добавлены'.")

except Exception as e:
    print(f"Ошибка: {e}")
finally:
    if connection:
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто.")
```
![image](https://github.com/user-attachments/assets/d2964c80-23cd-4a40-82e3-fbc9a4145e55)

**Выполним запрос с Join тк данные хранятся в 2 таблицах**
```
import psycopg2

try:
    # Подключение к базе данных
    connection = psycopg2.connect(
        user="postgres",
        password="Egor",
        host="localhost",
        port="5432",
        database="medical_db"
    )
    
    # Создание курсора
    cursor = connection.cursor()
    
    # SQL-запрос для получения пациентов, получающих лечение от Orthopedic
    select_query = """
        SELECT 
            p.patient_id,
            p.patient_name,
            p.doctor_id,
            d.doctor_name,
            d.speciality
        FROM 
            patient p
        JOIN 
            doctor d
        ON 
            p.doctor_id = d.doctor_id
        WHERE 
            d.speciality LIKE '%Orthopedic Surgeon%';
    """
    
    # Выполнение запроса
    cursor.execute(select_query)
    records = cursor.fetchall()

    # Вывод результатов
    print("Пациенты, получающие лечение от врача специальности 'Orthopedic Surgeon':")
    for row in records:
        print(f"ID пациента: {row[0]}, Имя пациента: {row[1]}, ID врача: {row[2]}, Имя врача: {row[3]}, Специальность врача: {row[4]}")

except (Exception, psycopg2.Error) as error:
    print("Ошибка при работе с PostgreSQL:", error)

finally:
    if connection:
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто.")
```
![image](https://github.com/user-attachments/assets/add697d9-1b47-4728-a9e3-4be5d215a7d6)

## Задание 4. **Выполните SQL-запрос для получения врачей, специализирующихся на "Dermatology".**
```
import psycopg2

try:
    # Подключение к базе данных
    connection = psycopg2.connect(
        user="postgres",
        password="Egor",
        host="localhost",
        port="5432",
        database="medical_db"
    )
    
    # Создание курсора
    cursor = connection.cursor()
    
    # SQL-запрос для получения врачей, специализирующихся на Dermatology
    select_query = """
        SELECT 
            doctor_id,
            doctor_name,
            hospital_id,
            joining_date,
            speciality,
            salary,
            experience
        FROM 
            doctor
        WHERE 
            speciality = %s;
    """
    
    # Выполнение запроса
    cursor.execute(select_query, ('Dermatologist',))
    records = cursor.fetchall()

    # Вывод результатов
    print("Врачи, специализирующиеся на Dermatologist:")
    for row in records:
        print(f"ID врача: {row[0]}, Имя врача: {row[1]}, ID больницы: {row[2]}, Дата приема: {row[3]}, Специальность: {row[4]}, Зарплата: {row[5]}, Опыт: {row[6]}")

except (Exception, psycopg2.Error) as error:
    print("Ошибка при работе с PostgreSQL:", error)

finally:
    if connection:
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто.")
```
![image](https://github.com/user-attachments/assets/66ea250f-33a3-4747-963f-3fee99a4f2eb)

## Задание 5.**Создайте диаграмму для анализа распределения зарплат в зависимости от опыта.**
**Подключим библиотеку для визуализации запросов**
![image](https://github.com/user-attachments/assets/2851a5f1-01c2-41e9-b8d9-22bc7ff065ac)
```
import psycopg2


try:
    # Подключение к базе данных
    connection = psycopg2.connect(
        user="postgres",
        password="Egor",
        host="localhost",
        port="5432",
        database="medical_db"
    )
    
    # Создание курсора
    cursor = connection.cursor()
    
    # SQL-запрос для получения зарплат и опыта
    select_query = """
        SELECT 
            experience,
            salary
        FROM 
            doctor
        WHERE 
            experience IS NOT NULL AND salary IS NOT NULL;
    """
    
    # Выполнение запроса
    cursor.execute(select_query)
    records = cursor.fetchall()

    # Разделение данных на два списка: опыт и зарплаты
    experience = [row[0] for row in records]
    salary = [row[1] for row in records]

    # Построение диаграммы
    plt.figure(figsize=(10, 6))
    plt.scatter(experience, salary, color='blue', alpha=0.7)

    # Настройка осей и заголовка
    plt.title('Распределение зарплат в зависимости от опыта', fontsize=16)
    plt.xlabel('Опыт (лет)', fontsize=14)
    plt.ylabel('Зарплата', fontsize=14)

    # Добавление сетки
    plt.grid(True, linestyle='--', alpha=0.6)

    # Отображение диаграммы
    plt.show()

except (Exception, psycopg2.Error) as error:
    print("Ошибка при работе с PostgreSQL:", error)

finally:
    if connection:
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто.")
```
![image](https://github.com/user-attachments/assets/93de3b7b-f413-44e4-888e-a9281a8f305b)





















   



