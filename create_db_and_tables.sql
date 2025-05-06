CREATE TABLE department (
  id serial primary key,
  name varchar(100) not null
  )

-- Создание таблицы
CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL
);

-- Вставка данных
INSERT INTO products (product_id, product_name) VALUES
(1, 'Термометр цифровой'),
(2, 'Маска хирургическая'),
(3, 'Перчатки стерильные'),
(4, 'Стерильные шприцы'),
(5, 'Глюкометр'),
(6, 'Бинты эластичные'),
(7, 'Антисептик для рук'),
(8, 'Жгут триклинт'),
(9, 'Стетоскоп'),
(10, 'Таблетки парацетамол');

CREATE TABLE doctor (
    doctor_id INTEGER PRIMARY KEY,
    doctor_name VARCHAR(100) NOT NULL,
    hospital_id INTEGER NOT NULL,
    joining_date DATE NOT NULL,
    speciality VARCHAR(100),
    salary INTEGER,
    experience SMALLINT
);

CREATE TABLE patient (
    patient_id INTEGER PRIMARY KEY,
    patient_name VARCHAR(100) NOT NULL,
    doctor_id INTEGER NOT NULL
);

INSERT INTO patient (patient_name, doctor_id) VALUES
('Анна Смирнова', 110),
('Дмитрий Петров', 111),
('Екатерина Иванова', 101),
('Максим Соколов', 103),
('Ольга Лебедева', 102);
