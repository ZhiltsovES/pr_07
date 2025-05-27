CREATE TABLE department (
  id serial primary key,
  name varchar(100) not null
  )

-- Создание таблицы
CREATE TABLE IF NOT EXISTS Products (
            Name VARCHAR(100) NOT NULL,
            Price DECIMAL
        );

-- Вставка данных
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


CREATE TABLE Doctor (
    Doctor_Id serial NOT NULL PRIMARY KEY,
    Doctor_Name VARCHAR (100) NOT NULL,
    Hospital_Id serial NOT NULL,
    Joining_Date DATE NOT NULL,
    Speciality VARCHAR (100) NOT NULL,
    Salary INTEGER NOT NULL,
    Experience SMALLINT
);


# Вставка данных о докторах
insert_query = 
INSERT INTO Doctor (Doctor_Id, Doctor_Name, Hospital_Id, Joining_Date, Speciality, Salary, Experience)
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


CREATE TABLE IF NOT EXISTS patient (
            patient_id SERIAL PRIMARY KEY,
            patient_name VARCHAR(100) NOT NULL,
            doctor_id INTEGER NOT NULL,
            FOREIGN KEY (doctor_id) REFERENCES doctor(doctor_id)
        );


INSERT INTO patient (Patient_id, patient_name, doctor_id)
        VALUES 
       ('1','Alice', 109),
('2','Bob', 110 ),
('3','Charlie', 109 );

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
            d.speciality LIKE '%Orthopedic Surgeon%;
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
  SELECT 
            experience,
            salary
        FROM 
            doctor
        WHERE 
            experience IS NOT NULL AND salary IS NOT NULL;
