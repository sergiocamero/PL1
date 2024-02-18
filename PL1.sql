CREATE TABLE Camiones
(
    id_camion SERIAL PRIMARY KEY,
    matricula VARCHAR(10) UNIQUE NOT NULL,
    empresa TEXT NOT NULL,
    kilometros INTEGER
);

COPY camiones FROM 'C:\\Users\\scamero\\Desktop\\UAH\\B.Datos2\\registros_camiones.txt'
DELIMITER ','
CSV;

\d import;
ROLLBACK;