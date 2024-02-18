\pset pager off

BEGIN;

/*psql -h localhost -d PL1 -U postgres -p 5432*/

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

\COPY Camiones FROM 'C:\\Users\\scamero\\Desktop\\UAH\\B.Datos2\\registros_camiones.txt' DELIMITER ',' CSV;
\COPY Peliculas_aux FROM 'peliculas.csv' WITH (ENCODING 'UTF-8',DELIMITER E'\t');

\d import;
ROLLBACK;
