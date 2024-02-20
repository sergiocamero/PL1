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

\COPY Camiones FROM 'C:\\Users\\scamero\\Desktop\\UAH\\B.Datos2\\registros_camiones.txt' DELIMITER ',' CSV;
/*Cuestion 1*/
SELECT oid, datname FROM pg_database WHERE datname = 'PL1';
/*Cuestion 2*/
show block_size;
Select avg(length(id_camion)) from "Camiones"
/*Cuestion 3*/
SELECT Matricula FROM camiones WHERE Kilometros = 200000;
/*Cuestión 4*/
CREATE TABLE Camiones2 AS
  SELECT *
  FROM Camiones
  ORDER BY kilometros;

ANALYZE Camiones2;
SELECT count(*) FROM Camiones2;
SELECT ceil(pg_total_relation_size('Camiones2') / current_setting('block_size')::numeric) AS numero_bloques;
SELECT pg_size_pretty(pg_total_relation_size('Camiones2')) AS tamano_tabla;
/*Cuestion 5*/
SELECT Matricula FROM Camiones2 WHERE Kilometros = 200000;
/*Cuestion 6*/
DELETE FROM camiones
WHERE id_camion IN (
    SELECT id_camion
    FROM camiones
    ORDER BY random()
    LIMIT 2000000
);
ANALYZE Camiones;
SELECT count(*) FROM Camiones;
SELECT ceil(pg_total_relation_size('Camiones') / current_setting('block_size')::numeric) AS numero_bloques;
SELECT pg_size_pretty(pg_total_relation_size('Camiones')) AS tamano_tabla;

\d import;
ROLLBACK;
