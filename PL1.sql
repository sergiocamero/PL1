\pset pager off

BEGIN;

/*psql -h localhost -d PL1 -U postgres -p 5432*/

CREATE TABLE Camiones
(
    id_camion SERIAL PRIMARY KEY,
    matricula VARCHAR(8) UNIQUE NOT NULL,
    empresa VARCHAR(12) NOT NULL,
    kilometros INTEGER
);
\COPY Camiones FROM 'C:\\Users\\scamero\\Desktop\\UAH\\B.Datos2\\registros_camiones.txt' DELIMITER ',' CSV;
SELECT empresa FROM Camiones;
DROP TABLE IF EXISTS Camiones;

/*Cuestion 1*/
SELECT oid, datname FROM pg_database WHERE datname = 'PL1';
/*Cuestion 2*/
SELECT current_setting('block_size') AS block_size FROM pg_class WHERE relname = 'camiones';

SELECT AVG(length(matricula)) FROM Camiones;
SELECT AVG(length(empresa)) FROM Camiones;
SELECT 
    CEIL(AVG(LENGTH(id_camion::text))) AS longitud_media_id_camion,
    CEIL(AVG(LENGTH(kilometros::text))) AS longitud_media_kilometros
FROM 
    Camiones;

SELECT 
    (current_setting('block_size')::numeric / (AVG(pg_column_size(matricula)) + AVG(pg_column_size(empresa)) + AVG(pg_column_size(id_camion::text)) + AVG(pg_column_size(kilometros::text)))) AS factor_de_bloque
FROM 
    camiones;


/*Cuestion 3*/
 SELECT pg_stat_reset();
 SELECT Matricula FROM camiones WHERE Kilometros = 200000;
 SELECT blks_read FROM pg_stat_database WHERE datname = current_database();

 SELECT  pg_total_relation_size('camiones') / pg_relation_size('camiones') AS factor_bloque_medio_real; 

/*Cuestión 4*/
CREATE TABLE Camiones2 AS
  SELECT *
  FROM Camiones
  ORDER BY kilometros;

ANALYZE Camiones2;
SELECT count(*) FROM Camiones2;
SELECT ceil(pg_total_relation_size('Camiones2') / current_setting('block_size')::numeric) AS numero_bloques;
SELECT pg_size_pretty(pg_total_relation_size('Camiones2')) AS tamano_tabla;

CREATE TABLE Camiones2
(
    id_camion SERIAL PRIMARY KEY,
    matricula VARCHAR(8) UNIQUE NOT NULL,
    empresa VARCHAR(12) NOT NULL,
    kilometros INTEGER
);
\COPY Camiones FROM 'C:\\Users\\scamero\\Desktop\\UAH\\B.Datos2\\registros_camiones.txt' DELIMITER ',' CSV;

SELECT * FROM Camiones2
ORDER BY kilometros ASC;

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
/*Cuestion 7*/
VACUUM FULL Camiones;
--Este comando realiza una reorganización más agresiva de la tabla y puede liberar más espacio, especialmente después de grandes eliminaciones. Sin embargo, ten en cuenta que VACUUM FULL bloqueará la tabla durante la operación.
REINDEX TABLE Camiones;
--Después de la eliminación masiva, es recomendable reconstruir los índices para mejorar el rendimiento de las consultas.
CLUSTER Camiones USING Camiones_kilometros_idx;
--Este comando reorganiza físicamente la tabla y sus índices en base al índice proporcionado (Camiones_kilometros_idx en este caso), lo que puede mejorar el rendimiento de las consultas que utilizan este índice.
ANALYZE Camiones;
--Es importante volver a ejecutar ANALYZE después de las operaciones anteriores para que las estadísticas de la tabla estén actualizadas y el optimizador de consultas pueda tomar decisiones informadas.

/*Cuestion 8*/
CREATE TABLE camiones3_part_0 PARTITION OF Camiones3 FOR VALUES WITH (MODULUS 20, REMAINDER 0);
CREATE TABLE camiones3_part_1 PARTITION OF Camiones3 FOR VALUES WITH (MODULUS 20, REMAINDER 1);
...
CREATE TABLE camiones3_part_19 PARTITION OF Camiones3 FOR VALUES WITH (MODULUS 20, REMAINDER 19);

CREATE TABLE Camiones3 (
    id_camion SERIAL PRIMARY KEY,
    matricula VARCHAR(8) UNIQUE NOT NULL,
    empresa VARCHAR(12) NOT NULL,
    kilometros INTEGER
) PARTITION BY HASH (kilometros);

/*Cuestion 10*/
DROP TABLE IF EXISTS Camiones;
DROP TABLE IF EXISTS Camiones2;
DROP TABLE IF EXISTS Camiones3;
CREATE TABLE Camiones
(
    id_camion SERIAL PRIMARY KEY,
    matricula VARCHAR(8) UNIQUE NOT NULL,
    empresa VARCHAR(12) NOT NULL,
    kilometros INTEGER
);
\COPY Camiones FROM 'C:\\Users\\scamero\\Desktop\\UAH\\B.Datos2\\registros_camiones.txt' DELIMITER ',' CSV;

-- Crear las particiones automáticamente usando un bloque PL/pgSQL
DO $$ 
DECLARE 
    i INT := 0;
BEGIN
    WHILE i < 20 LOOP
        EXECUTE FORMAT('CREATE TABLE Camiones3_part_%s PARTITION OF Camiones3 FOR VALUES WITH (MODULUS 20, REMAINDER %s)', i, i);
        i := i + 1;
    END LOOP;
END $$;


\d import;
ROLLBACK;
