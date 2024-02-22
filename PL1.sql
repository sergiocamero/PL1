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
SELECT AVG(id_camion) FROM Camiones;
SELECT AVG(length(matricula)) FROM Camiones;
SELECT AVG(length(empresa)) FROM Camiones;
SELECT AVG(kilometros) FROM Camiones;
SELECT 
    CEIL(AVG(LENGTH(id_camion::text))) AS longitud_media_id_camion,
    CEIL(AVG(LENGTH(kilometros::text))) AS longitud_media_kilometros
FROM 
    Camiones;
SELECT 
    AVG(1 + FLOOR(LOG10(ABS(id_camion)))) AS longitud_media_id_camion,
    AVG(CEIL(LOG10(kilometros + 1))) AS longitud_media_kilometros
FROM 
    Camiones;
/*Cuestion 3*/
SELECT Matricula FROM camiones WHERE Kilometros = 200000;
SELECT blks_read FROM pg_stat_database WHERE datname = current_database();

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
CREATE TABLE Camiones3
(
    id_camion SERIAL PRIMARY KEY,
    matricula VARCHAR(10) UNIQUE NOT NULL,
    empresa TEXT NOT NULL,
    kilometros INTEGER
);

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
