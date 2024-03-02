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

/*Cuestion 1*/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SELECT oid, relname FROM pg_class WHERE relname = 'camiones';

/*Cuestion 2*/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
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

SELECT pg_total_relation_size('camiones') / current_setting('block_size')::numeric AS size_in_blocks;


/*Cuestion 3*/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
 SELECT pg_stat_reset();
 SELECT Matricula FROM camiones WHERE Kilometros = 200000;
 SELECT blks_read FROM pg_stat_database WHERE datname = current_database();

 SELECT  pg_total_relation_size('camiones') / pg_relation_size('camiones') AS factor_bloque_medio_real; 

/*Cuestión 4*/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

CREATE TABLE Camiones2
(
    id_camion SERIAL,
    matricula VARCHAR(8) UNIQUE NOT NULL,
    empresa VARCHAR(12) NOT NULL,
    kilometros INTEGER
);

INSERT INTO Camiones2 (id_camion, matricula, empresa, kilometros)
SELECT *
FROM Camiones
ORDER BY kilometros;

SELECT kilometros FROM Camiones2;
SELECT oid, relname FROM pg_class WHERE relname = 'camiones2';
SELECT 
    (current_setting('block_size')::numeric / (AVG(pg_column_size(matricula)) + AVG(pg_column_size(empresa)) + AVG(pg_column_size(id_camion::text)) + AVG(pg_column_size(kilometros::text)))) AS factor_de_bloque
FROM 
    camiones2;
SELECT pg_relation_size('camiones2') / current_setting('block_size')::numeric AS number_of_blocks;



ANALYZE Camiones2;
SELECT count(*) FROM Camiones2;
SELECT ceil(pg_total_relation_size('Camiones2') / current_setting('block_size')::numeric) AS numero_bloques;
SELECT pg_size_pretty(pg_total_relation_size('Camiones2')) AS tamano_tabla;
<<<<<<< HEAD
SELECT pg_relation_size('camiones2') / current_setting('block_size')::numeric AS size_in_blocks;
/*Cuestion 5*/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SELECT pg_stat_reset();
SELECT Matricula FROM camiones2 WHERE Kilometros = 200000;
SELECT blks_read FROM pg_stat_database WHERE datname = current_database();


/*Cuestion 6*/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

DELETE FROM camiones WHERE id_camion IN (SELECT id_camion FROM camiones ORDER BY random() LIMIT 2000000);
SELECT COUNT(*) FROM camiones;

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

CREATE OR REPLACE FUNCTION hash_kilometros(km INTEGER) RETURNS INTEGER AS $$
BEGIN
    RETURN km % 20;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE TABLE Camiones3 (
    id_camion SERIAL,
    matricula VARCHAR(8) NOT NULL,
    empresa VARCHAR(12) NOT NULL,
    kilometros INTEGER
) PARTITION BY HASH (hash_kilometros(kilometros));

CREATE TABLE camiones3_part_0 PARTITION OF Camiones3 FOR VALUES WITH (MODULUS 20, REMAINDER 0);
CREATE TABLE camiones3_part_1 PARTITION OF Camiones3 FOR VALUES WITH (MODULUS 20, REMAINDER 1);
CREATE TABLE camiones3_part_2 PARTITION OF Camiones3 FOR VALUES WITH (MODULUS 20, REMAINDER 2);
CREATE TABLE camiones3_part_3 PARTITION OF Camiones3 FOR VALUES WITH (MODULUS 20, REMAINDER 3);
CREATE TABLE camiones3_part_4 PARTITION OF Camiones3 FOR VALUES WITH (MODULUS 20, REMAINDER 4);
CREATE TABLE camiones3_part_5 PARTITION OF Camiones3 FOR VALUES WITH (MODULUS 20, REMAINDER 5);
CREATE TABLE camiones3_part_6 PARTITION OF Camiones3 FOR VALUES WITH (MODULUS 20, REMAINDER 6);
CREATE TABLE camiones3_part_7 PARTITION OF Camiones3 FOR VALUES WITH (MODULUS 20, REMAINDER 7);
CREATE TABLE camiones3_part_8 PARTITION OF Camiones3 FOR VALUES WITH (MODULUS 20, REMAINDER 8);
CREATE TABLE camiones3_part_9 PARTITION OF Camiones3 FOR VALUES WITH (MODULUS 20, REMAINDER 9);
CREATE TABLE camiones3_part_10 PARTITION OF Camiones3 FOR VALUES WITH (MODULUS 20, REMAINDER 10);
CREATE TABLE camiones3_part_11 PARTITION OF Camiones3 FOR VALUES WITH (MODULUS 20, REMAINDER 11);
CREATE TABLE camiones3_part_12 PARTITION OF Camiones3 FOR VALUES WITH (MODULUS 20, REMAINDER 12);
CREATE TABLE camiones3_part_13 PARTITION OF Camiones3 FOR VALUES WITH (MODULUS 20, REMAINDER 13);
CREATE TABLE camiones3_part_14 PARTITION OF Camiones3 FOR VALUES WITH (MODULUS 20, REMAINDER 14);
CREATE TABLE camiones3_part_15 PARTITION OF Camiones3 FOR VALUES WITH (MODULUS 20, REMAINDER 15);
CREATE TABLE camiones3_part_16 PARTITION OF Camiones3 FOR VALUES WITH (MODULUS 20, REMAINDER 16);
CREATE TABLE camiones3_part_17 PARTITION OF Camiones3 FOR VALUES WITH (MODULUS 20, REMAINDER 17);
CREATE TABLE camiones3_part_18 PARTITION OF Camiones3 FOR VALUES WITH (MODULUS 20, REMAINDER 18);
CREATE TABLE camiones3_part_19 PARTITION OF Camiones3 FOR VALUES WITH (MODULUS 20, REMAINDER 19);

\COPY Camiones3 FROM 'C:\\Users\\scamero\\Desktop\\UAH\\B.Datos2\\registros_camiones.txt' DELIMITER ',' CSV;

SELECT pg_relation_size('camiones3_part_0') / current_setting('block_size')::numeric AS num_bloques_part_0;
SELECT pg_relation_size('camiones3_part_1') / current_setting('block_size')::numeric AS num_bloques_part_1;
SELECT pg_relation_size('camiones3_part_2') / current_setting('block_size')::numeric AS num_bloques_part_2;
SELECT pg_relation_size('camiones3_part_3') / current_setting('block_size')::numeric AS num_bloques_part_3;
SELECT pg_relation_size('camiones3_part_4') / current_setting('block_size')::numeric AS num_bloques_part_4;
SELECT pg_relation_size('camiones3_part_5') / current_setting('block_size')::numeric AS num_bloques_part_5;
SELECT pg_relation_size('camiones3_part_6') / current_setting('block_size')::numeric AS num_bloques_part_6;
SELECT pg_relation_size('camiones3_part_7') / current_setting('block_size')::numeric AS num_bloques_part_7;
SELECT pg_relation_size('camiones3_part_8') / current_setting('block_size')::numeric AS num_bloques_part_8;
SELECT pg_relation_size('camiones3_part_9') / current_setting('block_size')::numeric AS num_bloques_part_9;
SELECT pg_relation_size('camiones3_part_10') / current_setting('block_size')::numeric AS num_bloques_part_10;
SELECT pg_relation_size('camiones3_part_11') / current_setting('block_size')::numeric AS num_bloques_part_11;
SELECT pg_relation_size('camiones3_part_12') / current_setting('block_size')::numeric AS num_bloques_part_12;
SELECT pg_relation_size('camiones3_part_13') / current_setting('block_size')::numeric AS num_bloques_part_13;
SELECT pg_relation_size('camiones3_part_14') / current_setting('block_size')::numeric AS num_bloques_part_14;
SELECT pg_relation_size('camiones3_part_15') / current_setting('block_size')::numeric AS num_bloques_part_15;
SELECT pg_relation_size('camiones3_part_16') / current_setting('block_size')::numeric AS num_bloques_part_16;
SELECT pg_relation_size('camiones3_part_17') / current_setting('block_size')::numeric AS num_bloques_part_17;
SELECT pg_relation_size('camiones3_part_18') / current_setting('block_size')::numeric AS num_bloques_part_18;
SELECT pg_relation_size('camiones3_part_19') / current_setting('block_size')::numeric AS num_bloques_part_19;


/*Cuestion 10*/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
/*Cuestion 11*/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////



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
