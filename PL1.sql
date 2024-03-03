\pset pager off

BEGIN;

psql -h localhost -d PL1 -U postgres -p 5432

CREATE TABLE Camiones
(
    id_camion SERIAL PRIMARY KEY,
    matricula VARCHAR(8) UNIQUE NOT NULL,
    empresa VARCHAR(12) NOT NULL,
    kilometros INTEGER
);
\COPY Camiones FROM "D:\\GISI\\2º\\BASES DE DATOS AVANZADAS\\LABORATORIO\\registros_camiones.txt" DELIMITER ',' CSV;
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
 SELECT pg_relation_size('camiones') / current_setting('block_size')::numeric AS number_of_blocks;
 SELECT  relpages FROM   pg_class WHERE  relname = 'camiones';



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

=======

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

/*Cuestion 7*/
VACUUM FULL Camiones;
ANALYZE Camiones;

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

CREATE INDEX InArbol ON Camiones USING btree (kilometros);
SELECT indexrelid::regclass AS index_name, indexrelid AS index_oid FROM pg_stat_user_indexes WHERE indexrelname = 'inarbol';
SELECT relpages FROM pg_class WHERE relname = ‘inarbol’;
SELECT pg_relation_size(‘inarbol’);
SELECT * FROM pgstatindex(18850);



/*Cuestion 12*/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SELECT COUNT(DISTINCT kilometros) AS cantidad_valores_diferentes
FROM camiones;


SELECT typlen AS tamaño_puntero_tabla_bytes FROM pg_type WHERE typname = 'oid';


SELECT pg_column_size(ctid) AS tamaño_puntero_bloque_bytes
FROM camiones
LIMIT 1;


/*Cuestion 13*/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

CREATE INDEX idx_kilometros_hash ON Camiones USING hash (kilometros);
SELECT indexrelid::regclass AS index_name, indexrelid AS index_oid FROM pg_stat_user_indexes WHERE indexrelname = 'idx_kilometros_hash';
SELECT * FROM pgstattuple ('public."idx_kilometros_hash"');
SELECT relpages AS num_blocks FROM pg_class WHERE relname = 'idx_kilometros_hash';







/*Cuestion 15*/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
CREATE INDEX idx_matricula_btree ON Camiones USING btree (matricula);

SELECT indexrelid::regclass AS index_name, indexrelid AS index_oid FROM pg_stat_user_indexes WHERE indexrelname = 'idx_matricula_btree';
SELECT relpages FROM pg_class WHERE relname = ‘idx_matricula_btree’;
SELECT pg_relation_size(‘idx_matricula_btree’);
SELECT * FROM pgstatindex(18897);

/*Cuestion 17*/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
CREATE INDEX idx_matricula_hash ON Camiones USING HASH (matricula);

SELECT indexrelid::regclass AS index_name, indexrelid AS index_oid FROM pg_stat_user_indexes WHERE indexrelname = 'idx_matricula_hash';
SELECT * FROM pgstattuple ('public."idx_matricula_hash"');
SELECT relpages AS num_blocks FROM pg_class WHERE relname = 'idx_matricula_hash';

/*Cuestion 20*/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

CREATE INDEX InArbol ON Camiones USING btree (kilometros);
CREATE INDEX idx_kilometros_hash ON Camiones USING hash (kilometros);
CREATE INDEX idx_id_camion_hash ON Camiones USING hash (id_camion);

/*Cuestion 21*/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
Select  pg_stat_reset();
select pg_stat_reset() from pg_statio_user_indexes;
SELECT blks_read FROM pg_stat_database WHERE datname = current_database();
SELECT tup_returned,tup_inserted,tup_updated,tup_deleted,blks_read,blk_read_time FROM pg_stat_database WHERE datname = current_database();;

SELECT * FROM camiones WHERE Kilometros = 50000;
SELECT * FROM camiones WHERE id_camion = 30000;
SELECT COUNT(*) FROM camiones WHERE kilometros > 400000;
SELECT empresa, COUNT(*) AS numero_de_camiones FROM camiones GROUP BY empresa;
INSERT INTO camiones (id_camion, matricula, empresa, kilometros) VALUES (20000001,'8181 BAA', 'UPS', 30000);
UPDATE camiones SET kilometros = 20000 WHERE id_camion = 20000001;
SELECT * FROM camiones WHERE id_camion>80000 AND id_camion<100000;


/*Cuestion 22*/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

DROP INDEX IF EXISTS InArbol;
DROP INDEX IF EXISTS idx_kilometros_hash;
DROP INDEX IF EXISTS idx_id_camion_hash;
CREATE INDEX multikeybtree ON public."camiones" (empresa, kilometros);

/*Cuestion 23*/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

Select  pg_stat_reset();
select pg_stat_reset() from pg_statio_user_indexes;
SELECT blks_read FROM pg_stat_database WHERE datname = current_database();
SELECT tup_returned,tup_inserted,tup_updated,tup_deleted,blks_read,blk_read_time FROM pg_stat_database WHERE datname = current_database();;

SELECT COUNT(*) AS num_camiones FROM camiones WHERE empresa = 'UPS';
SELECT * FROM camiones WHERE empresa = 'UPS' OR kilometros = 90000;(2035 filas)
SELECT * FROM camiones WHERE empresa = 'UPS' AND kilometros = 60000;

/*Cuestion 24*/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

CREATE TABLE Camiones3 (
    id_camion SERIAL,
    matricula VARCHAR(8) NOT NULL,
    empresa VARCHAR(12) NOT NULL,
    kilometros INTEGER
) PARTITION BY RANGE (kilometros);


CREATE TABLE camiones_part_0_49999 PARTITION OF Camiones3 FOR VALUES FROM (0) TO (50001);
CREATE TABLE camiones_part_50000_99999 PARTITION OF Camiones3 FOR VALUES FROM (50001) TO (100001);
CREATE TABLE camiones_part_100000_149999 PARTITION OF Camiones3 FOR VALUES FROM (100001) TO (150001);
CREATE TABLE camiones_part_150000_199999 PARTITION OF Camiones3 FOR VALUES FROM (150001) TO (200001);
CREATE TABLE camiones_part_200000_249999 PARTITION OF Camiones3 FOR VALUES FROM (200001) TO (250001);
CREATE TABLE camiones_part_250000_299999 PARTITION OF Camiones3 FOR VALUES FROM (250001) TO (300001);
CREATE TABLE camiones_part_300000_349999 PARTITION OF Camiones3 FOR VALUES FROM (300001) TO (350001);
CREATE TABLE camiones_part_350000_399999 PARTITION OF Camiones3 FOR VALUES FROM (350001) TO (400001);
CREATE TABLE camiones_part_400000_449999 PARTITION OF Camiones3 FOR VALUES FROM (400001) TO (450001);
CREATE TABLE camiones_part_450000_499999 PARTITION OF Camiones3 FOR VALUES FROM (450001) TO (500001);

\COPY Camiones3 FROM 'C:\\Users\\scamero\\Desktop\\UAH\\B.Datos2\\registros_camiones.txt' DELIMITER ',' CSV;

Select  pg_stat_reset();
SELECT blks_read FROM pg_stat_database WHERE datname = current_database();
SELECT tup_returned,tup_inserted,tup_updated,tup_deleted,blks_read,blk_read_time FROM pg_stat_database WHERE datname = current_database();;

SELECT COUNT(*) FROM camiones3 WHERE kilometros > 600000;
SELECT * FROM Camiones3 WHERE kilometros BETWEEN 30000 AND 80000;SELECT * FROM Camiones3 WHERE kilometros > 30000 AND kilometros < 80000;
SELECT * FROM Camiones3 WHERE kilometros = 400000;

SELECT * FROM pg_statio_user_tables WHERE relname IN (
    'camiones3_p0', 'camiones3_p1', 'camiones3_p2', 'camiones3_p3',
    'camiones3_p4', 'camiones3_p5', 'camiones3_p6', 'camiones3_p7',
    'camiones3_p8', 'camiones3_p9'
);

\d import;
ROLLBACK;
