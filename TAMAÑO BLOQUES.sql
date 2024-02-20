-- Tamaño teórico en bloques
SELECT 
  (count(*) *  (4 + length(id_camion::text) + length(matricula::text) + length(empresa::text) + length(kilometros::text)) 
  + 20 + 2) / (current_setting('block_size')::int / 1024) as tamano_teorico_bloques
FROM camiones
GROUP BY id_camion, matricula, empresa, kilometros;

-- Tamaño real en bloques
SELECT 
  pg_size_pretty(pg_total_relation_size('camiones') / (current_setting('block_size')::int / 1024)) as tamano_real_bloques;

-- Factor de bloque medio real
SELECT 
  pg_total_relation_size('camiones') / pg_relation_size('camiones') as factor_bloque_medio_real;