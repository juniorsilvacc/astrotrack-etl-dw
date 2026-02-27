-- Tabela Fato de Impactos (Fireball)
DROP TABLE IF EXISTS gold_fireball_fct_impactos;

CREATE TABLE gold_fireball_fct_impactos AS
SELECT 
    f.data_evento, 
    t.tempo_pk,
    l.localizacao_sk,
    f.energia_irradiada_total_joules,
    f.energia_impacto_kt,
    f.altitude_km,
    f.velocidade_km_s
FROM df_fireball f
LEFT JOIN gold_fireball_dim_localizacao l 
    ON f.latitude = l.latitude AND f.longitude = l.longitude
LEFT JOIN gold_fireball_dim_tempo t 
    ON TO_CHAR(f.data_evento, 'YYYYMMDD')::INT = t.tempo_pk;

ALTER TABLE gold_fireball_fct_impactos ADD COLUMN id_fato SERIAL PRIMARY KEY;