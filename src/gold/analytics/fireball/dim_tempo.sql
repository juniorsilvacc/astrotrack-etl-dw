-- Tabela Dimensão de Tempo (Fireball)
DROP TABLE IF EXISTS gold_fireball_dim_tempo;

CREATE TABLE gold_fireball_dim_tempo AS
SELECT 
    tempo_pk,
    (TO_DATE(tempo_pk::TEXT, 'YYYYMMDD')) AS data_referencia,
    EXTRACT(YEAR FROM (TO_DATE(tempo_pk::TEXT, 'YYYYMMDD'))) AS ano,
    EXTRACT(MONTH FROM (TO_DATE(tempo_pk::TEXT, 'YYYYMMDD'))) AS mes,
    EXTRACT(QUARTER FROM (TO_DATE(tempo_pk::TEXT, 'YYYYMMDD'))) AS trimestre,
    TO_CHAR(TO_DATE(tempo_pk::TEXT, 'YYYYMMDD'), 'Day') AS dia_semana
FROM (
    SELECT DISTINCT TO_CHAR(data_evento, 'YYYYMMDD')::INT AS tempo_pk
    FROM df_fireball
) AS sub;

ALTER TABLE gold_fireball_dim_tempo ADD PRIMARY KEY (tempo_pk);