-- Tabela Fato de Monitoramento Asteroide (NeoWS)
DROP TABLE IF EXISTS gold_neows_fct_monitoramento CASCADE;

CREATE TABLE gold_neows_fct_monitoramento AS
WITH silver_unified AS (
    SELECT asteroide_id, data_aproximacao, velocidade_km_h, distancia_terra_km, corpo_orbital FROM df_neows
    UNION ALL
    SELECT asteroide_id, data_aproximacao, velocidade_km_h, distancia_terra_km, corpo_orbital FROM df_neows_historical
)
SELECT 
    dim.asteroide_sk,
    TO_CHAR(s.data_aproximacao, 'YYYYMMDD')::INT AS tempo_pk,
    s.velocidade_km_h,
    s.distancia_terra_km,
    s.corpo_orbital
FROM silver_unified s
JOIN gold_neows_dim_asteroide dim ON s.asteroide_id = dim.asteroide_id;

ALTER TABLE gold_neows_fct_monitoramento ADD COLUMN id_fato SERIAL PRIMARY KEY;