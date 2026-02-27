-- Tabela Fato de Monitoramento Asteroide (NeoWS)
DROP TABLE IF EXISTS gold_neows_fct_monitoramento;

CREATE TABLE gold_neows_fct_monitoramento AS
SELECT 
    dim.asteroide_sk,
    TO_CHAR(n.data_aproximacao, 'YYYYMMDD')::INT AS tempo_pk,
    n.velocidade_km_h,
    n.distancia_terra_km,
    n.corpo_orbital
FROM df_neows n
JOIN gold_neows_dim_asteroide dim ON n.asteroide_id = dim.asteroide_id;

ALTER TABLE gold_neows_fct_monitoramento ADD COLUMN id_fato SERIAL PRIMARY KEY;