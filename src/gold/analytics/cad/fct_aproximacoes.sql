-- Tabela Fato de Aproximações (CAD)
DROP TABLE IF EXISTS gold_cad_fct_aproximacoes;

CREATE TABLE gold_cad_fct_aproximacoes AS
SELECT 
    obj.objeto_sk,
    TO_CHAR(c.data_aproximacao, 'YYYYMMDD')::INT AS tempo_pk,
    c.distancia_nominal_km,
    c.distancia_minima_km,
    c.distancia_maxima_km,
    c.velocidade_relativa_km_s,
    -- Criando uma métrica de "Incerteza" (Diferença entre max e min)
    (c.distancia_maxima_km - c.distancia_minima_km) AS margem_erro_km
FROM df_cad c
JOIN gold_cad_dim_objeto obj ON c.nome_asteroide = obj.nome_asteroide;

ALTER TABLE gold_cad_fct_aproximacoes ADD COLUMN id_fato SERIAL PRIMARY KEY;