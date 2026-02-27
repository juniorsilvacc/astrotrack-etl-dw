DROP TABLE IF EXISTS gold_cad_dim_objeto;

CREATE TABLE gold_cad_dim_objeto AS
SELECT 
    ROW_NUMBER() OVER () AS objeto_sk,
    nome_asteroide,
    AVG(magnitude_absoluta) AS magnitude_media
FROM df_cad
GROUP BY nome_asteroide;

ALTER TABLE gold_cad_dim_objeto ADD PRIMARY KEY (objeto_sk);