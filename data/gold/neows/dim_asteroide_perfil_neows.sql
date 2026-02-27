-- Tabela Dimensão de Perfil Asteroide (CAD)
DROP TABLE IF EXISTS gold_neows_dim_asteroide;

CREATE TABLE gold_neows_dim_asteroide AS
SELECT 
    ROW_NUMBER() OVER () AS asteroide_sk,
    asteroide_id,
    nome_asteroide,
    magnitude_absoluta,
    diametro_medio_km,
    ameaca_potencial,
    objeto_sentry
FROM (
    SELECT DISTINCT ON (asteroide_id) * FROM df_neows 
    ORDER BY asteroide_id, data_aproximacao DESC
) AS unique_asteroids;

ALTER TABLE gold_neows_dim_asteroide ADD PRIMARY KEY (asteroide_sk);