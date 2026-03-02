-- Tabela Dimensão de Perfil Asteroide (NeoWS)
DROP TABLE IF EXISTS gold_neows_dim_asteroide CASCADE;

CREATE TABLE gold_neows_dim_asteroide AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY asteroide_id) AS asteroide_sk,
    asteroide_id,
    nome_asteroide,
    magnitude_absoluta,
    diametro_medio_km,
    ameaca_potencial,
    objeto_sentry
FROM (
    SELECT DISTINCT ON (asteroide_id)
        asteroide_id, nome_asteroide, magnitude_absoluta, 
        diametro_medio_km, ameaca_potencial, objeto_sentry
    FROM (
        SELECT asteroide_id, nome_asteroide, magnitude_absoluta, diametro_medio_km, ameaca_potencial, objeto_sentry FROM df_neows
        UNION ALL
        SELECT asteroide_id, nome_asteroide, magnitude_absoluta, diametro_medio_km, ameaca_potencial, objeto_sentry FROM df_neows_historical
    ) AS combined
    ORDER BY asteroide_id
) AS unique_asteroids;

ALTER TABLE gold_neows_dim_asteroide ADD PRIMARY KEY (asteroide_sk);