-- Tabela Dimensão de Localização (Fireball)
DROP TABLE IF EXISTS gold_fireball_dim_localizacao;

CREATE TABLE gold_fireball_dim_localizacao AS
SELECT 
    ROW_NUMBER() OVER () AS localizacao_sk, 
    latitude,
    longitude,
    CASE 
        WHEN latitude > 0 THEN 'Norte' 
        ELSE 'Sul' 
    END AS hemisferio_lat,
    CASE 
        WHEN longitude > 0 THEN 'Leste' 
        ELSE 'Oeste' 
    END AS hemisferio_lon
FROM (
    SELECT DISTINCT latitude, longitude 
    FROM df_fireball
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
) AS unique_coords;

ALTER TABLE gold_fireball_dim_localizacao ADD PRIMARY KEY (localizacao_sk);