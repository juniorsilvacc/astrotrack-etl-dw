import pandas as pd
import json
import glob
import os
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

INPUT_FOLDER = Path('data/bronze/neows_historical')
OUTPUT_FILE = Path('data/silver/neows_backfill_data.parquet')

COLUMNS_TO_DROP = ['links', 'nasa_jpl_url', '_miles', '_feet', '_astronomical']

COLUMNS_TO_RENAME = {
    'id': 'asteroide_id',
    'name': 'nome_asteroide',
    'absolute_magnitude_h': 'magnitude_absoluta',
    'is_potentially_hazardous_asteroid': 'ameaca_potencial',
    'is_sentry_object': 'objeto_sentry',
    'diameter_avg_km': 'diametro_medio_km',
    'app_close_approach_date': 'data_aproximacao',
    'app_relative_velocity_kilometers_per_hour': 'velocidade_km_h',
    'app_miss_distance_kilometers': 'distancia_terra_km',
    'app_orbiting_body': 'corpo_orbital'
}

TYPES_TO_CONVERT = {
    'absolute_magnitude_h',
    'app_relative_velocity_kilometers_per_hour',
    'app_miss_distance_kilometers',
    'estimated_diameter_kilometers_estimated_diameter_min',
    'estimated_diameter_kilometers_estimated_diameter_max'
}

def extract_and_normalize(file_path):
    """Extrai os asteroides de dentro da estrutura da NASA."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Essa chave contém datas, e dentro das datas estão as listas de asteroides.
        all_asteroids = []
        
        if 'near_earth_objects' in data:
            for date in data['near_earth_objects']:
                for asteroid in data['near_earth_objects'][date]:
                    all_asteroids.append(asteroid)
        else:
            logging.warning(f"Estrutura 'near_earth_objects' não encontrada em {os.path.basename(file_path)}")
            return pd.DataFrame()

        if not all_asteroids:
            return pd.DataFrame()

        # 1. Normalização do nível principal
        df = pd.json_normalize(all_asteroids, sep='_')

        # 2. Tratamento da lista 'close_approach_data' dentro de cada asteroide
        if 'close_approach_data' in df.columns:
            # Pega a primeira aproximação
            def get_first_approach(x):
                return x[0] if isinstance(x, list) and len(x) > 0 else None
            
            approach_series = df['close_approach_data'].apply(get_first_approach)
            df_app = pd.json_normalize([item for item in approach_series if item is not None], sep='_')
            df_app = df_app.add_prefix('app_')
            
            # Reseta o index para garantir o concat alinhado
            df = df.drop(columns=['close_approach_data']).reset_index(drop=True)
            df_app = df_app.reset_index(drop=True)
            df = pd.concat([df, df_app], axis=1)
            
        return df
    except Exception as e:
        logging.error(f"Erro na extração {os.path.basename(file_path)}: {e}")
        return None

def apply_transformations(df):
    """Limpeza, Tipagem e Deduplicação."""
    logging.info("Aplicando transformações de dados...")

    # 1. Conversão de Tipos
    for col in TYPES_TO_CONVERT:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 2. Cálculo do Diâmetro Médio  
    col_min = 'estimated_diameter_kilometers_estimated_diameter_min'
    col_max = 'estimated_diameter_kilometers_estimated_diameter_max'
    if col_min in df.columns and col_max in df.columns:
        df['diameter_avg_km'] = (df[col_min] + df[col_max]) / 2

    # 3. Datas
    if 'app_close_approach_date' in df.columns:
        df['app_close_approach_date'] = pd.to_datetime(df['app_close_approach_date'])
    
    # 4. Limpeza colunas irrelevantes
    # cols_remover = [c for c in df.columns if any(word in c for word in COLUMNS_TO_DROP)]
    cols_remover = []
    for col in df.columns:
        for word in COLUMNS_TO_DROP:
            if word in col:
                cols_remover.append(col)
                break
    df = df.drop(columns=cols_remover, errors='ignore')

    # 5. Deduplicação
    if 'id' in df.columns:
        logging.info(f"Deduplicando. Linhas antes: {len(df)}")
        df = df.drop_duplicates(subset=['id'])
        logging.info(f"Deduplicando. Linhas após: {len(df)}")
    
    # 6. Renomear colunas
    df = df.rename(columns=COLUMNS_TO_RENAME)

    return df

def run_backfill():
    files = list(INPUT_FOLDER.glob("*.json"))
    
    if not files:
        logging.error("Nenhum arquivo .json encontrado em {INPUT_FOLDER}!")
        return

    logging.info(f"Localizados {len(files)} arquivos. Extraindo asteroides...")
    
    list_dfs = []
    for f in files:
        temp_df = extract_and_normalize(f)
        if temp_df is not None and not temp_df.empty:
            list_dfs.append(temp_df)

    if not list_dfs:
        logging.error("Nenhum asteroide foi extraído.")
        return

    df_consolidado = pd.concat(list_dfs, ignore_index=True)
    df_end = apply_transformations(df_consolidado)

    # Criar pasta de saída
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    df_end.to_parquet(OUTPUT_FILE, index=False)
    
    logging.info(f"Backfill Concluído! ✅")
    logging.info(f"Total de asteroides únicos salvos: {len(df_end)}")

if __name__ == "__main__":
    run_backfill()