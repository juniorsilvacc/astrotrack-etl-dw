from src.shared.storage.db_handler import save_dataframe
from pathlib import Path
import pandas as pd
import json
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

CONFIG = {
    "folders": Path('data/bronze/fireball'),
    "output": Path('data/silver/fireball/fireball_data.parquet'),
    "rename": {
        "date": "data_evento",
        "energy": "energia_irradiada_total_joules",
        "impact-e": "energia_impacto_kt",
        "lat": "latitude",
        "lat-dir": "latitude_direcao",
        "lon": "longitude",
        "lon-dir": "longitude_direcao",
        "alt": "altitude_km",
        "vel": "velocidade_km_s"
    },
    "types": [
        "energy",
        "impact-e",
        "lat",
        "lon",
        "alt",
        "vel"
    ]
}

def extract_and_normalize(file_path):
    """Extrai dados da estrutura FireBall da NASA."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = json.load(f)
            
            if 'fields' in content and 'data' in content:
                columns = content['fields']
                records = content['data']
            
            df = pd.DataFrame(records, columns=columns)
            return df
    except Exception as e:
        logging.error(f"Erro na extração {os.path.basename(file_path)}: {e}")
        return None

def apply_transformations(df):
    """Limpeza, Tipagem e Deduplicação."""
    logging.info("Aplicando transformações de dados...")
    
    # 1. Conversão de Tipos Númericos
    for col in CONFIG["types"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 2. Conversão de Data
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        
    # 3. Tratamento de Coordenadas
    if 'lat' in df.columns and 'lat-dir' in df.columns:
        df.loc[df['lat-dir'] == 'S', 'lat'] *= -1
    if 'lon' in df.columns and 'lon-dir' in df.columns:
        df.loc[df['lon-dir'] == 'W', 'lon'] *= -1
    
    # 4. Criação de colunas de tempo para análise
    if 'date' in df.columns:
        df['ano_evento'] = df['date'].dt.year
        df['mes_evento'] = df['date'].dt.month
    
    # 5. Dropar colunas
    cols_drop = ['lat-dir', 'lon-dir']
    df = df.drop(columns=[c for c in cols_drop if c in df.columns], errors='ignore')
    
    # 6. Deduplicação (data_evento + latitude + longitude)
    subset_cols = ['date', 'lat', 'lon']
    df = df.drop_duplicates(subset=[c for c in subset_cols if c in df.columns])
    
    # 7. Renomear colunas
    df = df.rename(columns = CONFIG["rename"])
    
    return df

def run_transform_fireball():
    files = list(CONFIG["folders"].glob("*.json"))
    
    if not files:
        logging.error(f"Nenhum arquivo .json encontrado em {CONFIG['folders']}!")
        return
    
    list_dfs = []
    for f in files:
        temp_df = extract_and_normalize(f)
        if temp_df is not None and not temp_df.empty:
            list_dfs.append(temp_df)
    
    if not list_dfs:
        logging.error("Nenhum Fireball foi extraído.")
        return
    
    df_raw = pd.concat(list_dfs, ignore_index=True)
    
    df_end = apply_transformations(df_raw)
    
    CONFIG["output"].parent.mkdir(parents=True, exist_ok=True)
    df_end.to_parquet(CONFIG["output"], index=False)
    
    save_dataframe(df_end, table_name="df_fireball", if_exists="replace")
    
    logging.info(f"Processamento Concluído! ✅")
    logging.info(f"Total de registros únicos na silver: {len(df_end)}")
