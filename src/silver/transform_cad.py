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
    "folders": Path('data/bronze/cad'),
    "output": Path('data/silver/cad/cad_data.parquet'),
    "drop_keywords": ['jd', 'orbit_id', 't_sigma_f'],
    "rename": {
        "des": "nome_asteroide",
        "cd": "data_aproximacao",
        "dist": "distancia_au",
        "dist_min": "distancia_minima_au",
        "dist_max": "distancia_maxima_au",
        "dist_km": "distancia_nominal_km",
        "dist_min_km": "distancia_minima_km",
        "dist_max_km": "distancia_maxima_km",
        "v_rel": "velocidade_relativa_km_s",
        "v_inf": "velocidade_infinita_km_s",
        "h": "magnitude_absoluta"
    },
    "types": [
        "dist", 
        "dist_min", 
        "dist_max", 
        "v_rel", 
        "v_inf", 
        "h"
    ],
    "AU_TO_KM": 149597870.7
}

def extract_and_normalize(file_path):
    """Extrai dados da estrutura CAD (Close-Approach Data) da NASA."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = json.load(f)
        
        if 'fields' in content and 'data' in content:
            columns = content['fields']
            records = content['data']
            
            df = pd.DataFrame(records, columns=columns)
            return df
        else:
            logging.warning(f"Estrutura CAD padrão não encontrada em {file_path.name}")
            return pd.DataFrame()
    except Exception as e:
        logging.error(f"Erro na extração {os.path.basename(file_path)}: {e}")
        return None
    
def apply_transformations(df):
    """Limpeza, Tipagem e Deduplicação."""
    logging.info("Aplicando transformações de dados...")
    
    # 1. Conversão de Tipos
    for col in CONFIG["types"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 2. Transformar AU em KM
    if 'dist' in df.columns:
        df['dist_km'] = df['dist'] * CONFIG["AU_TO_KM"]
    if 'dist_min' in df.columns:
        df['dist_min_km'] = df['dist_min'] * CONFIG["AU_TO_KM"]
    if 'dist_max' in df.columns:
        df['dist_max_km'] = df['dist_max'] * CONFIG["AU_TO_KM"]
    
    # 3. Datas
    if 'cd' in df.columns:
        df['cd'] = pd.to_datetime(df['cd'])
    
    # 4. Limpeza colunas irrelevantes
    cols_remover = [col for col in CONFIG["drop_keywords"] if col in df.columns]
    df = df.drop(columns=cols_remover, errors='ignore')
    
    # 5. Deduplicação (Nome + Data)
    if 'des' in df.columns and 'cd' in df.columns:
        df = df.drop_duplicates(subset=['des', 'cd'], keep='first')
        
    # 6. Renomear colunas
    df = df.rename(columns=CONFIG["rename"])

    return df

def run_transform_cad():
    # 1. Localiza os arquivos
    files = list(CONFIG["folders"].glob("*.json"))
    
    if not files:
        logging.error(f"Nenhum arquivo .json encontrado em {CONFIG['folders']}!")
        return
    
    # 2. Extração
    list_dfs = []
    for f in files:
        temp_df = extract_and_normalize(f)
        if temp_df is not None and not temp_df.empty:
            list_dfs.append(temp_df)
    
    if not list_dfs:
        logging.error("Nenhum asteroide foi extraído.")
        return
    
    df_raw = pd.concat(list_dfs, ignore_index=True)
    
    # 3. Transformação
    df_end = apply_transformations(df_raw)
    
    # 4. Salvando
    CONFIG["output"].parent.mkdir(parents=True, exist_ok=True)
    df_end.to_parquet(CONFIG["output"], index=False)
    
    save_dataframe(df_end, table_name="df_cad", if_exists="replace")
    
    logging.info(f"Processamento Concluído! ✅")
    logging.info(f"Total de registros únicos na silver: {len(df_end)}")
