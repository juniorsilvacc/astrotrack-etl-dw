from src.integrations.fireball_api import APIFireBall
from datetime import datetime
import logging
import json
import os

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def save_to_bronze(data: dict, suffix: str):
    """Salva os dados brutos da Fireball na Camada Bronze."""
    base_path = "data/bronze/fireball"
    os.makedirs(base_path, exist_ok=True)
    
    file_name = f"fireball_{suffix}.json"
    full_path = os.path.join(base_path, file_name)
    
    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    logging.info(f"Dados de Fireballs salvos em: {full_path}")
    return full_path

def run_extract_fireball():
    """Extração de Fireballs (Meteoros de alto brilho)."""
    logging.info("Iniciando extração de dados da API Fireball...")
    
    try:
        fireball_client = APIFireBall()
        
        raw_data = fireball_client.get_fireball_data()
        
        today_str = datetime.now().strftime('%Y-%m-%d')
        
        path = save_to_bronze(raw_data, today_str)
        
        return path
        
    except Exception as e:
        logging.error(f"Erro na extração de Fireball: {e}")
        raise e

if __name__ == "__main__":
    # Teste manual
    run_extract_fireball()