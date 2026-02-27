from src.shared.integrations.fireball_api import APIFireBall
from src.shared.storage.local_storage import save_to_bronze
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_extract_fireball():
    """Extração de Fireballs (Meteoros de alto brilho)."""
    logging.info("Iniciando extração de dados da API Fireball...")
    
    try:
        fireball_client = APIFireBall()
        raw_data = fireball_client.get_fireball_data()
        
        today_str = datetime.now().strftime('%Y-%m-%d')
        
        path = save_to_bronze(
            raw_data, 
            today_str, 
            suffix = 'fireball'
        )
        
        return path
    except Exception as e:
        logging.error(f"Erro na extração de Fireball: {e}")
        raise e
