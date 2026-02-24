from src.integrations.neows_api import APINeoWs
from src.helpers.save_to_bronze import save_to_bronze
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_extract_neows(target_date: str = None):
    """Extração de NeoWs (Asteroides próximos da Terra)."""
    logging.info(f"Iniciando extração de dados da API NeoWs...")
    
    if not target_date:
        target_date = datetime.now().strftime('%Y-%m-%d')
    
    try:
        client = APINeoWs()
        raw_data = client.fetch_incremental(date=target_date)
        
        path = save_to_bronze(
            data=raw_data, 
            api_name="neows", 
            suffix=target_date
        )
        
        return path
    except Exception as e:
        logging.error(f"Erro na extração de NeoWs: {e}")
        raise e
