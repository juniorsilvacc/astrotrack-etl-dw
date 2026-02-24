from src.integrations.cad_api import APICad
from src.helpers.save_to_bronze import save_to_bronze
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_extract_cad():
    """Extração de Cads (Aproximações de asteroides)."""
    logging.info("Iniciando extração de dados da API Cad...")
    
    try:
        cad_client = APICad()
        raw_data = cad_client.get_cad_data()
        
        today_str = datetime.now().strftime('%Y-%m-%d')
        
        path = save_to_bronze(
            raw_data, 
            today_str, 
            suffix='cad'
        )
        
        return path
    except Exception as e:
        logging.error(f"Erro na extração de Cad: {e}")
        raise e

if __name__ == "__main__":
    run_extract_cad()