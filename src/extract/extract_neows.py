from src.integrations.neows_api import APINeoWs
from src.helpers.save_to_bronze import save_to_bronze
from datetime import datetime, timedelta
import logging

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_extract_neows(target_date: str = None, is_historical: bool = False):
    """Extração de NeoWs (Asteroides próximos da Terra)."""
    logging.info(f"Iniciando extração de dados da API NeoWs...")
    
    if not target_date:
        target_date = datetime.now().strftime('%Y-%m-%d')
    
    today_str = target_date

    # Define o nome da subpasta com base no tipo de extração
    api_folder = "neows_historical" if is_historical else f"{today_str}"
    
    try:
        client = APINeoWs()        
        raw_data = client.fetch_incremental(date=target_date) 
        
        path = save_to_bronze(
            data=raw_data, 
            api_name=api_folder, 
            suffix='neows'
        )
        
        return path
    except Exception as e:
        logging.error(f"Erro na extração NeoWs: {e}")
        raise e

def run_extract_backfill_neows(months: int = 6):
    """Extração para buscar o histórico de X meses."""
    logging.info(f"Iniciando Backfill de {months} meses...")
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30 * months)
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        
        run_extract_neows(target_date=date_str, is_historical=True)
        
        current_date += timedelta(days=1)
    
    logging.info("Backfill concluído com sucesso!")
