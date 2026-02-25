from src.transform.transform_cad import run_cad
from src.transform.transform_fireball import run_fireball
from src.transform.transform_neows import run_neows
from src.transform.transform_neows_backfill import run_backfill
import logging

if __name__ == "__main__":  
    try:
        run_cad()
        run_fireball()
        run_neows()
        run_backfill()
        
        logging.info("Toda a pipeline foi executada com sucesso! 🚀")
    except Exception as e:
        logging.error(f"Falha na execução da pipeline: {e}")