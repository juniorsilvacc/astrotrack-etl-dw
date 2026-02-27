from src.shared.storage.db_handler import run_sql_file
from pathlib import Path
import logging
import glob

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def build_gold_layer():
    scripts = sorted(glob.glob("src/gold/analytics/**/*.sql", recursive=True))
    
    logging.info("Iniciando modelagem analítica da Camada Gold...")
    
    if not scripts:
        logging.warning("⚠️ Nenhum arquivo SQL encontrado em data/gold/fireball/")
        return
    
    for script in scripts:
        file_path = Path(script)
        
        if file_path.exists():
            logging.info(f"Processando: {file_path.parent.name} -> {file_path.name}")
            run_sql_file(file_path)
        else:
            logging.warning(f"⚠️ Arquivo ignorado (não encontrado): {script}")
    
    logging.info("Camada Gold construída com sucesso para todos os datasets! ✅ ")
