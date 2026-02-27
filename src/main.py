from src.extract.extract_fireball import run_extract_fireball
from src.transform.transform_fireball import run_transform_fireball

from src.extract.extract_cad import run_extract_cad
from src.transform.transform_cad import run_transform_cad

from src.gold.build_gold import build_gold_layer
import logging

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def main():
    logging.info("🚀 Iniciando Pipeline...")

    try:
        run_extract_fireball()
        run_transform_fireball()
        
        run_extract_cad()
        run_transform_cad()
        
        build_gold_layer()

        logging.info("Pipeline finalizada com sucesso! ✅")

    except Exception as e:
        logging.error(f"❌ Falha na Pipeline: {e}")

if __name__ == "__main__":
    main()