from datetime import datetime, timedelta
from airflow.decorators import dag, task
from dotenv import load_dotenv
from pathlib import Path
import sys

sys.path.insert(0, '/opt/airflow')

# Ingestão e Transformação (Bronze, Silver)
from src.bronze.extract_fireball import run_extract_fireball
from src.silver.transform_fireball import run_transform_fireball
from src.bronze.extract_neows import run_extract_neows
from src.silver.transform_neows import run_neows_daily, run_neows_historical
from src.bronze.extract_cad import run_extract_cad
from src.silver.transform_cad import run_transform_cad

# Analytics (Gold)
from src.gold.build_gold import build_gold_layer

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path)

@dag(
    dag_id='astrotrack_etl',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
    },
    description='Pipeline ETL: NeoWS, Fireball e CAD',
    schedule='0 4 * * *', # Rodando às 04:00 AM (Todos os dias)
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=['nasa', 'cad', 'neows', 'fireball', 'etl']
) 
def astrotrack_pipeline():
    
    @task
    def process_fireball():
        run_extract_fireball()
        run_transform_fireball()
        
    @task
    def process_neows():
        run_extract_neows()
        run_neows_historical()
        run_neows_daily()

    @task
    def process_cad():
        run_extract_cad()
        run_transform_cad()
    
    @task
    def build_gold():
        build_gold_layer()
    
    [process_fireball(), process_neows(), process_cad()] >> build_gold()

astrotrack_pipeline()