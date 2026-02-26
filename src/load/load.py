from src.drivers.postgres_driver import PostgreDriver
from sqlalchemy import text
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def save_dataframe(df, table_name, if_exists='append'):
    """Envia os dados diretamente para uma tabela no banco."""
    db = PostgreDriver()
    engine = db.get_engine()
    
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=False,
            chunksize=1000
        )

        logging.info(f"Dados persistidos no banco! ✅")
    except Exception as e:
        logging.error(f"Erro ao salvar no banco: {e}")
        raise e

def run_sql_file(file_path):
    """Executa scripts SQL (Gold)."""
    db = PostgreDriver()
    engine = db.get_engine()
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_command = f.read()
        
        with engine.connect() as conn:
            conn.execute(text(sql_command))
            conn.commit()
            
        logging.info(f"Executado com sucesso: {file_path} ✅")
    except Exception as e:
        logging.error(f"Erro ao executar {file_path}: {e}")
        raise e