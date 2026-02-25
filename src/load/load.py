from src.drivers.postgres_driver import PostgreDriver
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