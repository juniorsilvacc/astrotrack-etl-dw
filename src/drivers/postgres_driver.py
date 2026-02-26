from src.drivers.interfaces.database_interface import DatabaseInterface
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import Engine, create_engine
from dotenv import load_dotenv
import logging
import time
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class PostgreDriver(DatabaseInterface):
    def __init__(self) -> None:
        load_dotenv()
        
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.host = os.getenv("DB_HOST")
        self.port = os.getenv("DB_PORT", "5432")
        self.db_name = os.getenv("DB_NAME")
        
        self.engine: Engine | None = None
        self._url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"

    def connect(self) -> Engine:
        """Estabelece a conexão com o banco de dados."""
        if self.engine is None:
            for i in range(1, 6):
                try:
                    temp_engine = create_engine(self._url)
                    with temp_engine.connect() as conn:
                        #logging.info("Conexão com o PostgreSQL estabelecida com sucesso!")
                        self.engine = temp_engine
                        return self.engine
                    
                except SQLAlchemyError as e:
                    logging.error(f"Tentativa {i}/5 - Erro ao conectar: {e}")
                    time.sleep(3)
            raise Exception("Não foi possível conectar ao banco de dados após várias tentativas.")
        return self.engine

    def get_engine(self) -> Engine:
        """Retorna o engine, conectando se necessário."""
        if not self.engine:
            return self.connect()
        return self.engine