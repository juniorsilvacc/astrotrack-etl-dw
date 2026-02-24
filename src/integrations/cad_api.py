from src.drivers.http_requester import HttpRequester
from dotenv import load_dotenv
import logging
import os

class APICad(HttpRequester):
    def __init__(self) -> None:
        load_dotenv()
        
        base_url = os.getenv("API_CAD_NASA")
        
        if not base_url:
            logging.error("A variável API_CAD_NASA não foi encontrada no .env")
            raise ValueError("URL da API NASA não configurada.")
        
        super().__init__(base_url)
    
    def get_cad_data(self) -> dict:
        """Busca os dados da API Cad da NASA."""
        return self.fetch("cad.api")
