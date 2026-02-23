from src.drivers.http_requester import HttpRequester
from dotenv import load_dotenv
import logging
import os

class APIFireBall(HttpRequester):
    def __init__(self) -> None:
        load_dotenv()
        
        base_url = os.getenv("API_FIREBALL_NASA")
        
        if not base_url:
            logging.error("A variável API_FIREBALL_NASA não foi encontrada no .env")
            raise ValueError("URL da API NASA não configurada.")
        
        super().__init__(base_url)
    
    def get_fireball_data(self, ) -> dict:
        """Busca os dados da API FireBall da NASA."""
        return self._fetch("fireball.api")
                     