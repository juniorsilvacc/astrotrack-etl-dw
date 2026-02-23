from src.drivers.interfaces.http_requester_interface import HttpRequesterInterface
import requests
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class HttpRequester(HttpRequesterInterface):
    def __init__(self, base_url: str) -> None:
        self._base_url = base_url
        
        self._headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://www.google.com/",
            "Connection": "keep-alive",
        }
        
        self._session = requests.Session()
        self._session.headers.update(self._headers)
    
    def _fetch(self, endpoint: str, params: dict | None = None) -> dict:
        """Método comum para realizar o GET com tratamento de erro."""
        url = f"{self._base_url}/{endpoint}"
        try:
            response = self._session.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.error(f"Erro na requisição para {url}: {e}")
            raise