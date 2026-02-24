from src.drivers.http_requester import HttpRequester
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging
import os

class APINeoWs(HttpRequester):
    def __init__(self) -> None:
        load_dotenv()
        
        base_url = os.getenv("API_NEOWS_NASA")
        self.api_key = os.getenv("API_KEY_NEOWS")
        
        if not base_url:
            logging.error("A variável API_NEOWS_NASA não foi encontrada no .env")
            raise ValueError("URL da API NASA não configurada.")
        
        super().__init__(base_url)
    
    def _fetch_range(self, start_date: str, end_date: str) -> dict:
        """Método que auxilia a formatar o endpoint e chamar o HttpRequester."""
        endpoint = f"feed?start_date={start_date}&end_date={end_date}&api_key={self.api_key}"
        return self.fetch(endpoint)
    
    def fetch_incremental(self, date: str = None) -> dict: # (Rodar diariamente (ex: às 02:00 AM)
        """Busca os dados de um único dia."""
        if date:
            target_data = date
        else:
            # Pega a data atual do sistema no formato 'AAAA-MM-DD'
            target_data = datetime.now().strftime('%Y-%m-%d')
            
        logging.info(f"Iniciando carga incremental para a data: {target_data}")

        # Isso garante que a API da NASA retorne apenas as 24h daquele dia.
        resultado = self._fetch_range(start_date=target_data, end_date=target_data)

        return resultado
    
    def fetch_historical(self, start_date_str: str, end_date_str: str): # (Rodar manualmente em um período específico)
        """Realiza o Backfill em janelas de no máximo 7 dias."""
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        
        current_start = start_date
        all_data = [] # Acumular os dados ou processar um por um
        
        while current_start <= end_date:
            # Calcula o fim da janela (máximo 7 dias à frente ou a data final total)
            current_end = min(current_start + timedelta(days=6), end_date)
            
            s_str = current_start.strftime('%Y-%m-%d')
            e_str = current_end.strftime('%Y-%m-%d')
            
            logging.info(f"Buscando histórico: {s_str} até {e_str}")
            
            try:
                response = self._fetch_range(s_str, e_str)
                all_data.append(response)
            except Exception as e:
                logging.error(f"Erro ao buscar intervalo {s_str} - {e_str}: {e}")
            
            # Pula para o dia seguinte ao fim da última janela
            current_start = current_end + timedelta(days=1)
            
        return all_data
    