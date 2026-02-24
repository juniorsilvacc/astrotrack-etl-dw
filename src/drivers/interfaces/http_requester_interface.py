from abc import ABC, abstractmethod

class HttpRequesterInterface(ABC):
    
    @abstractmethod
    def fetch(self, endpoint: str, params: dict | None = None) -> dict:
        pass
