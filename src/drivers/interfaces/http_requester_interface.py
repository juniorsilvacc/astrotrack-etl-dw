from abc import ABC, abstractmethod

class HttpRequesterInterface(ABC):
    
    @abstractmethod
    def _fetch(self, endpoint: str, params: dict | None = None) -> dict:
        pass
