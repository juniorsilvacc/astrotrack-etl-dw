from abc import ABC, abstractmethod
from sqlalchemy import Engine

class DatabaseInterface(ABC):
    
    @abstractmethod
    def connect(self) -> Engine: 
        pass
    
    @abstractmethod
    def get_engine(self) -> Engine:
        pass