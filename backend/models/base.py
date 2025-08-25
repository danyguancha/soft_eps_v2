from abc import ABC, abstractmethod

class AbstractFile(ABC):
    def __init__(self, file_path: str):
        self.file_path = file_path
    
    @abstractmethod
    def get_data(self, sheet_name: str = None) -> object:
        pass

    @abstractmethod
    def get_columns(self, sheet_name: str = None) -> list:
        pass
