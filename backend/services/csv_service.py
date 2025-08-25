import pandas as pd
from models.base import AbstractFile
from services.interfaces import IFileService

class CSVFile(AbstractFile):
    def get_data(self, sheet_name: str = None):
        return pd.read_csv(self.file_path, encoding="utf-8")
    def get_columns(self, sheet_name: str = None):
        df = self.get_data()
        return df.columns.tolist()

class CSVService(IFileService):
    def load(self, file_path: str) -> CSVFile:
        return CSVFile(file_path)
    def get_columns(self, obj: CSVFile, sheet_name: str = None) -> list:
        return obj.get_columns()
    def get_sheets(self, obj: CSVFile) -> list:
        return []
    def get_data(self, obj: CSVFile, sheet_name: str = None) -> object:
        return obj.get_data()
