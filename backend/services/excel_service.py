import pandas as pd
from openpyxl import load_workbook
from models.base import AbstractFile
from services.interfaces import IFileService

class ExcelFile(AbstractFile):
    def get_sheets(self):
        wb = load_workbook(self.file_path, read_only=True)
        return wb.sheetnames
    def get_data(self, sheet_name: str):
        return pd.read_excel(self.file_path, sheet_name=sheet_name)
    def get_columns(self, sheet_name: str):
        df = self.get_data(sheet_name)
        return df.columns.tolist()

class ExcelService(IFileService):
    def load(self, file_path: str) -> ExcelFile:
        return ExcelFile(file_path)
    def get_columns(self, obj: ExcelFile, sheet_name: str) -> list:
        return obj.get_columns(sheet_name)
    def get_sheets(self, obj: ExcelFile) -> list:
        return obj.get_sheets()
    def get_data(self, obj: ExcelFile, sheet_name: str) -> object:
        return obj.get_data(sheet_name)
