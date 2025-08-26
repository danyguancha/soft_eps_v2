# services/csv_service.py
import csv
import pandas as pd
from pathlib import Path
from models.base import AbstractFile
from services.interfaces import IFileService

def sniff_delimiter(path: str, sample_bytes: int = 32_768) -> str:
    """
    Devuelve el delimitador más probable del CSV.  
    Si no se puede inferir, retorna ',' por compatibilidad.
    """
    with open(path, "r", newline="", encoding="utf-8", errors="ignore") as fh:
        sample = fh.read(sample_bytes)
        try:
            return csv.Sniffer().sniff(sample).delimiter
        except csv.Error:
            # No se pudo determinar: asumir coma
            return ","
class CSVFile(AbstractFile):
    def __init__(self, file_path: str):
        super().__init__(file_path)
        self._delimiter: str | None = None  # cache interno

    # --- API pública ---
    def get_data(self, sheet_name: str | None = None):
        if self._delimiter is None:
            self._delimiter = sniff_delimiter(self.file_path)

        return pd.read_csv(
            self.file_path,
            sep=self._delimiter,
            engine="python",     
            encoding="utf-8",
        )

    def get_columns(self, sheet_name: str | None = None):
        return list(self.get_data().columns)


class CSVService(IFileService):
    def load(self, file_path: str) -> CSVFile:
        if not Path(file_path).is_file():
            raise FileNotFoundError(file_path)
        return CSVFile(file_path)

    def get_columns(self, obj: CSVFile, sheet_name: str | None = None) -> list:
        return obj.get_columns()

    def get_sheets(self, obj: CSVFile) -> list:
        return []  # CSV no tiene pestañas

    def get_data(self, obj: CSVFile, sheet_name: str | None = None):
        return obj.get_data()
