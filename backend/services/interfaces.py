from models.base import AbstractFile

class IFileService:
    def load(self, file_path: str) -> AbstractFile:
        pass
    def get_columns(self, obj: AbstractFile, sheet_name: str = None) -> list:
        pass
    def get_sheets(self, obj: AbstractFile) -> list:
        pass
    def get_data(self, obj: AbstractFile, sheet_name: str = None) -> object:
        pass
