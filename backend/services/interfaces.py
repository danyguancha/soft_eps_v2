from models.base import AbstractFile

class IFileService:
    def load(self, file_path: str) -> AbstractFile:
        """Load a file and return an AbstractFile object."""
        pass
    def get_columns(self, obj: AbstractFile, sheet_name: str = None) -> list:
        """Get columns from the given AbstractFile object, optionally for a specific sheet."""
        pass
    def get_sheets(self, obj: AbstractFile) -> list:
        """Get sheet names from the given AbstractFile object."""
        pass
    def get_data(self, obj: AbstractFile, sheet_name: str = None) -> object:
        """Get data from the given AbstractFile object, optionally for a specific sheet."""
        pass
