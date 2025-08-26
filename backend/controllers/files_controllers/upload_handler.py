# controllers/upload_handler.py
import uuid
import os
from fastapi import UploadFile
from typing import Dict, Any
from services.csv_service import CSVService
from services.excel_service import ExcelService
from controllers.files_controllers.storage_manager import FileStorageManager

class UploadHandler:
    def __init__(self, storage_manager: FileStorageManager):
        self.storage_manager = storage_manager
        self.file_services = {
            "csv": CSVService(),
            "xlsx": ExcelService(),
            "xls": ExcelService()
        }
    
    def upload_file(self, file: UploadFile) -> Dict[str, Any]:
        """Procesa la carga de archivo y devuelve metadatos"""
        ext = file.filename.split('.')[-1].lower()
        if ext not in self.file_services:
            raise ValueError(f"Tipo de archivo no soportado: {ext}")
        
        # Generar ID único para el archivo
        file_id = str(uuid.uuid4())
        upload_dir = self.storage_manager.ensure_upload_directory()
        file_path = os.path.join(upload_dir, f"{file_id}.{ext}")
        
        # Guardar archivo físicamente
        with open(file_path, "wb") as f:
            f.write(file.file.read())
        
        # Procesar archivo
        service = self.file_services[ext]
        file_obj = service.load(file_path)
        
        # Obtener metadatos
        sheets = service.get_sheets(file_obj) if ext != "csv" else None
        default_sheet = sheets[0] if sheets else None
        columns = service.get_columns(file_obj, default_sheet)
        
        # Obtener datos para contar filas
        df = service.get_data(file_obj, default_sheet)
        total_rows = len(df)
        
        # Almacenar información del archivo
        file_info = {
            "path": file_path,
            "ext": ext,
            "original_name": file.filename,
            "sheets": sheets,
            "default_sheet": default_sheet,
            "columns": columns,
            "total_rows": total_rows
        }
        
        self.storage_manager.store_file_info(file_id, file_info)
        
        # Cachear el DataFrame inicial
        cache_key = self.storage_manager.generate_cache_key(file_id, default_sheet)
        self.storage_manager.cache_dataframe(cache_key, df)
        
        return {
            "file_id": file_id,
            "columns": columns,
            "sheets": sheets,
            "total_rows": total_rows
        }
    
    def is_supported_file(self, filename: str) -> bool:
        """Verifica si el tipo de archivo es soportado"""
        ext = filename.split('.')[-1].lower()
        return ext in self.file_services
