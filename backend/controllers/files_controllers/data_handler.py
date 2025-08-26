# controllers/data_handler.py
from typing import Dict, Any
import pandas as pd
from models.schemas import DataRequest
from services.csv_service import CSVService
from services.excel_service import ExcelService
from services.filter_service import FilterService
from services.sort_service import SortService
from services.pagination_service import PaginationService
from controllers.files_controllers.storage_manager import FileStorageManager

class DataHandler:
    def __init__(self, storage_manager: FileStorageManager):
        self.storage_manager = storage_manager
        self.file_services = {
            "csv": CSVService(),
            "xlsx": ExcelService(),
            "xls": ExcelService()
        }
    
    def get_data(self, request: DataRequest) -> Dict[str, Any]:
        """Obtiene datos con filtros, ordenamiento y paginación"""
        file_info = self.storage_manager.get_file_info(request.file_id)
        if not file_info:
            raise ValueError("Archivo no encontrado")
        
        # Determinar hoja a usar
        sheet_name = request.sheet_name or file_info.get("default_sheet")
        cache_key = self.storage_manager.generate_cache_key(request.file_id, sheet_name)
        
        # Obtener DataFrame desde cache o cargar
        df = self.storage_manager.get_cached_dataframe(cache_key)
        if df is None:
            service = self.file_services[file_info["ext"]]
            file_obj = service.load(file_info["path"])
            df = service.get_data(file_obj, sheet_name)
            self.storage_manager.cache_dataframe(cache_key, df)
        else:
            df = df.copy()
        
        # Aplicar búsqueda global si está presente
        if request.search:
            df = FilterService.apply_search(df, request.search)
        
        # Aplicar filtros
        if request.filters:
            df = FilterService.apply_filters(df, request.filters)
        
        # Aplicar ordenamiento
        if request.sort:
            df = SortService.apply_sort(df, request.sort)
        
        # Aplicar paginación
        return PaginationService.paginate(df, request.page, request.page_size)
    
    def get_columns(self, file_id: str, sheet_name: str = None) -> Dict[str, Any]:
        """Obtiene columnas específicas de un archivo y hoja"""
        file_info = self.storage_manager.get_file_info(file_id)
        if not file_info:
            raise ValueError("Archivo no encontrado")
        
        if sheet_name and sheet_name not in (file_info.get("sheets") or []):
            # Si se especifica una hoja que no existe, cargar esa hoja
            service = self.file_services[file_info["ext"]]
            file_obj = service.load(file_info["path"])
            try:
                columns = service.get_columns(file_obj, sheet_name)
                return {"columns": columns, "sheet_name": sheet_name}
            except Exception as e:
                raise ValueError(f"Error al acceder a la hoja '{sheet_name}': {str(e)}")
        
        return {
            "columns": file_info["columns"],
            "sheet_name": sheet_name or file_info.get("default_sheet")
        }
