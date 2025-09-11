# controllers/files_controllers/data_handler.py (REEMPLAZADO)
from typing import Dict, Any
import pandas as pd
from models.schemas import DataRequest
from controllers.files_controllers.storage_manager import FileStorageManager
from services.duckdb_service.duckdb_service import duckdb_service


class DataHandler:
    def __init__(self, storage_manager: FileStorageManager):
        self.storage_manager = storage_manager
    
    def get_data(self, request: DataRequest) -> Dict[str, Any]:
        """OBTIENE DATOS ULTRA-RÁPIDO usando DuckDB"""
        file_info = self.storage_manager.get_file_info(request.file_id)
        if not file_info:
            raise ValueError("Archivo no encontrado")
        
        # Convertir filtros de modelos Pydantic a diccionarios
        filters = None
        if request.filters:
            filters = []
            for filter_condition in request.filters:
                filters.append({
                    "column": filter_condition.column,
                    "operator": filter_condition.operator.value,
                    "value": filter_condition.value,
                    "values": filter_condition.values
                })
        
        # Convertir ordenamiento
        sort_by = None
        sort_order = "ASC"
        if request.sort and len(request.sort) > 0:
            sort_by = request.sort[0].column
            sort_order = request.sort[0].direction.value.upper()
        
        # CONSULTA ULTRA-RÁPIDA con DuckDB
        return duckdb_service.query_data_ultra_fast(
            file_id=request.file_id,
            filters=filters,
            search=request.search,
            sort_by=sort_by,
            sort_order=sort_order,
            page=request.page,
            page_size=request.page_size
        )
    
    def get_columns(self, file_id: str, sheet_name: str = None) -> Dict[str, Any]:
        """Obtiene columnas del archivo"""
        file_info = self.storage_manager.get_file_info(file_id)
        if not file_info:
            raise ValueError("Archivo no encontrado")
        
        return {
            "columns": file_info["columns"],
            "sheet_name": sheet_name or file_info.get("default_sheet"),
            "ultra_fast": True
        }
