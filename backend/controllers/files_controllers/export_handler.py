# controllers/export_handler.py
import pandas as pd
from typing import Dict, Any
from models.schemas import ExportRequest, DataRequest
from services.export_service import ExportService
from controllers.files_controllers.storage_manager import FileStorageManager
from controllers.files_controllers.data_handler import DataHandler

class ExportHandler:
    def __init__(self, storage_manager: FileStorageManager, data_handler: DataHandler):
        self.storage_manager = storage_manager
        self.data_handler = data_handler
    
    def export_processed_data(self, request: ExportRequest) -> Dict[str, Any]:
        """Exporta datos procesados según filtros y transformaciones"""
        file_info = self.storage_manager.get_file_info(request.file_id)
        if not file_info:
            raise ValueError("Archivo no encontrado")
        
        # Obtener DataFrame desde cache
        sheet_name = request.sheet_name or file_info.get("default_sheet")
        cache_key = self.storage_manager.generate_cache_key(request.file_id, sheet_name)
        
        df = self.storage_manager.get_cached_dataframe(cache_key)
        if df is None:
            raise ValueError("Datos no encontrados en cache")
        else:
            df = df.copy()
        
        # Exportar usando el servicio de exportación
        export_result = ExportService.export_data(df, request)
        
        return {
            "message": "Datos exportados exitosamente",
            **export_result
        }
    
    def export_filtered_data(self, file_id: str, request: DataRequest, format: str = "csv") -> Dict[str, Any]:
        """Exporta datos filtrados a archivo"""
        # Obtener datos filtrados (sin paginación)
        request.page_size = 1000000  # Obtener todos los datos
        result = self.data_handler.get_data(request)
        
        df = pd.DataFrame(result.data)
        
        if format.lower() == "csv":
            filename = f"export_{file_id}.csv"
            df.to_csv(filename, index=False)
        elif format.lower() == "excel":
            filename = f"export_{file_id}.xlsx"
            df.to_excel(filename, index=False)
        else:
            raise ValueError("Formato no soportado")
        
        return {"filename": filename, "rows_exported": len(df)}
    
    def cleanup_exports(self, days_old: int = 7) -> Dict[str, Any]:
        """Limpia archivos de exportación antiguos"""
        return ExportService.cleanup_old_exports(days_old)
