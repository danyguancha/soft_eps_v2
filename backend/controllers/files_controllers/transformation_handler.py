# controllers/transformation_handler.py
from typing import Dict, Any
from models.schemas import TransformRequest
from services.transformation_service import TransformationService
from controllers.files_controllers.storage_manager import FileStorageManager

class TransformationHandler:
    def __init__(self, storage_manager: FileStorageManager):
        self.storage_manager = storage_manager
    
    def transform_data(self, request: TransformRequest) -> Dict[str, Any]:
        """Aplica transformación a los datos"""
        file_info = self.storage_manager.get_file_info(request.file_id)
        if not file_info:
            raise ValueError("Archivo no encontrado")
        
        # Obtener DataFrame actual
        cache_key = self.storage_manager.generate_cache_key(
            request.file_id, 
            file_info.get('default_sheet')
        )
        df = self.storage_manager.get_cached_dataframe(cache_key)
        if df is None:
            raise ValueError("Datos no encontrados en cache")
        
        # Aplicar transformación
        transformed_df = TransformationService.apply_transformation(
            df, request.operation, request.params
        )
        
        # Actualizar cache
        self.storage_manager.update_cached_dataframe(cache_key, transformed_df)
        
        # Actualizar columnas en storage
        file_info["columns"] = transformed_df.columns.tolist()
        self.storage_manager.store_file_info(request.file_id, file_info)
        
        return {
            "message": "Transformación aplicada exitosamente",
            "new_columns": transformed_df.columns.tolist()
        }
