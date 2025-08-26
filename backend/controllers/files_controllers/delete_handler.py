# controllers/delete_handler.py
from typing import Dict, Any, List
from models.schemas import DeleteRowsRequest, DeleteRowsByFilterRequest, BulkDeleteRequest
from services.delete_service import DeleteService
from controllers.files_controllers.storage_manager import FileStorageManager

class DeleteHandler:
    def __init__(self, storage_manager: FileStorageManager):
        self.storage_manager = storage_manager
    
    def delete_specific_rows(self, request: DeleteRowsRequest) -> Dict[str, Any]:
        """Elimina filas específicas por índices"""
        file_info = self.storage_manager.get_file_info(request.file_id)
        if not file_info:
            raise ValueError("Archivo no encontrado")
        
        # Obtener DataFrame desde cache
        sheet_name = request.sheet_name or file_info.get("default_sheet")
        cache_key = self.storage_manager.generate_cache_key(request.file_id, sheet_name)
        
        df = self.storage_manager.get_cached_dataframe(cache_key)
        if df is None:
            raise ValueError("Datos no encontrados en cache")
        
        # Eliminar filas
        result = DeleteService.delete_rows_by_indices(df, request.row_indices)
        
        # Actualizar cache con datos modificados
        self.storage_manager.update_cached_dataframe(cache_key, result["dataframe"])
        
        # Actualizar información del archivo
        file_info["total_rows"] = result["remaining_count"]
        self.storage_manager.store_file_info(request.file_id, file_info)
        
        return {
            "message": "Filas eliminadas exitosamente",
            "rows_deleted": result["deleted_count"],
            "remaining_rows": result["remaining_count"],
            "invalid_indices": result.get("invalid_indices", [])
        }
    
    def delete_rows_by_filter(self, request: DeleteRowsByFilterRequest) -> Dict[str, Any]:
        """Elimina filas que cumplan con filtros específicos"""
        file_info = self.storage_manager.get_file_info(request.file_id)
        if not file_info:
            raise ValueError("Archivo no encontrado")
        
        sheet_name = request.sheet_name or file_info.get("default_sheet")
        cache_key = self.storage_manager.generate_cache_key(request.file_id, sheet_name)
        
        df = self.storage_manager.get_cached_dataframe(cache_key)
        if df is None:
            raise ValueError("Datos no encontrados en cache")
        
        # Eliminar filas por filtro
        result = DeleteService.delete_rows_by_filters(df, request.filters)
        
        # Actualizar cache
        self.storage_manager.update_cached_dataframe(cache_key, result["dataframe"])
        
        # Actualizar información del archivo
        file_info["total_rows"] = result["remaining_count"]
        self.storage_manager.store_file_info(request.file_id, file_info)
        
        return {
            "message": "Filas eliminadas por filtro exitosamente",
            "rows_deleted": result["deleted_count"],
            "remaining_rows": result["remaining_count"],
            "deleted_indices": result["deleted_indices"]
        }
    
    def preview_delete_operation(self, file_id: str, filters: list, sheet_name: str = None) -> Dict[str, Any]:
        """Previsualiza qué filas serían eliminadas"""
        file_info = self.storage_manager.get_file_info(file_id)
        if not file_info:
            raise ValueError("Archivo no encontrado")
        
        sheet_name = sheet_name or file_info.get("default_sheet")
        cache_key = self.storage_manager.generate_cache_key(file_id, sheet_name)
        
        df = self.storage_manager.get_cached_dataframe(cache_key)
        if df is None:
            raise ValueError("Datos no encontrados en cache")
        
        return DeleteService.preview_delete_by_filters(df, filters)
    
    def bulk_delete_operation(self, request: BulkDeleteRequest) -> Dict[str, Any]:
        """Operación de eliminación masiva con confirmación"""
        if not request.confirm_delete:
            raise ValueError("Operación de eliminación masiva requiere confirmación explícita")
        
        file_info = self.storage_manager.get_file_info(request.file_id)
        if not file_info:
            raise ValueError("Archivo no encontrado")
        
        sheet_name = request.sheet_name or file_info.get("default_sheet")
        cache_key = self.storage_manager.generate_cache_key(request.file_id, sheet_name)
        
        df = self.storage_manager.get_cached_dataframe(cache_key)
        if df is None:
            raise ValueError("Datos no encontrados en cache")
        
        # Verificar que la operación no elimine más del 90% de los datos
        preview = DeleteService.preview_delete_by_filters(df, request.conditions)
        if preview["rows_to_delete_count"] > len(df) * 0.9:
            raise ValueError("Operación eliminaría más del 90% de los datos. Verifique los filtros.")
        
        # Proceder con la eliminación
        result = DeleteService.delete_rows_by_filters(df, request.conditions)
        
        # Actualizar cache
        self.storage_manager.update_cached_dataframe(cache_key, result["dataframe"])
        
        # Actualizar información del archivo
        file_info["total_rows"] = result["remaining_count"]
        self.storage_manager.store_file_info(request.file_id, file_info)
        
        return {
            "message": "Eliminación masiva completada exitosamente",
            "rows_deleted": result["deleted_count"],
            "remaining_rows": result["remaining_count"]
        }
    
    def delete_duplicates(self, file_id: str, columns: list = None, keep: str = 'first', sheet_name: str = None) -> Dict[str, Any]:
        """Elimina filas duplicadas"""
        file_info = self.storage_manager.get_file_info(file_id)
        if not file_info:
            raise ValueError("Archivo no encontrado")
        
        sheet_name = sheet_name or file_info.get("default_sheet")
        cache_key = self.storage_manager.generate_cache_key(file_id, sheet_name)
        
        df = self.storage_manager.get_cached_dataframe(cache_key)
        if df is None:
            raise ValueError("Datos no encontrados en cache")
        
        result = DeleteService.delete_duplicates(df, columns, keep)
        
        # Actualizar cache
        self.storage_manager.update_cached_dataframe(cache_key, result["dataframe"])
        
        # Actualizar información del archivo
        file_info["total_rows"] = result["remaining_count"]
        self.storage_manager.store_file_info(file_id, file_info)
        
        return {
            "message": "Duplicados eliminados exitosamente",
            "rows_deleted": result["deleted_count"],
            "remaining_rows": result["remaining_count"],
            "columns_checked": result["columns_checked"]
        }
