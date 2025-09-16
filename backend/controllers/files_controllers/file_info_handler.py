# controllers/file_info_handler.py
from typing import Dict, Any, List
from controllers.files_controllers.storage_manager import FileStorageManager

class FileInfoHandler:
    def __init__(self, storage_manager: FileStorageManager):
        self.storage_manager = storage_manager
    
    def get_file_info(self, file_id: str) -> Dict[str, Any]:
        """Obtiene información básica del archivo"""
        file_info = self.storage_manager.get_file_info(file_id)
        if not file_info:
            raise ValueError("Archivo no encontrado")
        
        return {
            "file_id": file_id,
            "original_name": file_info["original_name"],
            "columns": file_info["columns"],
            "sheets": file_info["sheets"],
            "total_rows": file_info["total_rows"]
        }
    
    def list_all_files(self) -> Dict[str, Any]:
        """Lista todos los archivos cargados"""
        storage = self.storage_manager.list_technical_files()
        files = []
        
        # for file_id, info in storage:
        #     files.append({
        #         "file_id": file_id,
        #         "original_name": info["original_name"],
        #         "total_rows": info["total_rows"],
        #         "columns_count": len(info["columns"]),
        #         "sheets": info.get("sheets")
        #     })
        
        
        return {"files": storage, "total": len(storage)}
    
    def delete_file(self, file_id: str) -> Dict[str, Any]:
        """Elimina archivo del sistema"""
        success = self.storage_manager.remove_file(file_id)
        if not success:
            raise ValueError("Archivo no encontrado")
        
        return {"message": "Archivo eliminado exitosamente"}
