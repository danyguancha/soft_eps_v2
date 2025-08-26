# controllers/cross_controller.py
from typing import Dict, Any
from controllers.files_controllers.cross_handler import CrossHandler
from controllers.files_controllers.storage_manager import FileStorageManager

class CrossController:
    def __init__(self, storage_manager: FileStorageManager):
        self.cross_handler = CrossHandler(storage_manager)
    
    def perform_cross(self, request) -> Dict[str, Any]:
        """Realiza cruce entre dos archivos"""
        return self.cross_handler.perform_cross(request)
    
    def get_file_columns_for_cross(self, file_id: str, sheet_name: str = None) -> Dict[str, Any]:
        """Obtiene columnas disponibles para cruce"""
        return self.cross_handler.get_available_columns(file_id, sheet_name)
    
    def preview_cross(self, request, limit: int = 100) -> Dict[str, Any]:
        """Previsualiza el resultado del cruce"""
        return self.cross_handler.preview_cross_operation(request, limit)

# Crear instancia usando el storage_manager del file_controller
from controllers.file_controller import file_controller
cross_controller = CrossController(file_controller.storage_manager)

# Funciones de compatibilidad (para mantener la API actual)
def perform_cross(request):
    return cross_controller.perform_cross(request)

def get_file_columns_for_cross(file_id: str, sheet_name: str = None):
    return cross_controller.get_file_columns_for_cross(file_id, sheet_name)

def preview_cross(request, limit: int = 100):
    return cross_controller.preview_cross(request, limit)
