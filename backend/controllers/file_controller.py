# controllers/file_controller.py
from fastapi import UploadFile
from typing import Dict, Any, List, Optional
from models.schemas import (
    DataRequest, TransformRequest, DeleteRowsRequest, 
    DeleteRowsByFilterRequest, BulkDeleteRequest, ExportRequest
)

# Importar todos los handlers
from controllers.files_controllers.storage_manager import FileStorageManager
from controllers.files_controllers.upload_handler import UploadHandler
from controllers.files_controllers.data_handler import DataHandler
from controllers.files_controllers.transformation_handler import TransformationHandler
from controllers.files_controllers.export_handler import ExportHandler
from controllers.files_controllers.delete_handler import DeleteHandler
from controllers.files_controllers.file_info_handler import FileInfoHandler

class FileController:
    def __init__(self):
        # Inicializar el gestor de almacenamiento
        self.storage_manager = FileStorageManager()
        
        # Inicializar todos los handlers
        self.upload_handler = UploadHandler(self.storage_manager)
        self.data_handler = DataHandler(self.storage_manager)
        self.transformation_handler = TransformationHandler(self.storage_manager)
        self.export_handler = ExportHandler(self.storage_manager, self.data_handler)
        self.delete_handler = DeleteHandler(self.storage_manager)
        self.file_info_handler = FileInfoHandler(self.storage_manager)
    
    # Operaciones de Upload
    def upload_file(self, file: UploadFile) -> Dict[str, Any]:
        """Procesa la carga de archivo"""
        return self.upload_handler.upload_file(file)
    
    # Operaciones de Datos
    def get_data(self, request: DataRequest) -> Dict[str, Any]:
        """Obtiene datos con filtros, ordenamiento y paginación"""
        return self.data_handler.get_data(request)
    
    def get_columns(self, file_id: str, sheet_name: str = None) -> Dict[str, Any]:
        """Obtiene columnas específicas de un archivo y hoja"""
        return self.data_handler.get_columns(file_id, sheet_name)
    
    # Operaciones de Transformación
    def transform_data(self, request: TransformRequest) -> Dict[str, Any]:
        """Aplica transformación a los datos"""
        return self.transformation_handler.transform_data(request)
    
    # Operaciones de Exportación
    def export_processed_data(self, request: ExportRequest) -> Dict[str, Any]:
        """Exporta datos procesados"""
        return self.export_handler.export_processed_data(request)
    
    def export_filtered_data(self, file_id: str, request: DataRequest, format: str = "csv") -> Dict[str, Any]:
        """Exporta datos filtrados"""
        return self.export_handler.export_filtered_data(file_id, request, format)
    
    def cleanup_exports(self, days_old: int = 7) -> Dict[str, Any]:
        """Limpia archivos de exportación antiguos"""
        return self.export_handler.cleanup_exports(days_old)
    
    # Operaciones de Eliminación
    def delete_specific_rows(self, request: DeleteRowsRequest) -> Dict[str, Any]:
        """Elimina filas específicas por índices"""
        return self.delete_handler.delete_specific_rows(request)
    
    def delete_rows_by_filter(self, request: DeleteRowsByFilterRequest) -> Dict[str, Any]:
        """Elimina filas que cumplan con filtros específicos"""
        return self.delete_handler.delete_rows_by_filter(request)
    
    def preview_delete_operation(self, file_id: str, filters: list, sheet_name: str = None) -> Dict[str, Any]:
        """Previsualiza qué filas serían eliminadas"""
        return self.delete_handler.preview_delete_operation(file_id, filters, sheet_name)
    
    def bulk_delete_operation(self, request: BulkDeleteRequest) -> Dict[str, Any]:
        """Operación de eliminación masiva con confirmación"""
        return self.delete_handler.bulk_delete_operation(request)
    
    def delete_duplicates(self, file_id: str, columns: list = None, keep: str = 'first', sheet_name: str = None) -> Dict[str, Any]:
        """Elimina filas duplicadas"""
        return self.delete_handler.delete_duplicates(file_id, columns, keep, sheet_name)
    
    # Operaciones de Información de Archivos
    def get_file_info(self, file_id: str) -> Dict[str, Any]:
        """Obtiene información básica del archivo"""
        return self.file_info_handler.get_file_info(file_id)
    
    def list_all_files(self) -> Dict[str, Any]:
        """Lista todos los archivos cargados"""
        return self.file_info_handler.list_all_files()
    
    def delete_file(self, file_id: str) -> Dict[str, Any]:
        """Elimina archivo del sistema"""
        return self.file_info_handler.delete_file(file_id)

# Instancia global del controlador
file_controller = FileController()

# Funciones de compatibilidad (para mantener la API actual)
def upload_file(file: UploadFile):
    return file_controller.upload_file(file)

def get_data(request: DataRequest):
    return file_controller.get_data(request)

def transform_data(request: TransformRequest):
    return file_controller.transform_data(request)

def get_file_info(file_id: str):
    return file_controller.get_file_info(file_id)

def delete_file(file_id: str):
    return file_controller.delete_file(file_id)

def list_all_files():
    return file_controller.list_all_files()

def get_columns(file_id: str, sheet_name: str = None):
    return file_controller.get_columns(file_id, sheet_name)

def export_filtered_data(file_id: str, request: DataRequest, format: str = "csv"):
    return file_controller.export_filtered_data(file_id, request, format)

def export_processed_data(request: ExportRequest):
    return file_controller.export_processed_data(request)

def delete_specific_rows(request: DeleteRowsRequest):
    return file_controller.delete_specific_rows(request)

def delete_rows_by_filter(request: DeleteRowsByFilterRequest):
    return file_controller.delete_rows_by_filter(request)

def preview_delete_operation(file_id: str, filters: list, sheet_name: str = None):
    return file_controller.preview_delete_operation(file_id, filters, sheet_name)

def bulk_delete_operation(request: BulkDeleteRequest):
    return file_controller.bulk_delete_operation(request)

def delete_duplicates(file_id: str, columns: list = None, keep: str = 'first', sheet_name: str = None):
    return file_controller.delete_duplicates(file_id, columns, keep, sheet_name)

def cleanup_exports(days_old: int = 7):
    return file_controller.cleanup_exports(days_old)
