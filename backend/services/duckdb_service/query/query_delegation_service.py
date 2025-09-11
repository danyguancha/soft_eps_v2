# services/duckdb_service/query/query_delegation_service.py
from typing import Dict, Any, List, Optional
from utils.duckdb_utils.validation_utils import build_availability_response

class QueryDelegationService:
    """Servicio para delegación de consultas con verificaciones"""
    
    def __init__(self, connection_manager, controllers: Dict[str, Any]):
        self.connection_manager = connection_manager
        self.controllers = controllers
    
    def delegate_cross_files_query(
        self,
        file1_id: str,
        file2_id: str,
        key_column_file1: str,
        key_column_file2: str,
        join_type: str = "LEFT",
        columns_to_include: Optional[Dict[str, List[str]]] = None,
        loaded_tables: Dict[str, Any] = None,
        file_loader_service=None
    ) -> Dict[str, Any]:
        """Delega cruce de archivos con carga bajo demanda"""
        if not self.connection_manager.is_available():
            return build_availability_response(False, True)
        
        # Verificar y cargar archivos bajo demanda
        for file_id in [file1_id, file2_id]:
            if loaded_tables and file_id not in loaded_tables:
                print(f"🔄 Archivo {file_id} no cargado, intentando carga bajo demanda...")
                
                if file_loader_service:
                    success = file_loader_service.load_file_on_demand(
                        file_id, 
                        self.controllers.get('file_conversion'),
                        self.controllers.get('query')
                    )
                    if success:
                        print(f"✅ Archivo {file_id} cargado bajo demanda")
                    else:
                        print(f"❌ No se pudo cargar archivo {file_id} bajo demanda")
                        return {
                            "success": False,
                            "error": f"Archivo {file_id} no se puede cargar en DuckDB",
                            "requires_fallback": True
                        }
        
        # Delegación al controlador de cruce
        cross_files_controller = self.controllers.get('cross_files')
        if not cross_files_controller:
            return build_availability_response(False, True)
        
        return cross_files_controller.cross_files_ultra_fast(
            file1_id, file2_id, key_column_file1, key_column_file2, 
            join_type, columns_to_include
        )
    
    def delegate_unique_values_query(
        self, 
        file_id: str, 
        column_name: str, 
        limit: int = 1000,
        loaded_tables: Dict[str, Any] = None,
        file_loader_service=None
    ) -> List[str]:
        """Delega valores únicos con carga bajo demanda"""
        if not self.connection_manager.is_available():
            return []
        
        # Verificar y cargar archivo bajo demanda
        if loaded_tables and file_id not in loaded_tables:
            if file_loader_service:
                if not file_loader_service.load_file_on_demand(
                    file_id,
                    self.controllers.get('file_conversion'),
                    self.controllers.get('query')
                ):
                    return []
        
        query_controller = self.controllers.get('query')
        if not query_controller:
            return []
        
        return query_controller.get_unique_values_ultra_fast(file_id, column_name, limit)
    
    def delegate_validation_query(self, file_id: str, sheet_name: str = None) -> Dict[str, Any]:
        """Delega validación de columnas"""
        if not self.connection_manager.is_available():
            return build_availability_response(False, True)
        
        validation_controller = self.controllers.get('file_validation')
        if not validation_controller:
            return build_availability_response(False, True)
        
        return validation_controller.get_file_columns_for_cross(
            file_id, sheet_name, self.controllers.get('loaded_tables', {})
        )
    
    def delegate_file_conversion(
        self, 
        file_path: str, 
        file_id: str, 
        original_name: str, 
        ext: str
    ) -> Dict[str, Any]:
        """Delega conversión de archivos"""
        if not self.connection_manager.is_available():
            return build_availability_response(False, True)
        
        conversion_controller = self.controllers.get('file_conversion')
        if not conversion_controller:
            return build_availability_response(False, True)
        
        return conversion_controller.convert_file_to_parquet(
            file_path, file_id, original_name, ext
        )
