# services/duckdb_service/duckdb_service.py
import os
import shutil
import pandas as pd
from typing import Dict, Any, List, Optional

# Controladores existentes
from controllers.duckdb_controller.file_validation_controller import FileValidationController
from controllers.duckdb_controller.cache_controller import CacheController
from controllers.duckdb_controller.file_conversion_controller import FileConversionController
from controllers.duckdb_controller.excel_sheets_controller import ExcelSheetsController
from controllers.duckdb_controller.query_controller import QueryController
from controllers.duckdb_controller.cross_files_controller import CrossFilesController

# Servicios especializados
from .connection.connection_manager import ConnectionManager
from .file_management.file_loader_service import FileLoaderService
from .query.query_delegation_service import QueryDelegationService
from utils.sql_utils import SQLUtils
from utils.duckdb_utils.validation_utils import build_availability_response

# Servicios auxiliares existentes
from services.aux_duckdb_services.recover_cache_files import RecoverCacheFiles
from services.aux_duckdb_services.query_pagination import QueryPagination


class DuckDBService:
    """Servicio principal para DuckDB - Refactorizado con separación de responsabilidades"""
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DuckDBService, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        # Solo inicializar una vez
        if self._initialized:
            return
        
        # Configuración de directorios
        self._setup_directories()
        self._clear_cache_directories_fast()
        
        # Inicializar manager de conexión
        self.connection_manager = ConnectionManager(
            self.duckdb_dir, 
            self.parquet_dir, 
            self.metadata_dir
        )
        
        # Registro de tablas cargadas
        self.loaded_tables: Dict[str, Dict[str, Any]] = {}
        
        # Inicializar servicios si la conexión es exitosa
        if self.connection_manager.is_available():
            self._initialize_services()
            self._initialized = True
        else:
            print("DuckDB no se pudo inicializar - Funcionando en modo fallback")
    
    def _setup_directories(self):
        """Configura los directorios necesarios"""
        self.parquet_dir = os.path.abspath("parquet_cache")
        self.duckdb_dir = os.path.abspath("duckdb_storage") 
        self.metadata_dir = os.path.abspath("metadata_cache")
        
        # Crear directorios
        for directory in [self.parquet_dir, self.duckdb_dir, self.metadata_dir]:
            os.makedirs(directory, exist_ok=True)
    
    def _clear_cache_directories_fast(self):
        """Limpia directorios de cache usando shutil (más rápido)"""
        directories_to_clean = [
            self.duckdb_dir,
            self.metadata_dir,
            self.parquet_dir
        ]
        
        for directory in directories_to_clean:
            try:
                # Eliminar directorio completo
                if os.path.exists(directory):
                    shutil.rmtree(directory)
                    print(f"✓ Directorio eliminado: {directory}")
                
                # Recrear vacío
                os.makedirs(directory, exist_ok=True)
                print(f"✓ Directorio recreado: {directory}")
            except Exception as e:
                print(f"✗ Error procesando {directory}: {e}")
                # Intentar crear el directorio si no existe
                os.makedirs(directory, exist_ok=True)
        
        print("✓ Cache limpiado completamente")
    
    def _initialize_services(self):
        """Inicializa todos los controladores y servicios especializados"""
        try:
            # Controladores existentes
            self.controllers = self._initialize_controllers()
            
            # Servicios especializados
            self.file_loader_service = FileLoaderService(
                self.metadata_dir, 
                self.controllers['cache'], 
                self.loaded_tables
            )
            
            self.query_delegation_service = QueryDelegationService(
                self.connection_manager,
                self.controllers
            )
            
            print("Controladores y servicios DuckDB inicializados")
            
            # CORREGIDO: Usar schedule_auto_recovery en lugar de auto_recover_cached_files
            from services.aux_duckdb_services.recover_cache_files import recover_cache_files
            recover_cache_files.schedule_auto_recovery(
                self.metadata_dir, 
                self.controllers['cache'], 
                self.loaded_tables
            )
            
        except Exception as e:
            print(f"Error inicializando servicios: {e}")
    
    def _initialize_controllers(self) -> Dict[str, Any]:
        """Inicializa los controladores existentes"""
        conn = self.connection_manager.get_connection()
        
        controllers = {
            'file_validation': FileValidationController(conn),
            'cache': CacheController(self.parquet_dir, self.metadata_dir),
            'excel_sheets': ExcelSheetsController(),
            'query': QueryController(conn, self.loaded_tables),
            'cross_files': CrossFilesController(conn, self.loaded_tables),
            'loaded_tables': self.loaded_tables
        }
        
        # Controlador de conversión requiere cache
        controllers['file_conversion'] = FileConversionController(
            conn, self.parquet_dir, controllers['cache']
        )
        
        return controllers
    
    # ========== PROPIEDADES DE COMPATIBILIDAD ==========
    
    @property
    def conn(self):
        """Propiedad de compatibilidad para acceder a la conexión"""
        return self.connection_manager.get_connection()
    
    @property
    def file_validation(self):
        """Propiedad de compatibilidad"""
        return self.controllers.get('file_validation')
    
    @property
    def cache(self):
        """Propiedad de compatibilidad"""
        return self.controllers.get('cache')
    
    @property
    def file_conversion(self):
        """Propiedad de compatibilidad"""
        return self.controllers.get('file_conversion')
    
    @property
    def excel_sheets(self):
        """Propiedad de compatibilidad"""
        return self.controllers.get('excel_sheets')
    
    @property
    def query(self):
        """Propiedad de compatibilidad"""
        return self.controllers.get('query')
    
    @property
    def cross_files(self):
        """Propiedad de compatibilidad"""
        return self.controllers.get('cross_files')
    
    # ========== MÉTODOS DE ESTADO ==========
    
    def is_available(self) -> bool:
        """Verifica si DuckDB está disponible y funcionando"""
        return self.connection_manager.is_available()
    
    def restart_connection(self) -> bool:
        """Reinicia la conexión DuckDB de forma segura"""
        success = self.connection_manager.restart_connection()
        
        if success and hasattr(self, 'controllers'):
            # Actualizar referencias en controladores
            self.connection_manager.update_controllers_connection(self.controllers)
        
        return success
    
    # ========== MÉTODOS DELEGADOS PRINCIPALES ==========
    
    def cross_files_ultra_fast(
        self,
        file1_id: str,
        file2_id: str,
        key_column_file1: str,
        key_column_file2: str,
        join_type: str = "LEFT",
        columns_to_include: Optional[Dict[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """Delega cruce de archivos ultra-rápido con carga bajo demanda"""
        return self.query_delegation_service.delegate_cross_files_query(
            file1_id, file2_id, key_column_file1, key_column_file2,
            join_type, columns_to_include, self.loaded_tables, self.file_loader_service
        )
    
    def get_unique_values_ultra_fast(
        self, 
        file_id: str, 
        column_name: str, 
        limit: int = 1000
    ) -> List[str]:
        """Delega valores únicos ultra-rápidos con carga bajo demanda"""
        return self.query_delegation_service.delegate_unique_values_query(
            file_id, column_name, limit, self.loaded_tables, self.file_loader_service
        )
    
    def get_file_columns_for_cross(
        self, 
        file_id: str, 
        sheet_name: str = None
    ) -> Dict[str, Any]:
        """Delega obtención de columnas para cruce"""
        return self.query_delegation_service.delegate_validation_query(file_id, sheet_name)
    
    def convert_file_to_parquet(
        self, 
        file_path: str, 
        file_id: str, 
        original_name: str, 
        ext: str
    ) -> Dict[str, Any]:
        """Delega conversión de archivos"""
        return self.query_delegation_service.delegate_file_conversion(
            file_path, file_id, original_name, ext
        )
    
    # ========== MÉTODOS DELEGADOS SIMPLES ==========
    
    def validate_parquet_file(self, parquet_path: str) -> Dict[str, Any]:
        """Delega validación de archivos Parquet"""
        if not self.is_available():
            return {"valid": False, "error": "DuckDB no disponible"}
        return self.file_validation.validate_parquet_file(parquet_path)
    
    def get_excel_sheets(self, file_path: str) -> Dict[str, Any]:
        """Delega obtención de hojas de Excel"""
        return self.excel_sheets.get_excel_sheets(file_path)
    
    def load_parquet_lazy(
        self, 
        file_id: str, 
        parquet_path: str, 
        table_name: Optional[str] = None
    ) -> str:
        """Delega carga lazy de Parquet"""
        if not self.is_available():
            print("DuckDB no disponible, simulando carga lazy")
            return f"fallback_table_{file_id}"
        return self.query.load_parquet_lazy(file_id, parquet_path, table_name, self.loaded_tables)
    
    def get_file_stats(self, file_id: str) -> Dict[str, Any]:
        """Delega estadísticas de archivo"""
        if not self.is_available():
            return {"loaded": False, "error": "DuckDB no disponible"}
        return self.query.get_file_stats(file_id)
    
    # ========== MÉTODOS DE CARGA Y CONVERSIÓN ==========
    
    def load_parquet_to_view(
        self, 
        file_id: str, 
        parquet_path: str, 
        table_name: Optional[str] = None
    ) -> str:
        """Delega creación de vista"""
        if not self.is_available():
            return f"fallback_view_{file_id}"
        
        return self.query.load_parquet_to_view(file_id, parquet_path, table_name, self.loaded_tables)
    
    def load_parquet_to_table(
        self, 
        file_id: str, 
        parquet_path: str, 
        table_name: Optional[str] = None
    ) -> str:
        """Delega carga de tabla (compatibilidad)"""
        return self.load_parquet_lazy(file_id, parquet_path, table_name)
    
    def load_parquet_to_table_materialized(
        self, 
        file_id: str, 
        parquet_path: str, 
        table_name: Optional[str] = None
    ) -> str:
        """Delega materialización completa"""
        if not self.is_available():
            return f"fallback_materialized_{file_id}"
        
        if hasattr(self.query, 'load_parquet_to_table_materialized'):
            return self.query.load_parquet_to_table_materialized(
                file_id, parquet_path, table_name, self.loaded_tables
            )
        else:
            # Fallback a lazy si no existe el método
            return self.load_parquet_lazy(file_id, parquet_path, table_name)
    
    def query_parquet_direct(
        self,
        file_id: str,
        filters: Optional[List[Dict[str, Any]]] = None,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "ASC",
        page: int = 1,
        page_size: int = 1000,
        selected_columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Delega consulta directa a Parquet"""
        if not self.is_available():
            return build_availability_response(False, True)
        
        if hasattr(self.query, 'query_parquet_direct'):
            return self.query.query_parquet_direct(
                file_id, filters, search, sort_by, sort_order, page, page_size, selected_columns
            )
        else:
            # Fallback a query normal
            return QueryPagination().query_data_ultra_fast(
                self.conn, file_id, filters, search, sort_by, sort_order, 
                page, page_size, selected_columns, self.loaded_tables
            )
    
    # ========== MÉTODOS DE CACHE ==========
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Delega estadísticas de cache"""
        if not self.cache:
            return {
                "total_cached_files": 0,
                "total_cache_size_mb": 0,
                "cache_hit_potential": "N/A",
                "error": "Cache no disponible"
            }
        return self.cache.get_cache_stats()
    
    def cleanup_old_cache(self, days_old: int = 30, min_access_count: int = 1):
        """Delega limpieza de cache"""
        if not self.cache:
            return {
                "cleaned_files": 0,
                "size_cleaned_mb": 0,
                "remaining_files": 0,
                "error": "Cache no disponible"
            }
        return self.cache.cleanup_old_cache(days_old, min_access_count)
    
    def cleanup_old_files(self, days_old: int = 7):
        """Método de compatibilidad para limpieza"""
        return self.cleanup_old_cache(days_old)
    
    # ========== MÉTODOS DE EXCEL ==========
    
    def get_columns_from_sheet(self, file_path: str, sheet_name: str) -> Dict[str, Any]:
        """Obtiene columnas de una hoja específica de Excel"""
        try:
            if not self.is_available():
                return build_availability_response(False, True)
            
            import pandas as pd
            
            # Leer solo las primeras filas para obtener columnas
            df = pd.read_excel(
                file_path,
                sheet_name=sheet_name,
                nrows=0,  # Solo headers
                engine='openpyxl'
            )
            
            columns = [str(col) for col in df.columns]
            
            return {
                "success": True,
                "columns": columns,
                "header_row_detected": 0,
                "method": "pandas_header_only"
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Error obteniendo columnas de hoja: {str(e)}"
            }
    
    def get_sheet_preview(self, file_path: str, sheet_name: str, max_rows: int = 5) -> Dict[str, Any]:
        """Obtiene preview de una hoja específica"""
        try:
            if not self.is_available():
                return build_availability_response(False, True)
            
            import pandas as pd
            
            # Leer pocas filas para preview
            df = pd.read_excel(
                file_path,
                sheet_name=sheet_name,
                nrows=max_rows,
                engine='openpyxl'
            )
            
            columns = [str(col) for col in df.columns]
            preview_data = df.to_dict('records')
            
            return {
                "success": True,
                "columns": columns,
                "preview_data": preview_data,
                "header_row": columns,
                "sample_rows": len(preview_data)
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Error obteniendo preview: {str(e)}"
            }
    
    def validate_sheet_exists(self, file_path: str, sheet_name: str) -> bool:
        """Valida que una hoja específica existe en el archivo"""
        try:
            sheet_info = self.get_excel_sheets(file_path)
            if sheet_info.get("success"):
                return sheet_name in sheet_info.get("sheets", [])
            return False
        except:
            return False
    
    def get_first_valid_sheet(self, file_path: str) -> str:
        """Obtiene la primera hoja válida del archivo"""
        try:
            sheet_info = self.get_excel_sheets(file_path)
            if sheet_info.get("success") and sheet_info.get("sheets"):
                return sheet_info["sheets"][0]
            return "Sheet1"  # Fallback
        except:
            return "Sheet1"
    
    # ========== MÉTODOS DE CONVERSIÓN ADICIONALES ==========
    
    def convert_csv_to_parquet_robust(self, file_path: str, parquet_path: str) -> Dict[str, Any]:
        """Delega conversión robusta de CSV"""
        if not self.is_available():
            return build_availability_response(False, True)
        
        if hasattr(self.file_conversion, 'convert_csv_to_parquet_robust'):
            return self.file_conversion.convert_csv_to_parquet_robust(file_path, parquet_path)
        else:
            return {"success": False, "error": "Método no disponible"}
    
    def convert_excel_to_parquet(self, file_path: str, parquet_path: str) -> Dict[str, Any]:
        """Delega conversión de Excel"""
        if not self.is_available():
            return build_availability_response(False, True)
        
        if hasattr(self.file_conversion, 'convert_excel_to_parquet'):
            return self.file_conversion.convert_excel_to_parquet(file_path, parquet_path)
        else:
            return {"success": False, "error": "Método no disponible"}
    
    # ========== MÉTODOS DE UTILIDAD ==========
    
    def escape_identifier(self, name: str) -> str:
        """Escape robusto para identificadores SQL"""
        return SQLUtils().escape_identifier(name)
    
    def _sanitize_table_name(self, table_name: str) -> str:
        """Sanitiza nombres de tabla para DuckDB"""
        return SQLUtils().sanitize_table_name(table_name)
    
    # ========== MÉTODOS DE ADMINISTRACIÓN ==========
    
    def manual_reload_files(self):
        """Recarga manual de archivos (útil para debugging)"""
        print("Recarga manual de archivos solicitada...")
        try:
            self.loaded_tables.clear()
            
            # CORREGIDO: Usar la instancia singleton
            from services.aux_duckdb_services.recover_cache_files import recover_cache_files
            recover_cache_files.auto_recover_cached_files(
                self.metadata_dir, self.controllers['cache'], self.loaded_tables
            )
            
            return {
                "success": True,
                "loaded_count": len(self.loaded_tables),
                "loaded_files": list(self.loaded_tables.keys())
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
        
    def get_loaded_files_info(self) -> Dict[str, Any]:
        """Información detallada de archivos cargados"""
        return {
            "total_loaded": len(self.loaded_tables),
            "files": {
                file_id: {
                    "table_name": info.get("table_name"),
                    "type": info.get("type"),
                    "loaded_at": info.get("loaded_at"),
                    "recovered": info.get("recovered", False)
                }
                for file_id, info in self.loaded_tables.items()
            }
        }
    
    # ========== MÉTODOS DE COMPATIBILIDAD ==========
    
    def run_sql_df(self, sql: str) -> pd.DataFrame:
        """Ejecuta SQL y retorna DataFrame"""
        return self.conn.execute(sql).fetchdf()
    
    def close(self):
        """Cierra conexión DuckDB de forma segura"""
        self.connection_manager.close()
    
    # ========== MÉTODO PRIVADO DE CARGA BAJO DEMANDA ==========
    
    def _load_file_on_demand(self, file_id: str) -> bool:
        """Método de compatibilidad para carga bajo demanda"""
        return self.file_loader_service.load_file_on_demand(
            file_id, 
            self.file_conversion, 
            self.query
        )
    
    def query_data_ultra_fast(
        self,
        file_id: str,
        filters: Optional[List[Dict[str, Any]]] = None,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "ASC",
        page: int = 1,
        page_size: int = 1000,
        selected_columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Consulta ultra-rápida de datos con paginación, filtros y búsqueda.
        """
        if not self.is_available():
            return {
                "success": False,
                "error": "DuckDB no disponible",
                "data": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
                "has_next": False,
                "has_previous": False
            }
        
        try:
            # Cargar archivo bajo demanda si no está en memoria
            if file_id not in self.loaded_tables:
                loaded = self._load_file_on_demand(file_id)
                if not loaded:
                    return {
                        "success": False,
                        "error": f"No se pudo cargar archivo: {file_id}",
                        "data": [],
                        "total": 0,
                        "page": page,
                        "page_size": page_size,
                        "has_next": False,
                        "has_previous": False
                    }
            
            # CORRECCIÓN: Crear QueryPagination sin argumentos
            query_pagination = QueryPagination()
            
            # MONKEY PATCH: Inyectar método _escape_identifier si no existe
            if not hasattr(query_pagination, '_escape_identifier'):
                sql_utils = SQLUtils()
                query_pagination._escape_identifier = lambda name: sql_utils.escape_identifier(name)
            
            result = query_pagination.query_data_ultra_fast(
                conn=self.conn,
                file_id=file_id,
                filters=filters,
                search=search,
                sort_by=sort_by,
                sort_order=sort_order,
                page=page,
                page_size=page_size,
                selected_columns=selected_columns,
                loaded_tables=self.loaded_tables
            )
            
            # Normalizar respuesta: asegurar que 'total' esté en la raíz
            if result.get("success"):
                # Si 'total' no está, extraerlo de pagination
                if "total" not in result and "pagination" in result:
                    result["total"] = result["pagination"].get("total_rows", 0)
                    result["has_next"] = result["pagination"].get("has_next", False)
                    result["has_previous"] = result["pagination"].get("has_prev", False)
                # Si aún no está, usar total_rows
                elif "total" not in result and "total_rows" in result:
                    result["total"] = result["total_rows"]
            else:
                # Si falló, asegurar estructura mínima
                result.setdefault("total", 0)
                result.setdefault("has_next", False)
                result.setdefault("has_previous", False)
            
            return result
            
        except Exception as e:
            import traceback
            print(f"Error en query_data_ultra_fast: {e}")
            print(traceback.format_exc())
            return {
                "success": False,
                "error": f"Error ejecutando consulta: {str(e)}",
                "data": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
                "has_next": False,
                "has_previous": False
            }


    


def get_duckdb_service():
    """Obtiene la instancia singleton del servicio DuckDB"""
    return DuckDBService()


# Crear instancia global de forma segura
try:
    duckdb_service = get_duckdb_service()
    print("DuckDB Service global inicializado")
except Exception as e:
    print(f"Error inicializando DuckDB Service global: {e}")
    duckdb_service = None


__all__ = ['DuckDBService', 'duckdb_service', 'get_duckdb_service']
