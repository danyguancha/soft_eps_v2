import duckdb
import os
import shutil
from typing import Dict, Any, List, Optional
from datetime import datetime
from controllers.duckdb_controller.file_validation_controller import FileValidationController
from controllers.duckdb_controller.cache_controller import CacheController
from controllers.duckdb_controller.file_conversion_controller import FileConversionController
from controllers.duckdb_controller.excel_sheets_controller import ExcelSheetsController
from controllers.duckdb_controller.query_controller import QueryController
from controllers.duckdb_controller.cross_files_controller import CrossFilesController

class DuckDBService:
    """Servicio principal para DuckDB - Con manejo robusto de errores de encoding"""
    
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
            
        # Directorios
        self.parquet_dir = os.path.abspath("parquet_cache")
        self.duckdb_dir = os.path.abspath("duckdb_storage") 
        self.metadata_dir = os.path.abspath("metadata_cache")
        
        # Crear directorios
        os.makedirs(self.parquet_dir, exist_ok=True)
        os.makedirs(self.duckdb_dir, exist_ok=True)
        os.makedirs(self.metadata_dir, exist_ok=True)
        
        # Inicializar conexión DuckDB con manejo robusto
        self.db_path = os.path.join(self.duckdb_dir, "main.duckdb")
        self.conn = self._initialize_duckdb_connection()
        
        # Registro de tablas cargadas
        self.loaded_tables: Dict[str, Dict[str, Any]] = {}
        
        # Inicializar controladores solo si la conexión es exitosa
        if self.conn:
            self._initialize_controllers()
            self._initialized = True
        else:
            print("❌ DuckDB no se pudo inicializar - Funcionando en modo fallback")

    def _initialize_duckdb_connection(self):
        """Inicializa conexión DuckDB con manejo robusto de errores"""
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                print(f"🔄 Intento {attempt + 1}/{max_attempts} inicializando DuckDB...")
                
                # Intentar conectar
                conn = duckdb.connect(self.db_path)
                
                # Configurar DuckDB para máximo rendimiento
                conn.execute("PRAGMA threads=4")
                conn.execute("PRAGMA memory_limit='8GB'")
                conn.execute("SET enable_progress_bar=true")
                
                # Test de conexión
                conn.execute("SELECT 1").fetchone()
                
                print(f"✅ DuckDB conectado exitosamente en intento {attempt + 1}")
                return conn
                
            except UnicodeDecodeError as e:
                print(f"❌ Error de encoding en DuckDB (intento {attempt + 1}): {e}")
                self._handle_corrupted_database(attempt)
                
            except Exception as e:
                print(f"❌ Error general en DuckDB (intento {attempt + 1}): {e}")
                self._handle_corrupted_database(attempt)
        
        # Si todos los intentos fallan, crear conexión en memoria
        print("⚠️ Usando DuckDB en memoria como fallback")
        try:
            conn = duckdb.connect(":memory:")
            conn.execute("PRAGMA threads=2")
            conn.execute("PRAGMA memory_limit='4GB'")
            return conn
        except Exception as e:
            print(f"❌ Error crítico: No se puede inicializar DuckDB: {e}")
            return None

    def _handle_corrupted_database(self, attempt: int):
        """Maneja base de datos corrupta"""
        try:
            if attempt == 0:
                # Primer intento: mover base de datos corrupta
                backup_name = f"main_corrupted_{datetime.now().strftime('%Y%m%d_%H%M%S')}.duckdb"
                backup_path = os.path.join(self.duckdb_dir, backup_name)
                
                if os.path.exists(self.db_path):
                    shutil.move(self.db_path, backup_path)
                    print(f"📦 Base de datos corrupta respaldada como: {backup_name}")
                
            elif attempt == 1:
                # Segundo intento: limpiar completamente el directorio
                if os.path.exists(self.duckdb_dir):
                    shutil.rmtree(self.duckdb_dir)
                    os.makedirs(self.duckdb_dir, exist_ok=True)
                    print("🧹 Directorio DuckDB limpiado completamente")
                
            else:
                # Último intento: limpiar todo el cache
                for dir_path in [self.duckdb_dir, self.parquet_dir, self.metadata_dir]:
                    if os.path.exists(dir_path):
                        shutil.rmtree(dir_path)
                        os.makedirs(dir_path, exist_ok=True)
                print("🧹 Todo el cache DuckDB limpiado")
                
        except Exception as cleanup_error:
            print(f"❌ Error limpiando base de datos corrupta: {cleanup_error}")

    def _initialize_controllers(self):
        """Inicializa todos los controladores solo si DuckDB está funcionando"""
        try:
            self.file_validation = FileValidationController(self.conn)
            self.cache = CacheController(self.parquet_dir, self.metadata_dir)
            self.file_conversion = FileConversionController(self.conn, self.parquet_dir, self.cache)
            self.excel_sheets = ExcelSheetsController()
            self.query = QueryController(self.conn, self.loaded_tables)
            self.cross_files = CrossFilesController(self.conn, self.loaded_tables)
            print("✅ Controladores DuckDB inicializados")
        except Exception as e:
            print(f"❌ Error inicializando controladores: {e}")

    def is_available(self) -> bool:
        """Verifica si DuckDB está disponible y funcionando"""
        try:
            if not self.conn:
                return False
            self.conn.execute("SELECT 1").fetchone()
            return True
        except:
            return False

    def restart_connection(self):
        """Reinicia la conexión DuckDB de forma segura"""
        try:
            print("🔄 Reiniciando conexión DuckDB...")
            
            if self.conn:
                try:
                    self.conn.close()
                except:
                    pass
            
            self.conn = self._initialize_duckdb_connection()
            
            if self.conn:
                # Actualizar referencia en controladores
                if hasattr(self, 'file_validation'):
                    self.file_validation.conn = self.conn
                if hasattr(self, 'file_conversion'):
                    self.file_conversion.conn = self.conn
                if hasattr(self, 'query'):
                    self.query.conn = self.conn
                if hasattr(self, 'cross_files'):
                    self.cross_files.conn = self.conn
                
                print("✅ Conexión DuckDB reiniciada exitosamente")
                return True
            else:
                print("❌ No se pudo reiniciar la conexión DuckDB")
                return False
                
        except Exception as e:
            print(f"❌ Error reiniciando conexión: {e}")
            return False

    # ========== MÉTODOS DELEGADOS CON VERIFICACIÓN ==========

    def validate_parquet_file(self, parquet_path: str) -> Dict[str, Any]:
        """Delega validación de archivos Parquet"""
        if not self.is_available():
            return {"valid": False, "error": "DuckDB no disponible"}
        return self.file_validation.validate_parquet_file(parquet_path)

    def get_file_columns_for_cross(self, file_id: str, sheet_name: str = None) -> Dict[str, Any]:
        """Delega obtención de columnas para cruce"""
        if not self.is_available():
            return {
                "success": False,
                "error": "DuckDB no disponible",
                "requires_fallback": True
            }
        return self.file_validation.get_file_columns_for_cross(file_id, sheet_name, self.loaded_tables)

    def convert_file_to_parquet(self, file_path: str, file_id: str, original_name: str, ext: str) -> Dict[str, Any]:
        """Delega conversión de archivos"""
        if not self.is_available():
            return {
                "success": False,
                "error": "DuckDB no disponible para conversión",
                "requires_fallback": True
            }
        return self.file_conversion.convert_file_to_parquet(file_path, file_id, original_name, ext)

    def get_excel_sheets(self, file_path: str) -> Dict[str, Any]:
        """Delega obtención de hojas de Excel"""
        return self.excel_sheets.get_excel_sheets(file_path)

    def load_parquet_lazy(self, file_id: str, parquet_path: str, table_name: Optional[str] = None) -> str:
        """Delega carga lazy de Parquet"""
        if not self.is_available():
            print("⚠️ DuckDB no disponible, simulando carga lazy")
            return f"fallback_table_{file_id}"
        return self.query.load_parquet_lazy(file_id, parquet_path, table_name, self.loaded_tables)

    def get_file_stats(self, file_id: str) -> Dict[str, Any]:
        """Delega estadísticas de archivo"""
        if not self.is_available():
            return {"loaded": False, "error": "DuckDB no disponible"}
        return self.query.get_file_stats(file_id)

    # ========== MÉTODOS DELEGADOS FALTANTES ==========

    def cross_files_ultra_fast(
        self,
        file1_id: str,
        file2_id: str,
        key_column_file1: str,
        key_column_file2: str,
        join_type: str = "LEFT",
        columns_to_include: Optional[Dict[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """Delega cruce de archivos ultra-rápido"""
        if not self.is_available():
            return {
                "success": False,
                "error": "DuckDB no disponible para cruce de archivos",
                "requires_fallback": True
            }
        
        # ✅ DELEGACIÓN CORRECTA al CrossFilesController
        return self.cross_files.cross_files_ultra_fast(
            file1_id, file2_id, key_column_file1, key_column_file2, join_type, columns_to_include
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
        """Delega consultas ultra-rápidas"""
        if not self.is_available():
            return {
                "success": False,
                "error": "DuckDB no disponible para consultas",
                "requires_fallback": True
            }
        
        # ✅ DELEGACIÓN al QueryController
        return self.query.query_data_ultra_fast(
            file_id, filters, search, sort_by, sort_order, page, page_size, selected_columns
        )

    def get_unique_values_ultra_fast(self, file_id: str, column_name: str, limit: int = 1000) -> List[str]:
        """Delega valores únicos ultra-rápidos"""
        if not self.is_available():
            return []
        
        return self.query.get_unique_values_ultra_fast(file_id, column_name, limit)

    def load_parquet_to_view(self, file_id: str, parquet_path: str, table_name: Optional[str] = None) -> str:
        """Delega creación de vista"""
        if not self.is_available():
            return f"fallback_view_{file_id}"
        
        return self.query.load_parquet_to_view(file_id, parquet_path, table_name, self.loaded_tables)

    def load_parquet_to_table(self, file_id: str, parquet_path: str, table_name: Optional[str] = None) -> str:
        """Delega carga de tabla (compatibilidad)"""
        return self.load_parquet_lazy(file_id, parquet_path, table_name)

    def load_parquet_to_table_materialized(self, file_id: str, parquet_path: str, table_name: Optional[str] = None) -> str:
        """Delega materialización completa"""
        if not self.is_available():
            return f"fallback_materialized_{file_id}"
        
        if hasattr(self.query, 'load_parquet_to_table_materialized'):
            return self.query.load_parquet_to_table_materialized(file_id, parquet_path, table_name, self.loaded_tables)
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
            return {
                "success": False,
                "error": "DuckDB no disponible para consulta directa",
                "requires_fallback": True
            }
        
        if hasattr(self.query, 'query_parquet_direct'):
            return self.query.query_parquet_direct(
                file_id, filters, search, sort_by, sort_order, page, page_size, selected_columns
            )
        else:
            # Fallback a query normal
            return self.query_data_ultra_fast(
                file_id, filters, search, sort_by, sort_order, page, page_size, selected_columns
            )

    # ========== MÉTODOS DE CACHE ==========

    def get_cache_stats(self) -> Dict[str, Any]:
        """Delega estadísticas de cache"""
        if not hasattr(self, 'cache') or not self.cache:
            return {
                "total_cached_files": 0,
                "total_cache_size_mb": 0,
                "cache_hit_potential": "N/A",
                "error": "Cache no disponible"
            }
        return self.cache.get_cache_stats()

    def cleanup_old_cache(self, days_old: int = 30, min_access_count: int = 1):
        """Delega limpieza de cache"""
        if not hasattr(self, 'cache') or not self.cache:
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

    # ========== MÉTODOS DE EXCEL ESPECÍFICOS ==========

    def get_columns_from_sheet(self, file_path: str, sheet_name: str) -> Dict[str, Any]:
        """Obtiene columnas de una hoja específica de Excel"""
        try:
            if not self.is_available():
                return {
                    "success": False,
                    "error": "DuckDB no disponible",
                    "requires_fallback": True
                }
            
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
                return {
                    "success": False,
                    "error": "DuckDB no disponible",
                    "requires_fallback": True
                }
            
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

    # ========== MÉTODO DE VALIDACIÓN DE HOJAS ==========

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
            return {
                "success": False,
                "error": "DuckDB no disponible para conversión CSV",
                "requires_fallback": True
            }
        
        if hasattr(self.file_conversion, 'convert_csv_to_parquet_robust'):
            return self.file_conversion.convert_csv_to_parquet_robust(file_path, parquet_path)
        else:
            return {"success": False, "error": "Método no disponible"}

    def convert_excel_to_parquet(self, file_path: str, parquet_path: str) -> Dict[str, Any]:
        """Delega conversión de Excel"""
        if not self.is_available():
            return {
                "success": False,
                "error": "DuckDB no disponible para conversión Excel",
                "requires_fallback": True
            }
        
        if hasattr(self.file_conversion, 'convert_excel_to_parquet'):
            return self.file_conversion.convert_excel_to_parquet(file_path, parquet_path)
        else:
            return {"success": False, "error": "Método no disponible"}

    # ========== MÉTODOS DE VALIDACIÓN ADICIONALES ==========

    def _sanitize_table_name(self, table_name: str) -> str:
        """Convierte nombres de tabla a formato seguro para DuckDB"""
        import re
        import time
        
        # Reemplazar guiones con guiones bajos y otros caracteres problemáticos
        sanitized = table_name.replace('-', '_')
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', sanitized)
        
        # Asegurar que no comience con número
        if sanitized and sanitized[0].isdigit():
            sanitized = "t_" + sanitized
        
        # Asegurar que no esté vacío
        if not sanitized:
            sanitized = "table_" + str(int(time.time()))
            
        return sanitized

    def _escape_identifier(self, identifier: str) -> str:
        """Escape robusto para identificadores SQL"""
        if not identifier:
            return '""'
        
        # Convertir a string y limpiar
        identifier = str(identifier).strip()
        
        if not identifier:
            return '""'
        
        # CASOS ESPECIALES
        if identifier.lower() in ("*", "all"):
            return identifier  # No escapar wildcards
        
        # ESCAPE SEGURO: Siempre usar comillas dobles
        escaped = identifier.replace('"', '""')  # Escapar comillas internas
        return f'"{escaped}"'

    def close(self):
        """Cierra conexión DuckDB de forma segura"""
        try:
            if self.conn:
                self.conn.close()
        except Exception as e:
            print(f"⚠️ Error cerrando DuckDB: {e}")

# Función para obtener la instancia
def get_duckdb_service():
    """Obtiene la instancia singleton del servicio DuckDB"""
    return DuckDBService()

# Crear instancia global de forma segura
try:
    duckdb_service = get_duckdb_service()
except Exception as e:
    print(f"❌ Error inicializando DuckDB Service global: {e}")
    duckdb_service = None

__all__ = ['DuckDBService', 'duckdb_service', 'get_duckdb_service']
