import duckdb
import os
import shutil
import json
import re
import time
from typing import Dict, Any, List, Optional
from datetime import datetime

import pandas as pd
from controllers.duckdb_controller.file_validation_controller import FileValidationController
from controllers.duckdb_controller.cache_controller import CacheController
from controllers.duckdb_controller.file_conversion_controller import FileConversionController
from controllers.duckdb_controller.excel_sheets_controller import ExcelSheetsController
from controllers.duckdb_controller.query_controller import QueryController
from controllers.duckdb_controller.cross_files_controller import CrossFilesController

class DuckDBService:
    """Servicio principal para DuckDB - Con manejo robusto de errores y auto-recuperación"""
    
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
            
            print(f"✅ DuckDB inicializado: {self.db_path}")
            print(f"📁 Parquet cache: {self.parquet_dir}")
            print(f"📋 Metadata cache: {self.metadata_dir}")
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
        """Inicializa todos los controladores y ejecuta auto-recuperación"""
        try:
            self.file_validation = FileValidationController(self.conn)
            self.cache = CacheController(self.parquet_dir, self.metadata_dir)
            self.file_conversion = FileConversionController(self.conn, self.parquet_dir, self.cache)
            self.excel_sheets = ExcelSheetsController()
            self.query = QueryController(self.conn, self.loaded_tables)
            self.cross_files = CrossFilesController(self.conn, self.loaded_tables)
            
            print("✅ Controladores DuckDB inicializados")
            
            # ✅ NUEVO: Auto-recuperación después de inicializar controladores
            self._auto_recover_cached_files()
            
        except Exception as e:
            print(f"❌ Error inicializando controladores: {e}")

    def _auto_recover_cached_files(self):
        """Auto-recupera archivos desde cache al reiniciar"""
        try:
            print("🔄 Iniciando auto-recuperación de archivos desde cache...")
            
            recovered_count = 0
            
            # Obtener todos los archivos de metadata
            if not os.path.exists(self.metadata_dir):
                print("📁 No hay metadata cache para recuperar")
                return
            
            # Buscar archivos de metadata
            for metadata_file in os.listdir(self.metadata_dir):
                if not metadata_file.endswith('_metadata.json'):
                    continue
                    
                try:
                    # Leer metadata
                    metadata_path = os.path.join(self.metadata_dir, metadata_file)
                    
                    with open(metadata_path, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)
                    
                    file_hash = metadata.get('file_hash')
                    if not file_hash:
                        continue
                        
                    parquet_path = self.cache.get_cached_parquet_path(file_hash)
                    
                    # Verificar que el Parquet existe
                    if not os.path.exists(parquet_path):
                        print(f"⚠️ Parquet no encontrado para hash {file_hash}: {parquet_path}")
                        continue
                    
                    # Buscar file_id correspondiente en el file_controller
                    file_id = self._find_file_id_for_metadata(metadata)
                    
                    if file_id:
                        # Recargar en DuckDB
                        self._recover_single_file(file_id, parquet_path, metadata)
                        recovered_count += 1
                        print(f"✅ Recuperado: {metadata.get('original_name')} ({file_id})")
                    else:
                        print(f"⚠️ No se encontró file_id para {metadata.get('original_name')}")
                        
                except Exception as e:
                    print(f"❌ Error recuperando {metadata_file}: {e}")
                    continue
            
            if recovered_count > 0:
                print(f"🎯 Auto-recuperación completada: {recovered_count} archivos restaurados")
                print(f"📊 Archivos cargados en DuckDB: {list(self.loaded_tables.keys())}")
            else:
                print("📋 No hay archivos para auto-recuperar")
                
        except Exception as e:
            print(f"❌ Error en auto-recuperación: {e}")

    def _find_file_id_for_metadata(self, metadata: Dict[str, Any]) -> Optional[str]:
        """Busca el file_id correspondiente para una metadata"""
        try:
            # Importar file_controller de forma segura
            from controllers import file_controller
            
            # Obtener todos los archivos del file_controller
            all_files_info = file_controller.list_all_files()
            
            if not all_files_info or not all_files_info.get("files"):
                return None
            
            original_name = metadata.get('original_name', '')
            file_size = metadata.get('original_size_mb', 0) * 1024 * 1024  # Convertir a bytes
            
            # Buscar por coincidencia de nombre y tamaño
            for file_info in all_files_info.get("files", []):
                # Comparar por nombre original
                if file_info.get("original_name") == original_name:
                    # Si también coincide el tamaño (aproximadamente), es muy probable que sea el mismo
                    info_size = file_info.get("size_mb", 0) * 1024 * 1024 if file_info.get("size_mb") else 0
                    
                    if abs(file_size - info_size) < 1024:  # Diferencia menor a 1KB
                        return file_info.get("file_id")
                    else:
                        # Si el nombre coincide pero no el tamaño, aún puede ser válido
                        return file_info.get("file_id")
            
            return None
            
        except Exception as e:
            print(f"❌ Error buscando file_id: {e}")
            return None

    def _recover_single_file(self, file_id: str, parquet_path: str, metadata: Dict[str, Any]):
        """Recupera un archivo individual en DuckDB"""
        try:
            # Registrar en loaded_tables
            table_name = self._sanitize_table_name(f"table_{file_id}")
            
            self.loaded_tables[file_id] = {
                "table_name": table_name,
                "parquet_path": parquet_path,
                "loaded_at": datetime.now().isoformat(),
                "load_time": 0.001,  # Recovered, no load time
                "type": "lazy",
                "recovered": True,
                "original_metadata": metadata
            }
            
            print(f"📋 Archivo recuperado en loaded_tables: {file_id} → {table_name}")
            
        except Exception as e:
            print(f"❌ Error recuperando archivo individual {file_id}: {e}")

    def _load_file_on_demand(self, file_id: str) -> bool:
        """Carga un archivo bajo demanda si existe en cache"""
        try:
            print(f"🔍 Buscando archivo {file_id} en cache para carga bajo demanda...")
            
            # Buscar en metadata cache por file_id o nombre
            if not os.path.exists(self.metadata_dir):
                return False
            
            # Buscar archivo en file_controller
            from controllers import file_controller
            
            try:
                file_info = file_controller.get_file_info(file_id)
                file_path = file_info.get("path")
                original_name = file_info.get("original_name")
                extension = file_info.get("extension", "xlsx")
            except:
                print(f"❌ No se pudo obtener info del archivo {file_id}")
                return False
            
            # Buscar Parquet existente por nombre
            parquet_found = None
            metadata_found = None
            
            for metadata_file in os.listdir(self.metadata_dir):
                if not metadata_file.endswith('_metadata.json'):
                    continue
                    
                try:
                    metadata_path = os.path.join(self.metadata_dir, metadata_file)
                    
                    with open(metadata_path, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)
                    
                    # Verificar si coincide el nombre original
                    if metadata.get('original_name') == original_name:
                        file_hash = metadata.get('file_hash')
                        parquet_path = self.cache.get_cached_parquet_path(file_hash)
                        
                        if os.path.exists(parquet_path):
                            parquet_found = parquet_path
                            metadata_found = metadata
                            break
                            
                except Exception as e:
                    continue
            
            if parquet_found:
                # Cargar desde cache existente
                self._recover_single_file(file_id, parquet_found, metadata_found)
                return True
            else:
                # No hay cache, convertir desde archivo original
                if file_path and os.path.exists(file_path):
                    print(f"🔄 Convirtiendo archivo {file_id} desde cero...")
                    
                    result = self.convert_file_to_parquet(
                        file_path=file_path,
                        file_id=file_id,
                        original_name=original_name,
                        ext=extension
                    )
                    
                    if result.get("success"):
                        self.load_parquet_lazy(file_id, result["parquet_path"])
                        return True
            
            return False
            
        except Exception as e:
            print(f"❌ Error en carga bajo demanda para {file_id}: {e}")
            return False

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

    # ========== MÉTODOS DELEGADOS CON CARGA BAJO DEMANDA ==========

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
        if not self.is_available():
            return {
                "success": False,
                "error": "DuckDB no disponible para cruce de archivos",
                "requires_fallback": True
            }
        
        # ✅ VERIFICAR Y CARGAR ARCHIVOS BAJO DEMANDA
        for file_id in [file1_id, file2_id]:
            if file_id not in self.loaded_tables:
                print(f"🔄 Archivo {file_id} no cargado, intentando carga bajo demanda...")
                
                if self._load_file_on_demand(file_id):
                    print(f"✅ Archivo {file_id} cargado bajo demanda")
                else:
                    print(f"❌ No se pudo cargar archivo {file_id} bajo demanda")
                    return {
                        "success": False,
                        "error": f"Archivo {file_id} no se puede cargar en DuckDB",
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
        """Query ultra-rápida CON PAGINACIÓN COMPLETA - evita KeyError"""
        
        try:
            # ✅ VERIFICAR Y REGENERAR PARQUET SI ES NECESARIO
            if file_id not in self.loaded_tables:
                print(f"🔄 Archivo {file_id} no cargado, cargando bajo demanda...")
                if not self._load_file_on_demand_with_regeneration(file_id):
                    raise Exception(f"No se pudo cargar archivo {file_id}")
            else:
                # Verificar que el Parquet exista físicamente
                if not self.ensure_parquet_exists_or_regenerate(file_id):
                    raise Exception(f"No se pudo asegurar la existencia del Parquet para {file_id}")
            
            # Obtener referencia a la tabla
            table_info = self.loaded_tables[file_id]
            
            if table_info.get("type") == "lazy":
                table_ref = f"read_parquet('{table_info['parquet_path']}')"
            else:
                table_ref = table_info["table_name"]
            
            # ✅ CONSTRUIR COLUMNAS PARA SELECT
            if selected_columns:
                columns_clause = ", ".join([self._escape_identifier(col) for col in selected_columns])
            else:
                columns_clause = "*"
            
            # ✅ CONSTRUIR CONDICIONES WHERE (COMPARTIDAS ENTRE COUNT Y SELECT)
            where_conditions = []
            
            # Aplicar filtros
            if filters:
                for filter_item in filters:
                    condition = self._build_filter_condition(filter_item)
                    if condition:
                        where_conditions.append(condition)
            
            # Aplicar búsqueda
            if search:
                search_condition = self._build_search_condition(search, table_info)
                if search_condition:
                    where_conditions.append(search_condition)
            
            # Construir cláusula WHERE
            where_clause = ""
            if where_conditions:
                where_clause = f" WHERE {' AND '.join(where_conditions)}"
            
            # ✅ PASO 1: OBTENER TOTAL DE REGISTROS (CON FILTROS APLICADOS)
            count_query = f"SELECT COUNT(*) FROM {table_ref}{where_clause}"
            
            print(f"🔢 Contando registros totales...")
            count_result = self.conn.execute(count_query).fetchone()
            total_records = count_result[0] if count_result else 0
            
            print(f"📊 Total de registros (con filtros): {total_records:,}")
            
            # ✅ PASO 2: CALCULAR INFORMACIÓN DE PAGINACIÓN
            import math
            total_pages = math.ceil(total_records / page_size) if total_records > 0 else 1
            has_next = page < total_pages
            has_prev = page > 1
            
            pagination_info = {
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "total_rows": total_records,  # ✅ TOTAL REAL, NO SOLO DE LA PÁGINA
                "has_next": has_next,
                "has_prev": has_prev,
                "next_page": page + 1 if has_next else None,
                "prev_page": page - 1 if has_prev else None
            }
            
            # ✅ PASO 3: CONSTRUIR QUERY PAGINADA
            base_query = f"SELECT {columns_clause} FROM {table_ref}{where_clause}"
            
            # Aplicar ordenamiento
            if sort_by:
                escaped_sort = self._escape_identifier(sort_by)
                base_query += f" ORDER BY {escaped_sort} {sort_order}"
            
            # Aplicar paginación
            offset = (page - 1) * page_size
            paginated_query = f"{base_query} LIMIT {page_size} OFFSET {offset}"
            
            print(f"📄 Ejecutando query paginada: página {page} de {total_pages}")
            
            # ✅ PASO 4: EJECUTAR QUERY PAGINADA
            result = self.conn.execute(paginated_query).fetchall()
            
            # ✅ PASO 5: OBTENER COLUMNAS
            if result:
                # Usar la misma query sin LIMIT para obtener columnas
                columns_query = f"{base_query} LIMIT 0"
                columns_result = self.conn.execute(columns_query).description
                columns = [col[0] for col in columns_result]
            else:
                # Si no hay datos, obtener columnas de la tabla directamente
                describe_query = f"DESCRIBE {table_ref}"
                describe_result = self.conn.execute(describe_query).fetchall()
                columns = [row[0] for row in describe_result]
            
            # ✅ PASO 6: CONVERTIR RESULTADO A DICCIONARIOS
            data = []
            for row in result:
                row_dict = {}
                for i, col_name in enumerate(columns):
                    if i < len(row):  # Protección contra índices fuera de rango
                        row_dict[col_name] = row[i]
                    else:
                        row_dict[col_name] = None
                data.append(row_dict)
            
            print(f"✅ Query completada: {len(data)} registros de página {page}")
            
            # ✅ PASO 7: RETORNO COMPLETO CON PAGINATION
            return {
                "success": True,
                "data": data,
                "columns": columns,
                
                # ✅ INFORMACIÓN DE PAGINACIÓN COMPLETA (evita KeyError)
                "pagination": pagination_info,
                
                # Campos adicionales para compatibilidad
                "total_rows": total_records,  # Total real con filtros
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "has_next": has_next,
                "has_prev": has_prev,
                
                # Metadata
                "method": "ultra_fast_with_complete_pagination",
                "filters_applied": len(filters) if filters else 0,
                "search_applied": bool(search),
                "sort_applied": bool(sort_by)
            }
            
        except Exception as e:
            error_msg = str(e)
            
            # ✅ DETECTAR ERROR DE ARCHIVO FALTANTE ESPECÍFICAMENTE
            if "No files found that match the pattern" in error_msg:
                print(f"🔧 Detectado error de Parquet faltante, intentando regeneración...")
                
                try:
                    # Forzar regeneración
                    if self.ensure_parquet_exists_or_regenerate(file_id):
                        print(f"✅ Parquet regenerado, reintentando consulta...")
                        # Reintentar consulta una vez (evitar recursión infinita)
                        return self.query_data_ultra_fast(
                            file_id, filters, search, sort_by, sort_order, page, page_size, selected_columns
                        )
                    else:
                        raise Exception(f"No se pudo regenerar Parquet para {file_id}")
                except Exception as regen_error:
                    raise Exception(f"Error regenerando Parquet: {regen_error}")
            
            print(f"❌ Error en query_data_ultra_fast: {e}")
            
            # ✅ RETORNO DE ERROR CON PAGINATION VACÍA (evita KeyError)
            return {
                "success": False,
                "error": error_msg,
                "data": [],
                "columns": [],
                
                # ✅ PAGINATION VACÍA PERO PRESENTE (evita KeyError)
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_pages": 0,
                    "total_rows": 0,
                    "has_next": False,
                    "has_prev": False,
                    "next_page": None,
                    "prev_page": None
                },
                
                # Campos adicionales
                "total_rows": 0,
                "page": page,
                "page_size": page_size,
                "total_pages": 0,
                "has_next": False,
                "has_prev": False,
                "method": "error_with_pagination"
            }



    def get_unique_values_ultra_fast(self, file_id: str, column_name: str, limit: int = 1000) -> List[str]:
        """Delega valores únicos ultra-rápidos con carga bajo demanda"""
        if not self.is_available():
            return []
        
        # ✅ VERIFICAR Y CARGAR ARCHIVO BAJO DEMANDA
        if file_id not in self.loaded_tables:
            if not self._load_file_on_demand(file_id):
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

    def escape_identifier(self, name: str) -> str:
        """Escape robusto para identificadores SQL con espacios"""
        if not name:
            return '""'
        
        # Convertir a string y limpiar
        name = str(name).strip()
        
        if not name:
            return '""'
        
        # CASOS ESPECIALES
        if name.lower() in ("*", "all"):
            return name  # No escapar wildcards
        
        # ✅ ESCAPE SEGURO: Siempre usar comillas dobles para nombres con espacios
        escaped = name.replace('"', '""')  # Escapar comillas internas
        return f'"{escaped}"'

    # ========== MÉTODOS DE ADMINISTRACIÓN ==========

    def manual_reload_files(self):
        """Recarga manual de archivos (útil para debugging)"""
        print("🔄 Recarga manual de archivos solicitada...")
        try:
            self.loaded_tables.clear()
            self._auto_recover_cached_files()
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
    
    def ensure_parquet_exists_or_regenerate(self, table_key: str) -> bool:
        """Verifica que el Parquet existe físicamente, si no lo regenera"""
        try:
            if table_key not in self.loaded_tables:
                print(f"⚠️ Tabla {table_key} no está en cache")
                return False
            
            table_info = self.loaded_tables[table_key]
            parquet_path = table_info.get('parquet_path')
            
            if not parquet_path:
                print(f"⚠️ No hay ruta de Parquet para {table_key}")
                return False
            
            # ✅ VERIFICACIÓN FÍSICA DEL ARCHIVO
            if not os.path.exists(parquet_path):
                print(f"❌ Archivo Parquet no existe físicamente: {parquet_path}")
                print(f"🔄 Intentando regenerar desde fuente original...")
                
                # Remover de cache para forzar regeneración
                del self.loaded_tables[table_key]
                return False
            
            # ✅ VERIFICAR QUE EL ARCHIVO NO ESTÉ VACÍO O CORRUPTO
            try:
                file_size = os.path.getsize(parquet_path)
                if file_size == 0:
                    print(f"❌ Archivo Parquet está vacío: {parquet_path}")
                    os.remove(parquet_path)
                    del self.loaded_tables[table_key]
                    return False
                
                print(f"✅ Parquet verificado: {parquet_path} ({file_size:,} bytes)")
                return True
                
            except Exception as file_error:
                print(f"❌ Error verificando archivo Parquet: {file_error}")
                return False
            
        except Exception as e:
            print(f"❌ Error en ensure_parquet_exists_or_regenerate: {e}")
            return False

    def _load_file_on_demand_with_regeneration(self, table_key: str) -> bool:
        """Carga archivo con regeneración automática si es necesario"""
        try:
            # ✅ SI ESTÁ EN CACHE, VERIFICAR EXISTENCIA FÍSICA
            if table_key in self.loaded_tables:
                if self.ensure_parquet_exists_or_regenerate(table_key):
                    return True
                # Si no existe físicamente, continuar con regeneración
            
            print(f"🔄 Carga bajo demanda no disponible para {table_key}")
            print(f"💡 Usa read_technical_file_data_paginated() para forzar conversión")
            return False
            
        except Exception as e:
            print(f"❌ Error en carga bajo demanda: {e}")
            return False

    def close(self):
        """Cierra conexión DuckDB de forma segura"""
        try:
            if self.conn:
                self.conn.close()
                print("✅ Conexión DuckDB cerrada")
        except Exception as e:
            print(f"⚠️ Error cerrando DuckDB: {e}")

    def run_sql_df(self, sql: str) -> pd.DataFrame:
        return self.conn.execute(sql).fetchdf()

    def escape_identifier(self, name: str) -> str:
        # escape básico para identificadores con comillas dobles
        return f"\"{name.replace('\"', '\"\"')}\""
    
    def _build_filter_condition(self, filter_item: Dict[str, Any]) -> Optional[str]:
        """
        Construye una condición WHERE SQL basada en un objeto de filtro
        
        filter_item formato esperado:
        {
            'column': 'Regional', 
            'operator': 'in', 
            'values': ['REGIONAL BOGOTA']
        }
        """
        try:
            column = filter_item.get('column')
            operator = filter_item.get('operator', '=').lower()
            values = filter_item.get('values', filter_item.get('value'))
            
            if not column or values is None:
                print(f"⚠️ Filtro incompleto: {filter_item}")
                return None
            
            # Asegurar que values sea una lista
            if not isinstance(values, list):
                values = [values]
            
            # Escapar nombre de columna
            escaped_column = self._escape_identifier(column)
            
            # ✅ FUNCIÓN AUXILIAR PARA ESCAPAR VALORES
            def escape_value(val):
                return str(val).replace("'", "''")
            
            # ✅ CONSTRUCCIÓN DE CONDICIONES SQL (COMILLAS CORREGIDAS)
            if operator in ['=', 'eq', 'equals']:
                if len(values) == 0:
                    return None
                escaped_val = escape_value(values[0])
                return f"{escaped_column} = '{escaped_val}'"
                
            elif operator in ['!=', 'ne', 'not_equals']:
                if len(values) == 0:
                    return None
                escaped_val = escape_value(values[0])
                return f"{escaped_column} != '{escaped_val}'"
                
            elif operator in ['in', 'IN']:
                if len(values) == 0:
                    return None
                # Escapar cada valor y construir lista
                escaped_values = [f"'{escape_value(v)}'" for v in values if v is not None]
                if not escaped_values:
                    return None
                values_str = ', '.join(escaped_values)
                return f"{escaped_column} IN ({values_str})"
                
            elif operator in ['not_in', 'not in', 'NOT IN']:
                if len(values) == 0:
                    return None
                escaped_values = [f"'{escape_value(v)}'" for v in values if v is not None]
                if not escaped_values:
                    return None
                values_str = ', '.join(escaped_values)
                return f"{escaped_column} NOT IN ({values_str})"
                
            elif operator in ['contains', 'like']:
                if len(values) == 0:
                    return None
                search_term = escape_value(values[0])
                return f"{escaped_column} LIKE '%{search_term}%'"
                
            elif operator in ['starts_with', 'startswith']:
                if len(values) == 0:
                    return None
                search_term = escape_value(values[0])
                return f"{escaped_column} LIKE '{search_term}%'"
                
            elif operator in ['ends_with', 'endswith']:
                if len(values) == 0:
                    return None
                search_term = escape_value(values[0])
                return f"{escaped_column} LIKE '%{search_term}'"
                
            elif operator in ['>', 'gt', 'greater_than']:
                if len(values) == 0:
                    return None
                escaped_val = escape_value(values[0])
                return f"{escaped_column} > '{escaped_val}'"
                
            elif operator in ['>=', 'gte', 'greater_than_or_equal']:
                if len(values) == 0:
                    return None
                escaped_val = escape_value(values[0])
                return f"{escaped_column} >= '{escaped_val}'"
                
            elif operator in ['<', 'lt', 'less_than']:
                if len(values) == 0:
                    return None
                escaped_val = escape_value(values[0])
                return f"{escaped_column} < '{escaped_val}'"
                
            elif operator in ['<=', 'lte', 'less_than_or_equal']:
                if len(values) == 0:
                    return None
                escaped_val = escape_value(values[0])
                return f"{escaped_column} <= '{escaped_val}'"
                
            elif operator in ['between']:
                if len(values) < 2:
                    return None
                val1 = escape_value(values[0])
                val2 = escape_value(values[1])
                return f"{escaped_column} BETWEEN '{val1}' AND '{val2}'"
                
            elif operator in ['is_null', 'null']:
                return f"{escaped_column} IS NULL"
                
            elif operator in ['is_not_null', 'not_null']:
                return f"{escaped_column} IS NOT NULL"
                
            else:
                print(f"⚠️ Operador no soportado: {operator}")
                return None
                
        except Exception as e:
            print(f"❌ Error construyendo filtro: {e}")
            print(f"   Filtro problemático: {filter_item}")
            return None

    def _build_search_condition(self, search_term: str, table_info: Dict[str, Any]) -> Optional[str]:
        """
        Construye condición de búsqueda que aplica a todas las columnas de texto
        """
        try:
            if not search_term or not search_term.strip():
                return None
            
            # Limpiar término de búsqueda
            clean_search = search_term.strip().replace("'", "''")
            
            # Obtener columnas disponibles
            columns = []
            if table_info.get("type") == "lazy":
                # Para lazy load, obtener columnas del parquet
                parquet_path = table_info.get("parquet_path")
                if parquet_path:
                    try:
                        describe_sql = f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}') LIMIT 0"
                        describe_result = self.conn.execute(describe_sql).fetchall()
                        columns = [row[0] for row in describe_result]
                    except:
                        columns = []
            else:
                # Para tablas normales
                table_name = table_info.get("table_name")
                if table_name:
                    try:
                        describe_sql = f"DESCRIBE {table_name}"
                        describe_result = self.conn.execute(describe_sql).fetchall()
                        columns = [row[0] for row in describe_result]
                    except:
                        columns = []
            
            if not columns:
                print("⚠️ No se pudieron obtener columnas para búsqueda")
                return None
            
            # ✅ CONSTRUIR BÚSQUEDA EN TODAS LAS COLUMNAS (STRING)
            search_conditions = []
            for column in columns:
                escaped_column = self._escape_identifier(column)
                # Convertir a string y buscar (DuckDB automáticamente maneja conversiones)
                condition = f"CAST({escaped_column} AS VARCHAR) LIKE '%{clean_search}%'"
                search_conditions.append(condition)
            
            if search_conditions:
                # Unir con OR - la búsqueda encuentra en cualquier columna
                return f"({' OR '.join(search_conditions)})"
            else:
                return None
                
        except Exception as e:
            print(f"❌ Error construyendo búsqueda: {e}")
            return None

    def _sanitize_table_name(self, table_name: str) -> str:
        """Sanitiza nombres de tabla para DuckDB"""
        if not table_name:
            return "temp_table"
        
        # Reemplazar caracteres problemáticos
        import re
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', str(table_name))
        
        # Asegurar que no empiece con número
        if sanitized and sanitized[0].isdigit():
            sanitized = f"table_{sanitized}"
        
        return sanitized or "temp_table"


# Función para obtener la instancia
def get_duckdb_service():
    """Obtiene la instancia singleton del servicio DuckDB"""
    return DuckDBService()

# Crear instancia global de forma segura
try:
    duckdb_service = get_duckdb_service()
    print("✅ DuckDB Service global inicializado")
except Exception as e:
    print(f"❌ Error inicializando DuckDB Service global: {e}")
    duckdb_service = None

__all__ = ['DuckDBService', 'duckdb_service', 'get_duckdb_service']
