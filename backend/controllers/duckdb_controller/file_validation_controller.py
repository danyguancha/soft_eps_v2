import os
import pandas as pd
import threading
from typing import Dict, Any, List
import time

class FileValidationController:
    """Controlador para validación de archivos Parquet"""
    
    def __init__(self, conn):
        self.conn = conn
        self.max_retries = 3
        self.retry_delay = 1

    def _validate_file_and_parquet(self, file_id: str, loaded_tables: Dict) -> tuple:
        """Valida que el archivo y su parquet existan y sean válidos"""
        # Verificar carga en DuckDB
        if file_id not in loaded_tables:
            return (False, {
                "success": False,
                "error": f"Archivo {file_id} no está cargado en DuckDB",
                "requires_loading": True
            })
        
        # Verificar existencia de parquet
        table_info = loaded_tables[file_id]
        parquet_path = table_info.get("parquet_path")
        
        if not parquet_path or not os.path.exists(parquet_path):
            return (False, {
                "success": False,
                "error": f"Archivo Parquet no encontrado: {parquet_path}",
                "requires_regeneration": True
            })
        
        # Verificar tamaño mínimo
        file_size = os.path.getsize(parquet_path)
        if file_size < 100:
            return (False, {
                "success": False,
                "error": f"Archivo corrupto (muy pequeño: {file_size} bytes)",
                "requires_regeneration": True
            })
        
        return (True, parquet_path)


    def _try_get_columns_with_retry(self, parquet_path: str, file_id: str) -> tuple:
        """Intenta obtener columnas con retry y reconexión"""
        for attempt in range(self.max_retries):
            try:
                columns = self._get_columns_with_timeout_and_retry(parquet_path, timeout_seconds=15)
                
                if columns:
                    return (True, {
                        "success": True,
                        "file_id": file_id,
                        "columns": columns,
                        "total_columns": len(columns),
                        "method": f"duckdb_attempt_{attempt + 1}"
                    })
            
            except Exception:
                if attempt < self.max_retries - 1:
                    print(f"⚠️ Esperando {self.retry_delay}s antes del siguiente intento...")
                    time.sleep(self.retry_delay)
                    
                    # Reconectar DuckDB si falla
                    try:
                        self._safe_reconnect_duckdb()
                    except Exception:
                        pass
        
        return (False, None)


    def get_file_columns_for_cross(self, file_id: str, sheet_name: str, loaded_tables: Dict) -> Dict[str, Any]:
        """ULTRA-ROBUSTO: Previene crashes con validación exhaustiva"""
        try:
            # PASO 1: Validar archivo y parquet
            is_valid, result = self._validate_file_and_parquet(file_id, loaded_tables)
            if not is_valid:
                return result
            
            parquet_path = result
            
            # PASO 2: Intentar obtener columnas con retry
            success, result = self._try_get_columns_with_retry(parquet_path, file_id)
            if success:
                return result
            
            # PASO 3: Fallback con pandas
            return self._fallback_pandas_columns(parquet_path, file_id)
            
        except Exception as e:
            return self._handle_critical_error(file_id, str(e))


    def _get_columns_with_timeout_and_retry(self, parquet_path: str, timeout_seconds: int = 15) -> List[str]:
        """Obtiene columnas con timeout y manejo seguro de errores"""
        columns = []
        exception_container = [None]
        success_container = [False]
        
        def get_columns_safe():
            try:
                #  PROTECCIÓN: Normalizar ruta para Windows
                safe_path = parquet_path.replace('\\', '/')
                
                #  CONSULTA SEGURA CON VALIDACIÓN PREVIA
                describe_sql = f"DESCRIBE SELECT * FROM read_parquet('{safe_path}')"
                
                # Verificar conexión antes de ejecutar
                if not self._is_connection_healthy():
                    raise ValueError("Conexión DuckDB no saludable")
                
                result = self.conn.execute(describe_sql).fetchall()
                
                if result:
                    columns.extend([str(row[0]) for row in result if row[0] is not None])
                    success_container[0] = True
                else:
                    raise ValueError("Consulta DESCRIBE no retornó resultados")
                    
            except Exception as e:
                exception_container[0] = e
        
        #  EJECUTAR CON TIMEOUT USANDO THREADING
        thread = threading.Thread(target=get_columns_safe)
        thread.daemon = True
        thread.start()
        thread.join(timeout_seconds)
        
        if thread.is_alive():
            # El thread sigue ejecutándose pero lo ignoramos
            return []
        
        if exception_container[0]:
            raise exception_container[0]
        
        if not success_container[0]:
            raise ValueError("Consulta no completada exitosamente")
        
        return columns

    def _is_connection_healthy(self) -> bool:
        """Verifica si la conexión DuckDB está saludable"""
        try:
            # Test simple y rápido
            result = self.conn.execute("SELECT 1 as test").fetchone()
            return result and result[0] == 1
        except Exception:
            return False

    def _safe_reconnect_duckdb(self):
        """Intenta reconectar DuckDB de forma segura"""
        try:            
            # Cerrar conexión actual si existe
            if hasattr(self, 'conn') and self.conn:
                try:
                    self.conn.close()
                except Exception:
                    pass
            
            # Crear nueva conexión
            import duckdb
            self.conn = duckdb.connect(":memory:")  # Usar memoria temporal
            
            # Configurar
            self.conn.execute("PRAGMA threads=2")
            self.conn.execute("PRAGMA memory_limit='4GB'")
            
        except Exception as e:
            raise ValueError(f"Fallo al reconectar DuckDB: {str(e)}")

    def _fallback_pandas_columns(self, parquet_path: str, file_id: str) -> Dict[str, Any]:
        """Método de fallback usando pandas"""
        try:            
            # Leer solo metadata del Parquet
            df = pd.read_parquet(parquet_path, engine='pyarrow')
            columns = [str(col) for col in df.columns]
            
            return {
                "success": True,
                "file_id": file_id,
                "columns": columns,
                "total_columns": len(columns),
                "method": "pandas_fallback"
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Tanto DuckDB como pandas fallaron. Error pandas: {str(e)}",
                "requires_regeneration": True
            }

    def _handle_critical_error(self, file_id: str, error_msg: str) -> Dict[str, Any]:
        """Maneja errores críticos sin causar crash"""
        
        return {
            "success": False,
            "file_id": file_id,
            "error": f"Error crítico manejado: {error_msg}",
            "columns": [],
            "total_columns": 0,
            "method": "critical_error_handled",
            "requires_fallback": True
        }

    def validate_parquet_file(self, parquet_path: str) -> Dict[str, Any]:
        """ VALIDACIÓN MEJORADA SIN CRASH"""
        try:
            if not os.path.exists(parquet_path):
                return {"valid": False, "error": "Archivo no existe"}
            
            # Verificar tamaño mínimo
            file_size = os.path.getsize(parquet_path)
            if file_size < 100:
                return {"valid": False, "error": f"Archivo muy pequeño: {file_size} bytes"}
            
            # Validación rápida con pandas (más segura)
            try:
                df = pd.read_parquet(parquet_path, engine='pyarrow')
                return {
                    "valid": True,
                    "method": "pandas_validation",
                    "total_rows": len(df),
                    "columns": list(df.columns)
                }
            except Exception as e:
                return {"valid": False, "error": f"Error validando con pandas: {str(e)}"}
                
        except Exception as e:
            return {"valid": False, "error": f"Error crítico en validación: {str(e)}"}
