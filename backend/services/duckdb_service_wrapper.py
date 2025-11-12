
import traceback
from typing import Dict, Any
from services.duckdb_service.duckdb_service import DuckDBService

class SafeDuckDBService:
    """Wrapper seguro para DuckDB Service que previene crashes"""
    
    def __init__(self):
        self._service = None
        self._initialize_service()
    
    def _initialize_service(self):
        """Inicializa el servicio con manejo de errores"""
        try:
            self._service = DuckDBService()
            print("DuckDB Service inicializado de forma segura")
        except Exception as e:
            print(f"Error inicializando DuckDB Service: {e}")
            self._service = None
    
    def get_file_columns_for_cross(self, file_id: str, sheet_name: str = None) -> Dict[str, Any]:
        """Obtiene columnas de forma segura"""
        try:
            if not self._service:
                return {
                    "success": False,
                    "error": "DuckDB Service no inicializado",
                    "requires_restart": True
                }
            
            # Ejecutar con timeout
            return self._execute_with_timeout(
                self._service.get_file_columns_for_cross,
                args=(file_id, sheet_name),
                timeout_seconds=30
            )
            
        except Exception as e:
            print(f"Error en get_file_columns_for_cross: {e}")
            traceback.print_exc()
            
            # Intentar reiniciar servicio
            self._safe_restart()
            
            return {
                "success": False,
                "error": f"Error obteniendo columnas: {str(e)}",
                "requires_restart": True
            }
    
    def _execute_with_timeout(self, func, args=(), kwargs=None, timeout_seconds=30):
        """Ejecuta función con timeout para prevenir hangs"""
        if kwargs is None:
            kwargs = {}
        
        import threading
        result_container = [None]
        exception_container = [None]
        
        def target():
            try:
                result_container[0] = func(*args, **kwargs)
            except Exception as e:
                exception_container[0] = e
        
        thread = threading.Thread(target=target)
        thread.daemon = True
        thread.start()
        thread.join(timeout_seconds)
        
        if thread.is_alive():
            print(f"⏱️ TIMEOUT: Operación tomó más de {timeout_seconds}s")
            return {
                "success": False,
                "error": f"Timeout después de {timeout_seconds}s",
                "requires_restart": True
            }
        
        if exception_container[0]:
            raise exception_container[0]
        
        return result_container[0]
    
    def _safe_restart(self):
        """Reinicia el servicio de forma segura"""
        try:
            print("🔄 Reiniciando DuckDB Service...")
            
            # Cerrar servicio actual
            if self._service:
                try:
                    self._service.close()
                except Exception:
                    pass
                self._service = None
            
            # Recrear servicio
            self._initialize_service()
            
        except Exception as e:
            print(f"Error reiniciando servicio: {e}")
            self._service = None
    
    def __getattr__(self, name):
        """Delegación segura de métodos"""
        if not self._service:
            self._initialize_service()
        
        if not self._service:
            raise ValueError("DuckDB Service no disponible")
        
        attr = getattr(self._service, name)
        
        if callable(attr):
            def safe_method(*args, **kwargs):
                try:
                    return self._execute_with_timeout(attr, args, kwargs)
                except Exception as e:
                    print(f"Error en método {name}: {e}")
                    self._safe_restart()
                    raise e
            return safe_method
        
        return attr

# Instancia segura
safe_duckdb_service = SafeDuckDBService()
