# services/duckdb_service/connection/connection_manager.py
import os
from typing import Dict, Any
from services.aux_duckdb_services.initialize_connection import InitializeConnection

class ConnectionManager:
    """Maneja la conexión DuckDB y su estado"""
    
    def __init__(self, duckdb_dir: str, parquet_dir: str, metadata_dir: str):
        self.duckdb_dir = duckdb_dir
        self.parquet_dir = parquet_dir
        self.metadata_dir = metadata_dir
        self.db_path = os.path.join(duckdb_dir, "main.duckdb")
        self.conn = None
        self._initialize_connection()
    
    def _initialize_connection(self) -> bool:
        """Inicializa la conexión DuckDB"""
        try:
            self.conn = InitializeConnection().initialize_duckdb_connection(
                self.duckdb_dir, 
                self.parquet_dir, 
                self.metadata_dir, 
                self.db_path
            )
            return self.conn is not None
        except Exception as e:
            print(f"Error inicializando conexión DuckDB: {e}")
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
    
    def restart_connection(self) -> bool:
        """Reinicia la conexión DuckDB de forma segura"""
        try:
            print("Reiniciando conexión DuckDB...")
            
            if self.conn:
                try:
                    self.conn.close()
                except:
                    pass
            
            success = self._initialize_connection()
            
            if success:
                print("Conexión DuckDB reiniciada exitosamente")
            else:
                print("No se pudo reiniciar la conexión DuckDB")
            
            return success
                
        except Exception as e:
            print(f"Error reiniciando conexión: {e}")
            return False
    
    def close(self):
        """Cierra conexión DuckDB de forma segura"""
        try:
            if self.conn:
                self.conn.close()
                print("Conexión DuckDB cerrada")
        except Exception as e:
            print(f"Error cerrando DuckDB: {e}")
    
    def get_connection(self):
        """Obtiene la conexión actual"""
        return self.conn
    
    def update_controllers_connection(self, controllers: Dict[str, Any]):
        """Actualiza la referencia de conexión en los controladores"""
        if not self.conn:
            return
        
        connection_dependent_controllers = [
            'file_validation', 'file_conversion', 'query', 'cross_files'
        ]
        
        for controller_name in connection_dependent_controllers:
            controller = controllers.get(controller_name)
            if controller and hasattr(controller, 'conn'):
                controller.conn = self.conn
