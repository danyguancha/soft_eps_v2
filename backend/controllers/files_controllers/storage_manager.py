
import os
from typing import Dict, Any, Optional
import pandas as pd

class FileStorageManager:
    def __init__(self):
        self.storage: Dict[str, Dict[str, Any]] = {}
        self.data_cache: Dict[str, pd.DataFrame] = {}
        self.upload_dir = "uploads"
    
    def ensure_upload_directory(self) -> str:
        """Asegura que el directorio de uploads exista"""
        if not os.path.exists(self.upload_dir):
            os.makedirs(self.upload_dir)
        return self.upload_dir
    
    def store_file_info(self, file_id: str, file_info: Dict[str, Any]):
        """Almacena información del archivo"""
        self.storage[file_id] = file_info
    
    def get_file_info(self, file_id: str) -> Optional[Dict[str, Any]]:
        """Obtiene información del archivo"""
        return self.storage.get(file_id)
    
    def cache_dataframe(self, cache_key: str, df: pd.DataFrame):
        """Cachea un DataFrame"""
        self.data_cache[cache_key] = df.copy()
    
    def get_cached_dataframe(self, cache_key: str) -> Optional[pd.DataFrame]:
        """Obtiene DataFrame del cache"""
        return self.data_cache.get(cache_key)
    
    def update_cached_dataframe(self, cache_key: str, df: pd.DataFrame):
        """Actualiza DataFrame en cache"""
        self.data_cache[cache_key] = df
    
    def remove_file(self, file_id: str) -> bool:
        """Remueve archivo del almacenamiento y cache"""
        if file_id not in self.storage:
            return False
        
        file_info = self.storage[file_id]
        
        # Eliminar archivo físico
        try:
            os.remove(file_info["path"])
        except:
            pass
        
        # Limpiar cache
        cache_keys_to_remove = [key for key in self.data_cache.keys() 
                               if key.startswith(file_id)]
        for key in cache_keys_to_remove:
            del self.data_cache[key]
        
        # Eliminar de storage
        del self.storage[file_id]
        return True
    
    def get_all_files(self) -> Dict[str, Dict[str, Any]]:
        """Obtiene todos los archivos almacenados"""
        return self.storage.copy()
    
    def generate_cache_key(self, file_id: str, sheet_name: str = None) -> str:
        """Genera clave para cache"""
        return f"{file_id}_{sheet_name}"
