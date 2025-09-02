# controllers/files_controllers/storage_manager.py
import os
import json
import glob
from typing import Dict, Any, Optional
import pandas as pd

class FileStorageManager:
    def __init__(self):
        self.data_cache: Dict[str, pd.DataFrame] = {}
        
        # ✅ USAR RUTA ABSOLUTA
        self.upload_dir = os.path.abspath("uploads")
        self.storage_file = os.path.join(self.upload_dir, "files_info.json")
        
        print(f"📁 Directorio uploads: {self.upload_dir}")
        print(f"📄 Archivo storage: {self.storage_file}")
        
        # ✅ CARGAR Y SINCRONIZAR AL INICIALIZAR
        self._load_and_sync_storage()
    
    def _get_existing_files(self):
        """Obtiene todos los archivos físicos realmente presentes"""
        if not os.path.exists(self.upload_dir):
            return set()
        
        # Buscar archivos con extensiones válidas
        pattern = os.path.join(self.upload_dir, "*")
        files = glob.glob(pattern)
        
        # Extraer solo los IDs de archivos válidos
        valid_extensions = {'.csv', '.xlsx', '.xls'}
        existing_ids = set()
        
        for file_path in files:
            filename = os.path.basename(file_path)
            if filename == 'files_info.json':
                continue
            
            name, ext = os.path.splitext(filename)
            if ext.lower() in valid_extensions:
                existing_ids.add(name)
        
        return existing_ids
    
    def _load_and_sync_storage(self):
        """Carga y sincroniza el storage con archivos físicos"""
        try:
            # ✅ OBTENER ARCHIVOS REALES
            existing_file_ids = self._get_existing_files()
            print(f"📂 Archivos físicos encontrados: {len(existing_file_ids)} - {list(existing_file_ids)}")
            
            # ✅ CARGAR JSON SI EXISTE
            if os.path.exists(self.storage_file):
                with open(self.storage_file, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                print(f"📥 JSON cargado: {len(json_data)} entradas")
                
                # ✅ SINCRONIZAR: Solo mantener archivos que existen físicamente
                synced_storage = {}
                for file_id, file_info in json_data.items():
                    if file_id in existing_file_ids:
                        # Verificar doble que el archivo físico existe
                        file_path = file_info.get("path", "")
                        if file_path and os.path.exists(file_path):
                            synced_storage[file_id] = file_info
                        else:
                            print(f"⚠️ JSON tiene entrada pero archivo no existe: {file_info.get('original_name', file_id)}")
                    else:
                        print(f"🗑️ Eliminando del JSON archivo inexistente: {file_info.get('original_name', file_id)}")
                
                self.storage = synced_storage
                
                # ✅ GUARDAR JSON SINCRONIZADO
                if len(synced_storage) != len(json_data):
                    print(f"🔄 Sincronizando JSON: {len(json_data)} → {len(synced_storage)} entradas")
                    self._save_storage()
                
            else:
                print("📝 JSON no existe, inicializando storage vacío")
                self.storage = {}
            
            print(f"✅ Storage sincronizado: {len(self.storage)} archivos válidos")
            if self.storage:
                print(f"📋 IDs disponibles: {list(self.storage.keys())}")
                
        except Exception as e:
            print(f"❌ Error sincronizando storage: {e}")
            self.storage = {}
    
    def _save_storage(self):
        """Guarda el storage en archivo JSON"""
        try:
            self.ensure_upload_directory()
            with open(self.storage_file, 'w', encoding='utf-8') as f:
                json.dump(self.storage, f, indent=2, ensure_ascii=False)
            print(f"💾 Storage guardado: {len(self.storage)} archivos")
        except Exception as e:
            print(f"❌ Error guardando storage: {e}")
    
    def ensure_upload_directory(self) -> str:
        """Asegura que el directorio de uploads exista"""
        if not os.path.exists(self.upload_dir):
            os.makedirs(self.upload_dir, exist_ok=True)
            print(f"📁 Directorio creado: {self.upload_dir}")
        return self.upload_dir
    
    def store_file_info(self, file_id: str, file_info: Dict[str, Any]):
        """Almacena información del archivo de forma persistente"""
        file_path = file_info.get("path", "")
        if file_path and os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            file_info["file_size"] = file_size
            file_info["stored_at"] = pd.Timestamp.now().isoformat()
            print(f"✅ Archivo almacenado: {file_info.get('original_name')} ({file_size:,} bytes)")
        
        self.storage[file_id] = file_info
        self._save_storage()
        print(f"📊 Storage actualizado: {len(self.storage)} archivos totales")
    
    def get_file_info(self, file_id: str) -> Optional[Dict[str, Any]]:
        """Obtiene información del archivo con auto-sincronización"""
        
        # ✅ RECARGAR Y SINCRONIZAR SI STORAGE ESTÁ VACÍO
        if not self.storage:
            print(f"⚠️ Storage vacío, recargando y sincronizando...")
            self._load_and_sync_storage()
        
        print(f"🔍 Buscando archivo: {file_id}")
        print(f"📊 Storage actual: {len(self.storage)} archivos")
        
        if file_id not in self.storage:
            print(f"❌ Archivo no encontrado: {file_id}")
            print(f"📋 IDs disponibles: {list(self.storage.keys())}")
            
            # ✅ ÚLTIMA OPORTUNIDAD: Resincronizar por si acaso
            print(f"🔄 Resincronizando storage por archivo faltante...")
            self._load_and_sync_storage()
            
            if file_id not in self.storage:
                print(f"❌ Archivo definitivamente no encontrado después de resincronizar")
                return None
            
        info = self.storage[file_id]
        file_path = info.get("path", "")
        
        print(f"📄 Archivo encontrado: {info.get('original_name', 'N/A')}")
        
        # ✅ VERIFICACIÓN FINAL DE EXISTENCIA FÍSICA
        if file_path and os.path.exists(file_path):
            print(f"✅ Archivo físico confirmado")
            return info
        else:
            print(f"❌ Archivo físico no encontrado, eliminando del storage: {file_path}")
            del self.storage[file_id]
            self._save_storage()
            return None
    
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
        """Remueve archivo del almacenamiento, cache Y archivo físico"""
        # ✅ CARGAR STORAGE SI ESTÁ VACÍO
        if not self.storage:
            self._load_and_sync_storage()
            
        if file_id not in self.storage:
            print(f"⚠️ Archivo no encontrado en storage para eliminar: {file_id}")
            return False
        
        file_info = self.storage[file_id]
        file_path = file_info.get("path", "")
        
        # ✅ ELIMINAR ARCHIVO FÍSICO
        try:
            if file_path and os.path.exists(file_path):
                os.remove(file_path)
                print(f"🗑️ Archivo físico eliminado: {file_path}")
        except Exception as e:
            print(f"⚠️ Error eliminando archivo físico: {e}")
        
        # ✅ LIMPIAR CACHE EN MEMORIA
        cache_keys_to_remove = [key for key in self.data_cache.keys() 
                               if key.startswith(file_id)]
        for key in cache_keys_to_remove:
            del self.data_cache[key]
        
        # ✅ ELIMINAR DEL STORAGE Y GUARDAR JSON
        del self.storage[file_id]
        self._save_storage()
        
        print(f"🗑️ Archivo eliminado completamente: {file_info.get('original_name', file_id)}")
        print(f"📊 Storage restante: {len(self.storage)} archivos")
        
        return True
    
    def get_all_files(self) -> Dict[str, Dict[str, Any]]:
        """Obtiene todos los archivos almacenados con sincronización"""
        if not self.storage:
            self._load_and_sync_storage()
        
        print(f"📊 Devolviendo {len(self.storage)} archivos sincronizados")
        return self.storage.copy()
    
    def generate_cache_key(self, file_id: str, sheet_name: str = None) -> str:
        """Genera clave para cache"""
        return f"{file_id}_{sheet_name}" if sheet_name else file_id
    
    def cleanup_storage(self):
        """Método manual para limpiar storage si es necesario"""
        print(f"🧹 Ejecutando limpieza manual del storage...")
        self._load_and_sync_storage()
        return len(self.storage)
storage_manager = FileStorageManager()