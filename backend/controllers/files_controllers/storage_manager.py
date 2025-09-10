# controllers/files_controllers/storage_manager.py - ✅ MODIFICADO PARA TECHNICAL_NOTE
import os
import json
import glob
from typing import Dict, Any, List, Optional
import pandas as pd
import shutil


class FileStorageManager:
    def __init__(self):
        self.data_cache: Dict[str, pd.DataFrame] = {}
        
        # ✅ CAMBIAR DIRECTORIO A TECHNICAL_NOTE
        self.upload_dir = os.path.abspath("technical_note")
        self.storage_file = os.path.join('uploads', "files_info.json")
                
        # CARGAR Y SINCRONIZAR AL INICIALIZAR
        self._load_and_sync_storage()
    
    def _get_existing_files(self):
        """Obtiene todos los archivos físicos realmente presentes con nombres originales"""
        if not os.path.exists(self.upload_dir):
            return set()
        
        # Buscar archivos con extensiones válidas
        pattern = os.path.join(self.upload_dir, "*")
        files = glob.glob(pattern)
        
        # ✅ EXTRAER NOMBRES ORIGINALES DE ARCHIVOS (NO UUIDs)
        valid_extensions = {'.csv', '.xlsx', '.xls'}
        existing_files = set()
        
        for file_path in files:
            filename = os.path.basename(file_path)
            if filename == 'files_info.json':
                continue
            
            _, ext = os.path.splitext(filename)
            if ext.lower() in valid_extensions:
                # ✅ USAR EL FILENAME COMPLETO COMO ID (nombre original)
                existing_files.add(filename)
        
        return existing_files
    
    def _load_and_sync_storage(self):
        """Carga y sincroniza el storage con archivos físicos usando nombres originales"""
        try:
            # OBTENER ARCHIVOS REALES
            existing_files = self._get_existing_files()
            
            # CARGAR JSON SI EXISTE
            if os.path.exists(self.storage_file):
                with open(self.storage_file, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                
                # ✅ SINCRONIZAR: Solo mantener archivos que existen físicamente
                synced_storage = {}
                for file_id, file_info in json_data.items():
                    original_name = file_info.get("original_name", file_id)
                    
                    # ✅ VERIFICAR POR NOMBRE ORIGINAL, NO POR UUID
                    if original_name in existing_files:
                        file_path = os.path.join(self.upload_dir, original_name)
                        if os.path.exists(file_path):
                            # ✅ ACTUALIZAR PATH CON EL CORRECTO
                            file_info["path"] = file_path
                            synced_storage[file_id] = file_info
                        else:
                            print(f"⚠️ JSON tiene entrada pero archivo no existe: {original_name}")
                    else:
                        print(f"🗑️ Eliminando del JSON archivo inexistente: {original_name}")
                
                self.storage = synced_storage
                
                # GUARDAR JSON SINCRONIZADO
                if len(synced_storage) != len(json_data):
                    self._save_storage()
                
            else:                
                self.storage = {}
                
        except Exception as e:
            print(f"❌ Error cargando storage: {e}")
            self.storage = {}
    
    def _save_storage(self):
        """Guarda el storage en archivo JSON"""
        try:
            self.ensure_upload_directory()
            with open(self.storage_file, 'w', encoding='utf-8') as f:
                json.dump(self.storage, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"❌ Error guardando storage: {e}")
    
    def ensure_upload_directory(self) -> str:
        """Asegura que el directorio technical_note exista"""
        if not os.path.exists(self.upload_dir):
            os.makedirs(self.upload_dir, exist_ok=True)
            print(f"✅ Directorio creado: {self.upload_dir}")
        return self.upload_dir
    
    def store_file_with_original_name(
        self, 
        source_file_path: str, 
        original_filename: str, 
        file_info: Dict[str, Any], 
        overwrite: bool = True
    ) -> str:
        """
        Almacena archivo con nombre original en technical_note
        
        Args:
            source_file_path: Ruta del archivo temporal a copiar
            original_filename: Nombre original del archivo
            file_info: Información adicional del archivo
            overwrite: Si True, sobrescribe archivos existentes
            
        Returns:
            str: Path final donde se guardó el archivo
        """
        try:
            # ✅ ASEGURAR QUE EL DIRECTORIO EXISTE
            self.ensure_upload_directory()
            
            # ✅ CONSTRUIR RUTA DESTINO CON NOMBRE ORIGINAL
            destination_path = os.path.join(self.upload_dir, original_filename)
            
            # ✅ MANEJAR ARCHIVOS EXISTENTES
            if os.path.exists(destination_path):
                if overwrite:
                    print(f"⚠️ Sobrescribiendo archivo existente: {original_filename}")
                    os.remove(destination_path)
                else:
                    # ✅ GENERAR NOMBRE ÚNICO SI NO SE QUIERE SOBRESCRIBIR
                    base_name, ext = os.path.splitext(original_filename)
                    counter = 1
                    while os.path.exists(destination_path):
                        new_filename = f"{base_name}_{counter}{ext}"
                        destination_path = os.path.join(self.upload_dir, new_filename)
                        counter += 1
                    original_filename = os.path.basename(destination_path)
                    print(f"📝 Renombrado a: {original_filename}")
            
            # ✅ COPIAR ARCHIVO AL DESTINO
            if os.path.exists(source_file_path):
                shutil.copy2(source_file_path, destination_path)
                print(f"✅ Archivo copiado: {original_filename}")
            else:
                raise FileNotFoundError(f"Archivo fuente no encontrado: {source_file_path}")
            
            # ✅ ACTUALIZAR INFORMACIÓN DEL ARCHIVO
            file_size = os.path.getsize(destination_path)
            file_info.update({
                "path": destination_path,
                "original_name": original_filename,
                "file_size": file_size,
                "stored_at": pd.Timestamp.now().isoformat()
            })
            
            # ✅ USAR NOMBRE ORIGINAL COMO ID
            file_id = original_filename
            self.storage[file_id] = file_info
            self._save_storage()
            
            return destination_path
            
        except Exception as e:
            print(f"❌ Error almacenando archivo {original_filename}: {e}")
            raise
    
    def store_file_info(self, file_id: str, file_info: Dict[str, Any]):
        """Almacena información del archivo de forma persistente (COMPATIBILIDAD)"""
        file_path = file_info.get("path", "")
        if file_path and os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            file_info["file_size"] = file_size
            file_info["stored_at"] = pd.Timestamp.now().isoformat()
        
        self.storage[file_id] = file_info
        self._save_storage()
    
    def get_file_info(self, file_id: str) -> Optional[Dict[str, Any]]:
        """Obtiene información del archivo con auto-sincronización"""
        
        # RECARGAR Y SINCRONIZAR SI STORAGE ESTÁ VACÍO
        if not self.storage:
            self._load_and_sync_storage()
        
        if file_id not in self.storage:
            self._load_and_sync_storage()
            
            if file_id not in self.storage:
                return None
            
        info = self.storage[file_id]
        file_path = info.get("path", "")
        
        # VERIFICACIÓN FINAL DE EXISTENCIA FÍSICA
        if file_path and os.path.exists(file_path):
            return info
        else:
            # ✅ LIMPIAR ENTRADA INVÁLIDA
            del self.storage[file_id]
            self._save_storage()
            return None
    
    def get_file_info_by_original_name(self, original_filename: str) -> Optional[Dict[str, Any]]:
        """Obtiene información del archivo por su nombre original"""
        # ✅ BUSCAR POR NOMBRE ORIGINAL
        for file_id, file_info in self.storage.items():
            if file_info.get("original_name") == original_filename:
                return file_info
        
        # ✅ FALLBACK: Buscar por file_id si coincide con filename
        return self.get_file_info(original_filename)
    
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
        # CARGAR STORAGE SI ESTÁ VACÍO
        if not self.storage:
            self._load_and_sync_storage()
            
        if file_id not in self.storage:
            return False
        
        file_info = self.storage[file_id]
        file_path = file_info.get("path", "")
        
        # ELIMINAR ARCHIVO FÍSICO
        try:
            if file_path and os.path.exists(file_path):
                os.remove(file_path)
                print(f"🗑️ Archivo físico eliminado: {file_path}")
        except Exception as e:
            print(f"⚠️ Error eliminando archivo físico: {e}")
        
        # LIMPIAR CACHE EN MEMORIA
        cache_keys_to_remove = [key for key in self.data_cache.keys() 
                               if key.startswith(file_id)]
        for key in cache_keys_to_remove:
            del self.data_cache[key]
        
        # ELIMINAR DEL STORAGE Y GUARDAR JSON
        del self.storage[file_id]
        self._save_storage()        
        return True
    
    def remove_file_by_original_name(self, original_filename: str) -> bool:
        """Remueve archivo por su nombre original"""
        # ✅ BUSCAR FILE_ID POR NOMBRE ORIGINAL
        file_id_to_remove = None
        for file_id, file_info in self.storage.items():
            if file_info.get("original_name") == original_filename:
                file_id_to_remove = file_id
                break
        
        if file_id_to_remove:
            return self.remove_file(file_id_to_remove)
        
        # ✅ FALLBACK: Intentar por file_id directo
        return self.remove_file(original_filename)
    
    def get_all_files(self) -> Dict[str, Dict[str, Any]]:
        """Obtiene todos los archivos almacenados con sincronización"""
        if not self.storage:
            self._load_and_sync_storage()
        return self.storage.copy()
    
    def list_technical_files(self) -> List[Dict[str, Any]]:
        """Lista todos los archivos en technical_note con información completa"""
        files_info = []
        all_files = self.get_all_files()
        
        for file_id, file_info in all_files.items():
            original_name = file_info.get("original_name", file_id)
            file_path = file_info.get("path", "")
            
            if os.path.exists(file_path):
                files_info.append({
                    "file_id": file_id,
                    "filename": original_name,
                    "display_name": original_name.replace('.csv', '').replace('_', ' ').title(),
                    "file_path": file_path,
                    "file_size": file_info.get("file_size", 0),
                    "stored_at": file_info.get("stored_at"),
                    "extension": os.path.splitext(original_name)[1].lower().replace('.', ''),
                    "is_custom_upload": True
                })
        
        return files_info
    
    def generate_cache_key(self, file_id: str, sheet_name: str = None) -> str:
        """Genera clave para cache"""
        return f"{file_id}_{sheet_name}" if sheet_name else file_id
    
    def cleanup_storage(self):
        """Método manual para limpiar storage si es necesario"""
        self._load_and_sync_storage()
        return len(self.storage)
    
    def get_technical_note_path(self) -> str:
        """Obtiene la ruta del directorio technical_note"""
        return self.upload_dir
    
    def file_exists(self, filename: str) -> bool:
        """Verifica si un archivo existe en technical_note"""
        file_path = os.path.join(self.upload_dir, filename)
        return os.path.exists(file_path)
    
    def get_file_path(self, filename: str) -> str:
        """Obtiene la ruta completa de un archivo en technical_note"""
        return os.path.join(self.upload_dir, filename)


# ✅ INSTANCIA GLOBAL
storage_manager = FileStorageManager()
