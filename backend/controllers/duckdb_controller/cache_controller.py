import os
import json
import hashlib
from datetime import datetime
from typing import Dict, Any, Optional

class CacheController:
    """Controlador para manejo inteligente de cache"""
    
    def __init__(self, parquet_dir: str, metadata_dir: str):
        self.parquet_dir = parquet_dir
        self.metadata_dir = metadata_dir
        self.file_cache: Dict[str, Dict[str, Any]] = {}
        self._load_cache_metadata()

    def calculate_file_id(self, file_path: str) -> str:
        """Calcula hash SHA-256 del archivo para identificación única"""
        hash_sha256 = hashlib.sha256()
        
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_sha256.update(chunk)
            
            file_id = hash_sha256.hexdigest()[:16]
            return file_id
            
        except Exception as e:
            stat = os.stat(file_path)
            fallback_hash = hashlib.sha256(
                f"{stat.st_size}_{stat.st_mtime}_{os.path.basename(file_path)}".encode()
            ).hexdigest()[:16]
            
            return fallback_hash

    def get_cache_metadata_path(self, file_id: str) -> str:
        """Obtiene path del archivo de metadata del cache"""
        return os.path.join(self.metadata_dir, f"{file_id}_metadata.json")

    def get_cached_parquet_path(self, file_id: str) -> str:
        """Obtiene path del archivo Parquet cacheado"""
        return os.path.join(self.parquet_dir, f"{file_id}.parquet")

    def save_cache_metadata(self, file_id: str, metadata: Dict[str, Any]):
        """Guarda metadata del archivo cacheado"""
        metadata_path = self.get_cache_metadata_path(file_id)
        
        try:
            # Limpiar columns antes de guardar
            if "columns" in metadata and isinstance(metadata["columns"], list):
                metadata["columns"] = [str(col) if col is not None else f'col_{i}' 
                                     for i, col in enumerate(metadata["columns"])]            
            # Agregar información de cache
            cache_info = {
                **metadata,
                "cached_at": datetime.now().isoformat(),
                "last_accessed": datetime.now().isoformat(),
                "access_count": 1,
                "file_id": file_id
            }
            
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(cache_info, f, indent=2)
            
            # Actualizar cache en memoria
            self.file_cache[file_id] = cache_info
            
        except Exception as e:
            print(f"Error guardando metadata: {e}")

    def _load_cache_metadata(self):
        """Carga metadata de archivos cacheados al iniciar"""
        if not os.path.exists(self.metadata_dir):
            return
        
        cached_count = 0
        
        for metadata_file in os.listdir(self.metadata_dir):
            if not metadata_file.endswith('_metadata.json'):
                continue
                
            try:
                metadata_path = os.path.join(self.metadata_dir, metadata_file)
                
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                
                file_id = metadata.get('file_id')
                if file_id:
                    self.file_cache[file_id] = metadata
                    cached_count += 1
                    
            except Exception as e:
                print(f"Error cargando metadata {metadata_file}: {e}")
        
        if cached_count > 0:
            print(f"Cargados {cached_count} archivos en cache")

    def update_cache_access(self, file_id: str):
        """Actualiza estadísticas de acceso al cache"""
        if file_id in self.file_cache:
            self.file_cache[file_id]["last_accessed"] = datetime.now().isoformat()
            self.file_cache[file_id]["access_count"] = self.file_cache[file_id].get("access_count", 0) + 1
            
            # Guardar metadata actualizado
            metadata_path = self.get_cache_metadata_path(file_id)
            try:
                with open(metadata_path, 'w', encoding='utf-8') as f:
                    json.dump(self.file_cache[file_id], f, indent=2)
            except Exception as e:
                print(f"Error actualizando estadísticas de acceso: {e}")

    def is_file_cached(self, file_path: str) -> tuple[bool, Optional[str], Optional[Dict[str, Any]]]:
        """Verifica si el archivo ya está en cache con validación"""
        
        # Calcular hash del archivo actual
        file_id = self.calculate_file_id(file_path)
        
        # Verificar si existe en memoria
        if file_id not in self.file_cache:
            return False, file_id, None
        
        # Verificar si el archivo Parquet físicamente existe
        parquet_path = self.get_cached_parquet_path(file_id)
        
        if not os.path.exists(parquet_path):
            self._cleanup_inconsistent_cache(file_id)
            return False, file_id, None
        return True, file_id, self.file_cache[file_id]

    def _cleanup_inconsistent_cache(self, file_id: str):
        """Limpia cache inconsistente"""
        try:
            # Remover de memoria
            if file_id in self.file_cache:
                del self.file_cache[file_id]
            
            # Remover archivos físicos
            parquet_path = self.get_cached_parquet_path(file_id)
            metadata_path = self.get_cache_metadata_path(file_id)
            
            for path in [parquet_path, metadata_path]:
                if os.path.exists(path):
                    os.remove(path)
                    
        except Exception as e:
            print(f"Error limpiando cache inconsistente: {e}")

    def get_cache_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas del cache"""
        if not self.file_cache:
            return {
                "total_cached_files": 0,
                "total_cache_size_mb": 0,
                "cache_hit_potential": "N/A"
            }
        
        total_files = len(self.file_cache)
        total_size = 0
        total_accesses = 0
        files_with_multiple_access = 0
        
        for file_id, metadata in self.file_cache.items():
            parquet_path = self.get_cached_parquet_path(file_id)
            if os.path.exists(parquet_path):
                total_size += os.path.getsize(parquet_path)
            
            access_count = metadata.get("access_count", 0)
            total_accesses += access_count
            
            if access_count > 1:
                files_with_multiple_access += 1
        
        cache_efficiency = (files_with_multiple_access / total_files * 100) if total_files > 0 else 0
        
        return {
            "total_cached_files": total_files,
            "total_cache_size_mb": round(total_size / 1024 / 1024, 2),
            "files_with_multiple_access": files_with_multiple_access,
            "cache_efficiency_percent": round(cache_efficiency, 1),
            "total_accesses": total_accesses,
            "average_accesses_per_file": round(total_accesses / total_files, 1) if total_files > 0 else 0
        }

    def cleanup_old_cache(self, days_old: int = 30, min_access_count: int = 1):
        """Limpieza inteligente del cache"""
        import time
        cutoff_time = time.time() - (days_old * 24 * 60 * 60)
        cleaned_files = 0
        total_size_cleaned = 0
        
        files_to_remove = []
        
        for file_id, metadata in self.file_cache.items():
            cached_at = metadata.get("cached_at")
            access_count = metadata.get("access_count", 0)
            
            should_remove = False
            reason = ""
            
            try:
                if cached_at:
                    cached_timestamp = datetime.fromisoformat(cached_at).timestamp()
                    if cached_timestamp < cutoff_time and access_count <= min_access_count:
                        should_remove = True
                        reason = f"antiguo ({days_old}+ días) y poco usado ({access_count} accesos)"
                
                # Verificar si los archivos físicos existen
                parquet_path = self.get_cached_parquet_path(file_id)
                if not os.path.exists(parquet_path):
                    should_remove = True
                    reason = "archivo Parquet faltante"
                
                if should_remove:
                    files_to_remove.append((file_id, reason))
                    
            except Exception as e:
                files_to_remove.append((file_id, "error de evaluación"))
        
        # Remover archivos identificados
        for file_id, reason in files_to_remove:
            try:
                parquet_path = self.get_cached_parquet_path(file_id)
                metadata_path = self.get_cache_metadata_path(file_id)
                
                # Calcular tamaño antes de remover
                if os.path.exists(parquet_path):
                    total_size_cleaned += os.path.getsize(parquet_path)
                
                # Remover archivos físicos
                for path in [parquet_path, metadata_path]:
                    if os.path.exists(path):
                        os.remove(path)
                
                # Remover de memoria
                if file_id in self.file_cache:
                    original_name = self.file_cache[file_id].get("original_name", file_id[:8])
                    del self.file_cache[file_id]
                    cleaned_files += 1
                
            except Exception as e:
                print(f"Error removiendo {file_id}: {e}")
                
        return {
            "cleaned_files": cleaned_files,
            "size_cleaned_mb": round(total_size_cleaned/1024/1024, 1),
            "remaining_files": len(self.file_cache)
        }
