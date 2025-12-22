# services/cache_cleanup_service.py
"""
Servicio de limpieza automática de cache basado en tiempo de inactividad (TTL)
"""
import os
import json
import time
import threading
from datetime import datetime
from typing import Dict, Any, Optional
from config.config import cache_config


class CacheAccessTracker:
    """Rastrea el último acceso a cada archivo en el sistema"""
    
    def __init__(self, tracking_file: str = None):
        self.tracking_file = tracking_file or cache_config.ACCESS_TRACKING_FILE
        self.access_data: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.Lock()
        self._load_tracking_data()
    
    def _load_tracking_data(self):
        """Carga datos de acceso desde archivo JSON"""
        try:
            if os.path.exists(self.tracking_file):
                with open(self.tracking_file, 'r', encoding='utf-8') as f:
                    self.access_data = json.load(f)
                print(f"✓ Tracking data cargado: {len(self.access_data)} archivos rastreados")
            else:
                self.access_data = {}
                print(f"⚠️ No existe tracking file, iniciando nuevo registro")
        except Exception as e:
            print(f"✗ Error cargando tracking data: {e}")
            self.access_data = {}
    
    def _save_tracking_data(self):
        """Guarda datos de acceso a archivo JSON"""
        try:
            with self.lock:
                with open(self.tracking_file, 'w', encoding='utf-8') as f:
                    json.dump(self.access_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"✗ Error guardando tracking data: {e}")
    
    def track_access(self, file_path: str, directory: str = ""):
        """Registra un acceso a un archivo"""
        try:
            with self.lock:
                file_key = self._get_file_key(file_path, directory)
                
                self.access_data[file_key] = {
                    "file_path": file_path,
                    "directory": directory,
                    "last_access": datetime.now().isoformat(),
                    "last_access_timestamp": time.time(),
                    "access_count": self.access_data.get(file_key, {}).get("access_count", 0) + 1
                }
            
            # Guardar periódicamente (cada 10 accesos)
            if sum(1 for _ in self.access_data) % 10 == 0:
                self._save_tracking_data()
                
        except Exception as e:
            print(f"✗ Error tracking access: {e}")
    
    def get_last_access_time(self, file_path: str, directory: str = "") -> Optional[float]:
        """Obtiene el timestamp del último acceso a un archivo"""
        try:
            file_key = self._get_file_key(file_path, directory)
            access_info = self.access_data.get(file_key)
            
            if access_info:
                return access_info.get("last_access_timestamp")
            
            # Si no hay registro, usar tiempo de modificación del archivo
            if os.path.exists(file_path):
                return os.path.getmtime(file_path)
            
            return None
            
        except Exception as e:
            print(f"✗ Error obteniendo último acceso: {e}")
            return None
    
    def get_file_age_minutes(self, file_path: str, directory: str = "") -> Optional[float]:
        """Calcula cuántos minutos han pasado desde el último acceso"""
        last_access = self.get_last_access_time(file_path, directory)
        
        if last_access:
            age_seconds = time.time() - last_access
            return age_seconds / 60.0
        
        return None
    
    def remove_tracking(self, file_path: str, directory: str = ""):
        """Elimina el tracking de un archivo (cuando se borra)"""
        try:
            with self.lock:
                file_key = self._get_file_key(file_path, directory)
                if file_key in self.access_data:
                    del self.access_data[file_key]
                    self._save_tracking_data()
        except Exception as e:
            print(f"✗ Error removiendo tracking: {e}")
    
    def cleanup_orphaned_entries(self):
        """Elimina entradas de archivos que ya no existen"""
        try:
            with self.lock:
                orphaned = []
                for file_key, info in self.access_data.items():
                    file_path = info.get("file_path")
                    if file_path and not os.path.exists(file_path):
                        orphaned.append(file_key)
                
                for key in orphaned:
                    del self.access_data[key]
                
                if orphaned:
                    print(f"✓ Eliminadas {len(orphaned)} entradas huérfanas del tracking")
                    self._save_tracking_data()
        except Exception as e:
            print(f"✗ Error limpiando entradas huérfanas: {e}")
    
    def _get_file_key(self, file_path: str, directory: str) -> str:
        """Genera clave única para un archivo"""
        return f"{directory}::{file_path}"
    
    def get_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas del tracking"""
        total_files = len(self.access_data)
        
        if total_files == 0:
            return {
                "total_tracked_files": 0,
                "oldest_access": None,
                "newest_access": None
            }
        
        timestamps = [
            info.get("last_access_timestamp", 0)
            for info in self.access_data.values()
        ]
        
        return {
            "total_tracked_files": total_files,
            "oldest_access": datetime.fromtimestamp(min(timestamps)).isoformat() if timestamps else None,
            "newest_access": datetime.fromtimestamp(max(timestamps)).isoformat() if timestamps else None,
            "tracking_file": self.tracking_file
        }


class AutomaticCacheCleanupService:
    """Servicio de limpieza automática de cache basado en TTL"""
    
    def __init__(self):
        self.tracker = CacheAccessTracker()
        self.is_running = False
        self.cleanup_thread: Optional[threading.Thread] = None
        self.lock = threading.Lock()
        
        print("✓ AutomaticCacheCleanupService inicializado")
    
    def start_automatic_cleanup(self):
        """Inicia el proceso de limpieza automática en background"""
        if self.is_running:
            print("⚠️ Limpieza automática ya está corriendo")
            return
        
        self.is_running = True
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()
        
        print(f"✅ Limpieza automática INICIADA")
        print(f"   - Intervalo: cada {cache_config.CLEANUP_INTERVAL_MINUTES} minutos")
        print(f"   - TTLs configurados:")
        for dir_name, config in cache_config.DIRECTORIES.items():
            print(f"      • {dir_name}: {config['ttl_minutes']} minutos")
    
    def stop_automatic_cleanup(self):
        """Detiene el proceso de limpieza automática"""
        self.is_running = False
        if self.cleanup_thread:
            self.cleanup_thread.join(timeout=5)
        print("✓ Limpieza automática DETENIDA")
    
    def _cleanup_loop(self):
        """Loop principal de limpieza automática"""
        interval_seconds = cache_config.CLEANUP_INTERVAL_MINUTES * 60
        
        while self.is_running:
            try:
                print(f"\n{'='*60}")
                print(f"🧹 LIMPIEZA AUTOMÁTICA - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"{'='*60}")
                
                cleanup_result = self.cleanup_expired_cache()
                
                if cache_config.VERBOSE_CLEANUP_LOGGING:
                    self._log_cleanup_result(cleanup_result)
                
                # Limpiar entradas huérfanas del tracking
                self.tracker.cleanup_orphaned_entries()
                
                print(f"{'='*60}")
                print(f"✓ Limpieza completada. Próxima ejecución en {cache_config.CLEANUP_INTERVAL_MINUTES} minutos")
                print(f"{'='*60}\n")
                
            except Exception as e:
                print(f"✗ Error en cleanup loop: {e}")
                import traceback
                traceback.print_exc()
            
            # Esperar hasta el próximo ciclo
            time.sleep(interval_seconds)
    
    def cleanup_expired_cache(self) -> Dict[str, Any]:
        """
        Ejecuta limpieza de archivos expirados en todos los directorios configurados
        
        Returns:
            Dict con resultados de la limpieza
        """
        result = {
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "directories_processed": [],
            "total_files_deleted": 0,
            "total_files_preserved": 0,
            "total_space_freed_mb": 0.0,
            "errors": []
        }
        
        for dir_name, dir_config in cache_config.DIRECTORIES.items():
            dir_path = dir_config["path"]
            ttl_minutes = dir_config["ttl_minutes"]
            
            if not os.path.exists(dir_path):
                continue
            
            dir_result = self._cleanup_directory(
                directory=dir_path,
                directory_name=dir_name,
                ttl_minutes=ttl_minutes
            )
            
            result["directories_processed"].append(dir_result)
            result["total_files_deleted"] += dir_result["files_deleted"]
            result["total_files_preserved"] += dir_result["files_preserved"]
            result["total_space_freed_mb"] += dir_result["space_freed_mb"]
            
            if dir_result["errors"]:
                result["errors"].extend(dir_result["errors"])
        
        if result["errors"]:
            result["success"] = False
        
        return result
    
    def _cleanup_directory(
        self,
        directory: str,
        directory_name: str,
        ttl_minutes: int
    ) -> Dict[str, Any]:
        """
        Limpia un directorio específico eliminando archivos expirados
        
        Args:
            directory: Ruta del directorio
            directory_name: Nombre lógico del directorio
            ttl_minutes: TTL en minutos para este directorio
            
        Returns:
            Dict con resultados de la limpieza
        """
        result = {
            "directory": directory,
            "directory_name": directory_name,
            "ttl_minutes": ttl_minutes,
            "files_deleted": 0,
            "files_preserved": 0,
            "files_protected": 0,
            "space_freed_mb": 0.0,
            "deleted_files_list": [],
            "errors": []
        }
        
        try:
            if not os.path.exists(directory):
                return result
            
            protected_files = cache_config.PROTECTED_FILES.get(directory_name, [])
            
            for item_name in os.listdir(directory):
                item_path = os.path.join(directory, item_name)
                
                try:
                    # Saltar subdirectorios
                    if os.path.isdir(item_path):
                        continue
                    
                    # Verificar si está protegido
                    if item_name in protected_files:
                        result["files_protected"] += 1
                        continue
                    
                    # Calcular edad del archivo
                    file_age_minutes = self.tracker.get_file_age_minutes(item_path, directory_name)
                    
                    if file_age_minutes is None:
                        # Si no hay tracking, usar tiempo de modificación
                        if os.path.exists(item_path):
                            mtime = os.path.getmtime(item_path)
                            file_age_minutes = (time.time() - mtime) / 60.0
                        else:
                            continue
                    
                    # Verificar si ha expirado
                    if file_age_minutes >= ttl_minutes:
                        # Obtener tamaño antes de eliminar
                        file_size = os.path.getsize(item_path)
                        
                        # Eliminar archivo
                        os.remove(item_path)
                        
                        # Actualizar estadísticas
                        result["files_deleted"] += 1
                        result["space_freed_mb"] += file_size / (1024 * 1024)
                        result["deleted_files_list"].append({
                            "filename": item_name,
                            "age_minutes": round(file_age_minutes, 2),
                            "size_mb": round(file_size / (1024 * 1024), 2)
                        })
                        
                        # Remover del tracking
                        self.tracker.remove_tracking(item_path, directory_name)
                        
                        if cache_config.VERBOSE_CLEANUP_LOGGING:
                            print(f"   🗑️ Eliminado: {item_name} (edad: {file_age_minutes:.1f} min)")
                    else:
                        result["files_preserved"] += 1
                        
                except Exception as e:
                    error_msg = f"Error procesando {item_name}: {str(e)}"
                    result["errors"].append(error_msg)
                    if cache_config.VERBOSE_CLEANUP_LOGGING:
                        print(f"   ✗ {error_msg}")
        
        except Exception as e:
            error_msg = f"Error limpiando directorio {directory}: {str(e)}"
            result["errors"].append(error_msg)
            print(f"✗ {error_msg}")
        
        return result
    
    def _log_cleanup_result(self, result: Dict[str, Any]):
        """Imprime resultado detallado de la limpieza"""
        print(f"\n📊 RESULTADOS DE LIMPIEZA:")
        print(f"   - Archivos eliminados: {result['total_files_deleted']}")
        print(f"   - Archivos preservados: {result['total_files_preserved']}")
        print(f"   - Espacio liberado: {result['total_space_freed_mb']:.2f} MB")
        
        if result['directories_processed']:
            print(f"\n📁 POR DIRECTORIO:")
            for dir_result in result['directories_processed']:
                if dir_result['files_deleted'] > 0:
                    print(f"   {dir_result['directory_name']}:")
                    print(f"      • Eliminados: {dir_result['files_deleted']}")
                    print(f"      • Espacio: {dir_result['space_freed_mb']:.2f} MB")
                    print(f"      • TTL: {dir_result['ttl_minutes']} min")
        
        if result['errors']:
            print(f"\n⚠️ ERRORES ({len(result['errors'])}):")
            for error in result['errors'][:5]:  # Mostrar solo primeros 5
                print(f"   - {error}")
    
    def force_cleanup_now(self) -> Dict[str, Any]:
        """Fuerza una limpieza inmediata (útil para testing o mantenimiento manual)"""
        print("\n🔥 LIMPIEZA FORZADA MANUAL")
        return self.cleanup_expired_cache()
    
    def get_service_status(self) -> Dict[str, Any]:
        """Obtiene el estado actual del servicio"""
        return {
            "is_running": self.is_running,
            "cleanup_interval_minutes": cache_config.CLEANUP_INTERVAL_MINUTES,
            "tracker_stats": self.tracker.get_stats(),
            "ttl_config": cache_config.get_config_summary()
        }


# Instancia global del servicio
cache_cleanup_service = AutomaticCacheCleanupService()
cache_access_tracker = cache_cleanup_service.tracker
