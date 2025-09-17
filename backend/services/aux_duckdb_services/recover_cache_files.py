# services/aux_duckdb_services/recover_cache_files.py (COMPLETO CORREGIDO)
from datetime import datetime
import json
import os
import re
import time
from typing import Any, Dict, Optional

from services.aux_duckdb_services.registry import registry

class RecoverCacheFiles:
    def __init__(self):
        self._recovery_pending = False
        self._metadata_dir = None
        self._cache = None
        self._loaded_tables = None
    
    def schedule_auto_recovery(self, metadata_dir, cache, loaded_tables):
        """
        SOLUCIÓN: Programa la auto-recuperación para ejecutarse cuando sea seguro
        En lugar de ejecutar inmediatamente durante import
        """
        print("📅 Programando auto-recuperación para cuando file_controller esté disponible...")
        
        self._recovery_pending = True
        self._metadata_dir = metadata_dir
        self._cache = cache
        self._loaded_tables = loaded_tables
        
        # Intentar ejecutar inmediatamente si file_controller ya está disponible
        if registry.is_registered('file_controller'):
            print("✅ file_controller ya está disponible, ejecutando recuperación...")
            self._execute_pending_recovery()
        else:
            print("⏳ file_controller no disponible aún, esperando registro...")
    
    def _execute_pending_recovery(self):
        """Ejecuta la recuperación pendiente si hay una programada"""
        if not self._recovery_pending:
            return
            
        print("🚀 Ejecutando auto-recuperación programada...")
        self._recovery_pending = False
        
        # Ejecutar la recuperación real
        self.auto_recover_cached_files(
            self._metadata_dir, 
            self._cache, 
            self._loaded_tables
        )
        
        # Limpiar referencias
        self._metadata_dir = None
        self._cache = None
        self._loaded_tables = None
    
    def trigger_recovery_if_pending(self):
        """
        Método público para activar recuperación pendiente
        Llamado por file_controller cuando esté listo
        """
        if self._recovery_pending and registry.is_registered('file_controller'):
            self._execute_pending_recovery()
    
    def auto_recover_cached_files(self, metadata_dir, cache, loaded_tables):
        """Auto-recupera archivos desde cache - SOLUCIÓN DEFINITIVA"""
        try:
            print("🔄 Iniciando auto-recuperación de archivos desde cache...")
            
            recovered_count = 0
            
            if not os.path.exists(metadata_dir):
                print("📁 No hay metadata cache para recuperar")
                return
            
            for metadata_file in os.listdir(metadata_dir):
                if not metadata_file.endswith('_metadata.json'):
                    continue
                    
                try:
                    metadata_path = os.path.join(metadata_dir, metadata_file)
                    
                    with open(metadata_path, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)
                    
                    # SOLUCIÓN: Usar directamente el file_id de metadata
                    file_id = metadata.get('file_id')
                    original_name = metadata.get('original_name', 'archivo_desconocido')
                    
                    if not file_id:
                        print(f"⚠️ No hay file_id en metadata: {original_name}")
                        continue
                    
                    # Usar el file_id para construir la ruta del parquet
                    parquet_path = cache.get_cached_parquet_path(file_id)
                    
                    # Verificar que el Parquet existe
                    if not os.path.exists(parquet_path):
                        print(f"⚠️ Parquet no encontrado: {parquet_path}")
                        continue
                    
                    # Recuperar directamente usando el file_id
                    self._recover_single_file(file_id, parquet_path, metadata, loaded_tables)
                    recovered_count += 1
                    print(f"✅ Recuperado: {original_name} ({file_id})")
                            
                except Exception as e:
                    print(f"❌ Error recuperando {metadata_file}: {e}")
                    continue
            
            if recovered_count > 0:
                print(f"🎯 Auto-recuperación completada: {recovered_count} archivos restaurados")
            else:
                print("📋 No hay archivos para auto-recuperar")
                
        except Exception as e:
            print(f"❌ Error en auto-recuperación: {e}")


    def _recover_single_file(self, file_id: str, parquet_path: str, metadata: Dict[str, Any], loaded_tables: Dict[str, Any] = {}) -> None:
        """Recupera un archivo individual en DuckDB"""
        try:
            table_name = self._sanitize_table_name(f"table_{file_id}")
            
            loaded_tables[file_id] = {
                "table_name": table_name,
                "parquet_path": parquet_path,
                "loaded_at": datetime.now().isoformat(),
                "load_time": 0.001,
                "type": "lazy",
                "recovered": True,
                "original_metadata": metadata
            }
            
            print(f"📋 Archivo recuperado en loaded_tables: {file_id} → {table_name}")
            
        except Exception as e:
            print(f"❌ Error recuperando archivo individual {file_id}: {e}")

    def _sanitize_table_name(self, table_name: str) -> str:
        """Convierte nombres de tabla a formato seguro para DuckDB"""
        sanitized = table_name.replace('-', '_')
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', sanitized)
        
        if sanitized and sanitized[0].isdigit():
            sanitized = "t_" + sanitized
        
        if not sanitized:
            sanitized = "table_" + str(int(time.time()))
            
        return sanitized

    def get_recovery_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas de la recuperación"""
        try:
            file_controller_instance = registry.get('file_controller')
            if not file_controller_instance:
                return {"error": "file_controller no disponible"}
            
            all_files = file_controller_instance.list_all_files()
            
            return {
                "registry_available": registry.is_registered('file_controller'),
                "total_files": len(all_files.get("files", [])) if all_files else 0,
                "services_registered": registry.list_services()
            }
            
        except Exception as e:
            return {"error": str(e)}

    def force_cleanup_recovery(self, metadata_dir: str):
        """Fuerza limpieza de metadatos de archivos no disponibles"""
        try:
            if not os.path.exists(metadata_dir):
                return
            
            file_controller_instance = registry.get('file_controller')
            
            if not file_controller_instance:
                print("⚠️ No se puede limpiar sin file_controller")
                return
            
            all_files = file_controller_instance.list_all_files()
            available_files = {f.get("original_name") for f in all_files.get("files", [])}
            
            cleaned_count = 0
            
            for metadata_file in os.listdir(metadata_dir):
                if not metadata_file.endswith('_metadata.json'):
                    continue
                
                try:
                    metadata_path = os.path.join(metadata_dir, metadata_file)
                    with open(metadata_path, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)
                    
                    original_name = metadata.get('original_name', '')
                    
                    if original_name not in available_files:
                        os.remove(metadata_path)
                        cleaned_count += 1
                        print(f"🗑️ Eliminado metadata obsoleto: {original_name}")
                        
                except Exception as e:
                    print(f"❌ Error limpiando {metadata_file}: {e}")
            
            if cleaned_count > 0:
                print(f"🧹 Limpieza completada: {cleaned_count} metadatos obsoletos eliminados")
            else:
                print("✨ No hay metadatos obsoletos para limpiar")
                
        except Exception as e:
            print(f"❌ Error en limpieza forzada: {e}")

# INSTANCIA GLOBAL - SINGLETON
recover_cache_files = RecoverCacheFiles()
