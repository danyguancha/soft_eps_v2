

from datetime import datetime
import json
import os
import re
import time
from typing import Any, Dict, Optional


class RecoverCacheFiles:
    
    def auto_recover_cached_files(self, metadata_dir, cache, loaded_tables):
        """Auto-recupera archivos desde cache al reiniciar"""
        try:
            print("🔄 Iniciando auto-recuperación de archivos desde cache...")
            
            recovered_count = 0
            
            # Obtener todos los archivos de metadata
            if not os.path.exists(metadata_dir):
                print("📁 No hay metadata cache para recuperar")
                return
            
            # Buscar archivos de metadata
            for metadata_file in os.listdir(metadata_dir):
                if not metadata_file.endswith('_metadata.json'):
                    continue
                    
                try:
                    # Leer metadata
                    metadata_path = os.path.join(metadata_dir, metadata_file)
                    
                    with open(metadata_path, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)
                    
                    file_hash = metadata.get('file_hash')
                    if not file_hash:
                        continue
                        
                    parquet_path = cache.get_cached_parquet_path(file_hash)
                    
                    # Verificar que el Parquet existe
                    if not os.path.exists(parquet_path):
                        print(f"⚠️ Parquet no encontrado para hash {file_hash}: {parquet_path}")
                        continue
                    
                    # Buscar file_id correspondiente en el file_controller
                    file_id = self.find_file_id_for_metadata(metadata)
                    
                    if file_id:
                        # Recargar en DuckDB
                        self._recover_single_file(file_id, parquet_path, metadata, loaded_tables)
                        recovered_count += 1
                        print(f"✅ Recuperado: {metadata.get('original_name')} ({file_id})")
                    else:
                        print(f"⚠️ No se encontró file_id para {metadata.get('original_name')}")
                        
                except Exception as e:
                    print(f"❌ Error recuperando {metadata_file}: {e}")
                    continue
            
            if recovered_count > 0:
                print(f"🎯 Auto-recuperación completada: {recovered_count} archivos restaurados")
                
            else:
                print("📋 No hay archivos para auto-recuperar")
                
        except Exception as e:
            print(f"❌ Error en auto-recuperación: {e}")

    def find_file_id_for_metadata(self, metadata: Dict[str, Any]) -> Optional[str]:
        """Busca el file_id correspondiente para una metadata"""
        try:
            # SOLUCIÓN: Import lazy dentro de la función
            import importlib
            
            # Importar de forma segura para evitar circular import
            try:
                file_controller_module = importlib.import_module('controllers.file_controller')
                list_all_files_func = getattr(file_controller_module, 'list_all_files', None)
                
                if not list_all_files_func:
                    print("⚠️ Función list_all_files no disponible en file_controller")
                    return None
                    
            except (ImportError, AttributeError) as e:
                print(f"⚠️ No se pudo importar file_controller: {e}")
                return None
            
            # Obtener todos los archivos del file_controller
            all_files_info = list_all_files_func()
            
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

        
    def recover_single_file(self, file_id: str, parquet_path: str, metadata: Dict[str, Any], loaded_tables: Dict[str, Any] = {}) -> None:
        """Recupera un archivo individual en DuckDB"""
        try:
            # Registrar en loaded_tables
            table_name = self._sanitize_table_name(f"table_{file_id}")
            
            loaded_tables[file_id] = {
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