# services/duckdb_service/file_management/file_loader_service.py
import os
import json
from typing import Dict, Any, Optional
from utils.duckdb_utils.validation_utils import validate_file_id_format

class FileLoaderService:
    """Servicio especializado para carga de archivos bajo demanda"""
    
    def __init__(self, metadata_dir: str, cache_controller, loaded_tables: Dict[str, Any]):
        self.metadata_dir = metadata_dir
        self.cache = cache_controller
        self.loaded_tables = loaded_tables
    
    def load_file_on_demand(self, file_id: str, conversion_service, query_service) -> bool:
        """Carga un archivo bajo demanda si existe en cache"""
        try:            
            if not validate_file_id_format(file_id):
                print(f"File ID inválido: {file_id}")
                return False
            
            if not os.path.exists(self.metadata_dir):
                return False
            
            # Obtener información del archivo
            file_info = self._get_file_info(file_id)
            if not file_info:
                return False
            
            # Buscar Parquet existente
            parquet_path, metadata = self._find_existing_parquet(file_info['original_name'])
            
            if parquet_path:
                # Cargar desde cache existente
                from services.aux_duckdb_services.recover_cache_files import RecoverCacheFiles
                RecoverCacheFiles().recover_single_file(
                    file_id, parquet_path, metadata, self.loaded_tables
                )
                return True
            else:
                # Convertir desde archivo original
                return self._convert_from_original(
                    file_id, file_info, conversion_service, query_service
                )
                
        except Exception as e:
            print(f"Error en carga bajo demanda para {file_id}: {e}")
            return False
    
    def _get_file_info(self, file_id: str) -> Optional[Dict[str, Any]]:
        """Obtiene información del archivo desde file_controller"""
        try:
            from controllers import file_controller
            file_info = file_controller.get_file_info(file_id)
            return {
                'path': file_info.get("path"),
                'original_name': file_info.get("original_name"),
                'extension': file_info.get("extension", "xlsx")
            }
        except:
            print(f"No se pudo obtener info del archivo {file_id}")
            return None
    
    def _find_existing_parquet(self, original_name: str) -> tuple:
        """Busca Parquet existente por nombre original"""
        for metadata_file in os.listdir(self.metadata_dir):
            if not metadata_file.endswith('_metadata.json'):
                continue
                
            try:
                metadata_path = os.path.join(self.metadata_dir, metadata_file)
                
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                
                # Verificar si coincide el nombre original
                if metadata.get('original_name') == original_name:
                    file_id = metadata.get('file_id')
                    parquet_path = self.cache.get_cached_parquet_path(file_id)
                    
                    if os.path.exists(parquet_path):
                        return parquet_path, metadata
                        
            except Exception:
                continue
        
        return None, None
    
    def _convert_from_original(self, file_id: str, file_info: Dict[str, Any], 
                             conversion_service, query_service) -> bool:
        """Convierte archivo original a Parquet"""
        file_path = file_info['path']
        if not file_path or not os.path.exists(file_path):
            return False
        
        try:
            print(f"Convirtiendo archivo {file_id} desde cero...")
            
            result = conversion_service.convert_file_to_parquet(
                file_path=file_path,
                file_id=file_id,
                original_name=file_info['original_name'],
                ext=file_info['extension']
            )
            
            if result.get("success"):
                query_service.load_parquet_lazy(file_id, result["parquet_path"])
                return True
            
            return False
            
        except Exception as e:
            print(f"Error convirtiendo archivo {file_id}: {e}")
            return False
