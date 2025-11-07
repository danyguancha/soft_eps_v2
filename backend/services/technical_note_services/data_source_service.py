# services/technical_note_services/data_source_service.py
import os
from typing import Optional
from services.duckdb_service.duckdb_service import duckdb_service
from services.aux_duckdb_services.query_pagination import QueryPagination
from utils.technical_note_utils.file_utils import find_csv_file

class DataSourceService:
    """Servicio especializado para manejo de fuentes de datos"""
    
    def __init__(self, static_files_dir: str = "technical_note"):
        self.static_files_dir = static_files_dir
        self.query_pagination = QueryPagination()
    
    def ensure_data_source_available(self, filename: str, file_key: str) -> str:
        """
        Asegura que la fuente de datos esté disponible usando métodos existentes
        """
        try:
            print(f"🔍 Verificando fuente de datos para: {filename}")
            
            # Usar método existente: ensure_parquet_exists_or_regenerate
            if self.query_pagination.ensure_parquet_exists_or_regenerate(
                file_key, duckdb_service.loaded_tables
            ):
                table_info = duckdb_service.loaded_tables[file_key]
                parquet_path = table_info.get('parquet_path')
                print(f"Usando Parquet existente: {parquet_path}")
                return f"read_parquet('{parquet_path}')"
            
            # Usar método existente: _load_file_on_demand_with_regeneration
            if self.query_pagination._load_file_on_demand_with_regeneration(
                file_key, duckdb_service.loaded_tables
            ):
                table_info = duckdb_service.loaded_tables[file_key]
                parquet_path = table_info.get('parquet_path')
                print(f"Cargado bajo demanda: {parquet_path}")
                return f"read_parquet('{parquet_path}')"
            
            # Convertir desde CSV como última opción
            return self._convert_from_csv(filename, file_key)
            
        except Exception as e:
            print(f"❌ Error asegurando fuente de datos: {e}")
            raise Exception(f"No se pudo obtener fuente de datos para {filename}: {e}")
    
    def _convert_from_csv(self, filename: str, file_key: str) -> str:
        """Convierte CSV a formato DuckDB usando métodos existentes"""
        csv_path = self._find_csv_file(filename)
        if not csv_path:
            raise Exception(f"No se encontró archivo CSV para {filename}")
        
        print(f"🔄 Convirtiendo usando DuckDBService: {csv_path}")
        
        # Usar método existente: convert_file_to_parquet
        _, ext = os.path.splitext(filename)
        conversion_result = duckdb_service.convert_file_to_parquet(
            file_path=csv_path,
            file_id=file_key,
            original_name=filename,
            ext=ext.replace('.', '')
        )
        
        if conversion_result.get("success"):
            parquet_path = conversion_result.get("parquet_path")
            # Usar método existente: load_parquet_lazy
            duckdb_service.load_parquet_lazy(file_key, parquet_path)
            print(f"Conversión exitosa: {parquet_path}")
            return f"read_parquet('{parquet_path}')"
        
        # Fallback a CSV directo
        print(f"⚠️ Fallback a CSV directo: {csv_path}")
        return f"read_csv('{csv_path}', AUTO_DETECT=true, header=true)"
    
    def _find_csv_file(self, filename: str) -> Optional[str]:
        """Busca archivo CSV usando utilidad"""
        search_paths = [self.static_files_dir, ".", ""]
        return find_csv_file(filename, search_paths)
    
    def verify_data_source_readable(self, data_source: str) -> bool:
        """Verifica que la fuente de datos sea legible"""
        try:
            test_query = f"SELECT COUNT(*) FROM {data_source}"
            result = duckdb_service.conn.execute(test_query).fetchone()
            print(f"Archivo legible: {result[0]} filas totales")
            return True
        except Exception as test_error:
            print(f"❌ Error leyendo archivo: {test_error}")
            return False
