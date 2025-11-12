# controllers/technical_note_controller/technical_note.py
import os
import shutil
from typing import Dict, Any, List, Optional
from fastapi import HTTPException


from controllers.technical_note_controller.absent_user_controller import AbsentUserController
from controllers.technical_note_controller.age_controller import AgeController
from services.duckdb_service.duckdb_service import duckdb_service
from services.aux_duckdb_services.query_pagination import QueryPagination


from services.technical_note_services.data_source_service import DataSourceService
from services.technical_note_services.geographic_service import GeographicService
from services.technical_note_services.report_service import ReportService
from utils.technical_note_utils.file_utils import generate_file_key, is_supported_file
from utils.technical_note_utils.display_utils import generate_display_name, generate_description


class TechnicalNoteController:
    """Controlador principal refactorizado con separación de responsabilidades"""
    
    def __init__(self, storage_manager):
        self.storage_manager = storage_manager
        self.static_files_dir = "technical_note"
        self.loaded_technical_files = {}
        
        # NUEVO: Limpiar archivos precargados al iniciar
        self._clear_technical_note_directory()
        
        # Servicios inyectados
        self.data_source_service = DataSourceService(self.static_files_dir)
        self.geographic_service = GeographicService()
        self.report_service = ReportService()
        self.query_pagination = QueryPagination()
    
    def _clear_technical_note_directory(self):
        """Limpia archivos precargados del directorio technical_note al iniciar"""
        try:
            if os.path.exists(self.static_files_dir):
                # Eliminar directorio completo
                shutil.rmtree(self.static_files_dir)
                print(f"✓ Directorio eliminado: {self.static_files_dir}")
            
            # Recrear directorio vacío
            os.makedirs(self.static_files_dir, exist_ok=True)
            print(f"✓ Directorio recreado vacío: {self.static_files_dir}")
            print("✓ Archivos precargados de technical_note eliminados")
            
        except Exception as e:
            print(f"✗ Error limpiando directorio technical_note: {e}")
            # Asegurar que el directorio exista incluso si falla la limpieza
            os.makedirs(self.static_files_dir, exist_ok=True)
    
    def get_geographic_values(
        self, 
        filename: str, 
        geo_type: str,
        departamento: Optional[str] = None,
        municipio: Optional[str] = None
    ) -> Dict[str, Any]:
        """Obtiene valores únicos de columnas geográficas usando servicio especializado"""
        try:
            file_key = generate_file_key(filename)
            
            try:
                data_source = self.data_source_service.ensure_data_source_available(filename, file_key)
            except Exception as data_error:
                return self._build_error_response(
                    f"No se puede leer el archivo: {data_error}", 
                    geo_type
                )
            
            if not self.data_source_service.verify_data_source_readable(data_source):
                return self._build_error_response(
                    "No se puede leer el archivo", 
                    geo_type
                )
            
            result = self.geographic_service.get_geographic_values(
                data_source, geo_type, departamento, municipio
            )
            
            result["filename"] = filename
            return result
            
        except Exception as e:
            print(f"Error en get_geographic_values: {e}")
            import traceback
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=f"Error obteniendo valores geográficos: {str(e)}")
    
    def read_technical_file_data_paginated(
        self, 
        filename: str, 
        page: int = 1, 
        page_size: int = 1000,
        sheet_name: Optional[str] = None,
        filters: Optional[List[Dict[str, Any]]] = None,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None
    ) -> Dict[str, Any]:
        """Lectura paginada usando infraestructura existente y servicios"""
        try:
            file_path = os.path.join(self.static_files_dir, filename)
            
            if not os.path.exists(file_path):
                raise HTTPException(status_code=404, detail=f"Archivo no encontrado: {filename}")
            
            file_key = generate_file_key(filename)
            
            self._ensure_file_loaded(filename, file_path, file_key)
            
            result = self.query_pagination.query_data_ultra_fast(
                conn=duckdb_service.conn,
                file_id=file_key,
                filters=filters,
                search=search,
                sort_by=sort_by,
                sort_order=sort_order or "asc",
                page=page,
                page_size=page_size,
                selected_columns=None,
                loaded_tables=duckdb_service.loaded_tables
            )
            
            if not result.get("success", True):
                raise HTTPException(status_code=500, detail=result.get("error", "Error en consulta"))
            
            return self._enrich_pagination_response(result, filename, filters, search, sort_by, sort_order)
            
        except HTTPException:
            raise
        except Exception as e:
            print(f"Error en read_technical_file_data_paginated: {e}")
            import traceback
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    
    def get_keyword_age_report(
        self,
        filename: str,
        keywords: Optional[List[str]] = None,
        min_count: int = 0,
        include_temporal: bool = True,
        departamento: Optional[str] = None,
        municipio: Optional[str] = None,
        ips: Optional[str] = None,
        corte_fecha: str = None  # SIN VALOR POR DEFECTO - DEBE VENIR DEL FRONTEND
    ) -> Dict[str, Any]:
        """Genera reporte CON NUMERADOR/DENOMINADOR usando FECHA DINÁMICA"""
        try:
            # VALIDAR QUE VENGA LA FECHA
            if not corte_fecha:
                raise HTTPException(
                    status_code=400,
                    detail="El parámetro 'corte_fecha' es obligatorio y debe venir desde el frontend"
                )            
            file_key = generate_file_key(filename)
            
            try:
                data_source = self.data_source_service.ensure_data_source_available(filename, file_key)
            except Exception as data_error:
                raise HTTPException(
                    status_code=500, 
                    detail=f"No se pudo acceder a los datos de {filename}: {str(data_error)}"
                )
            
            geographic_filters = {
                'departamento': departamento,
                'municipio': municipio,
                'ips': ips
            }
            
            # PASAR FECHA DINÁMICA AL SERVICIO
            return self.report_service.generate_keyword_age_report(
                data_source=data_source,
                filename=filename,
                keywords=keywords,
                min_count=min_count,
                include_temporal=include_temporal,
                geographic_filters=geographic_filters,
                corte_fecha=corte_fecha  # FECHA DINÁMICA
            )
            
        except HTTPException:
            raise
        except Exception as e:
            print(f"Error completo en reporte: {e}")
            import traceback
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=f"Error generando reporte: {str(e)}")
    
    def get_technical_file_metadata(self, filename: str) -> Dict[str, Any]:
        """Obtiene metadatos usando servicios especializados"""
        try:
            print(f"Obteniendo metadatos para: {filename}")
            
            file_key = generate_file_key(filename)
            data_source = self.data_source_service.ensure_data_source_available(filename, file_key)
            
            describe_sql = f"DESCRIBE SELECT * FROM {data_source}"
            columns_result = duckdb_service.conn.execute(describe_sql).fetchall()
            
            count_sql = f"SELECT COUNT(*) FROM {data_source}"
            total_rows = duckdb_service.conn.execute(count_sql).fetchone()[0]
            
            columns = [
                {
                    "name": row[0],
                    "type": row[1],
                    "null": row[2] if len(row) > 2 else "Unknown"
                }
                for row in columns_result
            ]
            
            print(f"Metadatos obtenidos: {len(columns)} columnas, {total_rows:,} filas")
            
            return {
                "success": True,
                "filename": filename,
                "display_name": generate_display_name(filename),
                "description": generate_description(filename),
                "total_rows": total_rows,
                "total_columns": len(columns),
                "columns": [col["name"] for col in columns],
                "detailed_columns": columns,
                "file_size": 0,
                "extension": "csv",
                "encoding": "utf-8",
                "recommended_page_size": 1000,
                "data_source_used": data_source,
                "engine": "DuckDB_Service_Existing_Methods"
            }
            
        except Exception as e:
            print(f"Error obteniendo metadatos: {e}")
            return {
                "success": False,
                "error": str(e),
                "filename": filename,
                "total_rows": 0,
                "total_columns": 0,
                "columns": []
            }
    
    def get_column_unique_values(
        self, 
        filename: str, 
        column_name: str,
        sheet_name: Optional[str] = None,
        limit: int = 1000
    ) -> Dict[str, Any]:
        """Valores únicos usando sistema existente"""
        try:
            file_key = generate_file_key(filename)
            data_source = self.data_source_service.ensure_data_source_available(filename, file_key)
            
            try:
                unique_values = duckdb_service.get_unique_values_ultra_fast(
                    file_key, column_name, limit
                )
            except Exception:
                unique_values = self._get_unique_values_fallback(data_source, column_name, limit)
            
            return {
                "filename": filename,
                "column_name": column_name,
                "unique_values": unique_values,
                "total_unique": len(unique_values),
                "limited": len(unique_values) >= limit,
                "limit_applied": limit,
                "ultra_fast": True,
                "engine": "DuckDB_Service_Existing"
            }
            
        except Exception as e:
            print(f"Error en get_column_unique_values: {e}")
            raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    
    def get_available_static_files(self) -> List[Dict[str, Any]]:
        """Lista archivos técnicos disponibles"""
        try:
            if not os.path.exists(self.static_files_dir):
                return []
            
            available_files = []
            
            for filename in os.listdir(self.static_files_dir):
                file_path = os.path.join(self.static_files_dir, filename)
                
                if not os.path.isfile(file_path) or not is_supported_file(filename):
                    continue
                
                file_info = self._build_file_info(filename, file_path)
                available_files.append(file_info)
            
            return available_files
            
        except Exception as e:
            print(f"Error listando archivos técnicos: {e}")
            return []
    
    # Métodos auxiliares privados
    def _ensure_file_loaded(self, filename: str, file_path: str, file_key: str):
        """Asegura que el archivo esté cargado en el sistema"""
        if file_key not in duckdb_service.loaded_tables:
            print(f"Cargando archivo técnico: {filename}")
            
            _, ext = os.path.splitext(filename)
            
            parquet_result = duckdb_service.convert_file_to_parquet(
                file_path=file_path,
                file_id=file_key,
                original_name=filename,
                ext=ext.replace('.', '')
            )
            
            if parquet_result["success"]:
                table_name = duckdb_service.load_parquet_lazy(
                    file_key, 
                    parquet_result["parquet_path"]
                )
                
                self.loaded_technical_files[file_key] = {
                    "table_name": table_name,
                    "columns": parquet_result["columns"],
                    "total_rows": parquet_result["total_rows"],
                    "parquet_path": parquet_result["parquet_path"]
                }
                
                print(f"Archivo técnico cargado: {filename} -> {table_name}")
            else:
                raise HTTPException(status_code=500, detail="Error convirtiendo archivo técnico")
        else:
            table_info = duckdb_service.loaded_tables[file_key]
            if file_key not in self.loaded_technical_files:
                self.loaded_technical_files[file_key] = {
                    "table_name": table_info.get("table_name", f"table_{file_key}"),
                    "columns": [],
                    "total_rows": 0,
                    "parquet_path": table_info.get("parquet_path")
                }
    
    def _enrich_pagination_response(
        self, result: Dict[str, Any], filename: str, 
        filters, search, sort_by, sort_order
    ) -> Dict[str, Any]:
        """Enriquece la respuesta de paginación"""
        file_key = generate_file_key(filename)
        file_stats = duckdb_service.get_file_stats(file_key)
        
        return {
            "success": True,
            "filename": filename,
            "display_name": generate_display_name(filename),
            "description": generate_description(filename),
            "data": result["data"],
            "columns": result["columns"],
            "pagination": result["pagination"],
            "filters_applied": filters or [],
            "search_applied": search,
            "sort_applied": {"column": sort_by, "order": sort_order} if sort_by else None,
            "file_info": {
                "total_rows": result.get("total_rows", file_stats.get("total_rows", 0)),
                "total_columns": len(result["columns"]),
                "processing_method": "duckdb_ultra_fast",
                "ultra_fast": True,
                "engine": "DuckDB_Service_Methods"
            }
        }
    
    def _get_unique_values_fallback(self, data_source: str, column_name: str, limit: int) -> List:
        """Fallback para obtener valores únicos"""
        column_escaped = duckdb_service.escape_identifier(column_name)
        query = f"""
        SELECT DISTINCT {column_escaped} 
        FROM {data_source} 
        WHERE {column_escaped} IS NOT NULL 
        ORDER BY {column_escaped} 
        LIMIT {limit}
        """
        
        result = duckdb_service.conn.execute(query).fetchall()
        return [row[0] for row in result if row[0] is not None]
    
    def _build_file_info(self, filename: str, file_path: str) -> Dict[str, Any]:
        """Construye información de archivo"""
        _, ext = os.path.splitext(filename)
        file_size = os.path.getsize(file_path)
        
        return {
            "filename": filename,
            "display_name": generate_display_name(filename),
            "file_path": file_path,
            "extension": ext.lower().replace('.', ''),
            "file_size": file_size,
            "description": generate_description(filename),
            "ultra_fast_ready": True
        }
    
    def _build_error_response(self, error_message: str, geo_type: str) -> Dict[str, Any]:
        """Construye respuesta de error para operaciones geográficas"""
        return {
            "success": False,
            "error": error_message,
            "geo_type": geo_type,
            "values": []
        }
    
    def get_age_ranges(self, filename: str, corte_fecha: str):
        """MODIFICADO: Pasar fecha dinámica al controlador de edad"""
        return AgeController().get_age_ranges(filename, corte_fecha, self.static_files_dir)
    
    def get_inasistentes_report(
        self, filename: str, selected_months: List[int],
        selected_years: List[int] = None, selected_keywords: List[str] = None,
        corte_fecha: str = None,  # SIN VALOR POR DEFECTO
        departamento: Optional[str] = None, municipio: Optional[str] = None,
        ips: Optional[str] = None
    ):
        """MODIFICADO: Pasar fecha dinámica al controlador de ausentes"""
        return AbsentUserController().get_inasistentes_report(
            filename, selected_months, selected_years, selected_keywords, corte_fecha,
            departamento, municipio, ips, self.static_files_dir
        )
    
    def export_inasistentes_csv(
        self, filename: str, selected_months: List[int],
        selected_years: List[int] = None, selected_keywords: List[str] = None,
        corte_fecha: str = None,  # SIN VALOR POR DEFECTO
        departamento: Optional[str] = None,
        municipio: Optional[str] = None, ips: Optional[str] = None
    ):
        """MODIFICADO: Pasar fecha dinámica al exportador"""
        return AbsentUserController().export_inasistentes_to_csv(
            filename, selected_months, selected_years, selected_keywords, 
            corte_fecha, departamento, municipio, ips, self.static_files_dir
        )


# Función factory para mantener compatibilidad
def get_technical_note_controller():
    from controllers.files_controllers.storage_manager import storage_manager
    return TechnicalNoteController(storage_manager)


technical_note_controller = get_technical_note_controller()
