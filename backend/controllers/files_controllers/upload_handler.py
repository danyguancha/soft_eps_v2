# controllers/files_controllers/upload_handler.py (VERSIÓN COMPLETA CON HOJAS EXCEL)
import uuid
import os
import time
import aiofiles
from fastapi import UploadFile, HTTPException
from typing import Dict, Any, Optional, List
from services.csv_service import CSVService
from services.excel_service import ExcelService
from controllers.files_controllers.storage_manager import FileStorageManager
from services.duckdb_service import duckdb_service

class UploadHandler:
    def __init__(self, storage_manager: FileStorageManager):
        self.storage_manager = storage_manager
        self.max_file_size = 5 * 1024 * 1024 * 1024  # 5GB límite
        self.chunk_size = 8192
        
        self.file_services = {
            "csv": CSVService(),
            "xlsx": ExcelService(),
            "xls": ExcelService()
        }

    async def upload_file(self, file: UploadFile) -> Dict[str, Any]:
        """UPLOAD ULTRA-OPTIMIZADO que SIEMPRE devuelve hojas Excel"""
        if not file.filename:
            raise HTTPException(status_code=400, detail="Nombre de archivo requerido")
            
        ext = file.filename.split('.')[-1].lower()
        if ext not in self.file_services:
            raise HTTPException(
                status_code=400, 
                detail=f"Tipo de archivo no soportado: {ext}"
            )
        
        file_id = str(uuid.uuid4())
        upload_dir = self.storage_manager.ensure_upload_directory()
        file_path = os.path.join(upload_dir, f"{file_id}.{ext}")
        
        try:
            # Guardar archivo
            await self._save_file_streaming(file, file_path)
            
            # OBTENER HOJAS PARA ARCHIVOS EXCEL (SIEMPRE)
            sheets = []
            default_sheet = None
            sheet_detection_time = 0
            
            if ext in ['xlsx', 'xls']:
                sheet_start = time.time()
                
                try:
                    sheet_info = duckdb_service.get_excel_sheets(file_path)
                    sheet_detection_time = time.time() - sheet_start
                    
                    if sheet_info["success"]:
                        sheets = sheet_info["sheets"]
                        default_sheet = sheet_info["default_sheet"]
                    else:
                        sheets = ["Sheet1"]  # Fallback
                        default_sheet = "Sheet1"
                        
                except Exception as e:
                    sheets = ["Sheet1"]  # Fallback de emergencia
                    default_sheet = "Sheet1"
                    sheet_detection_time = 0.001
            else:
                # Para archivos CSV
                sheets = []
                default_sheet = None
        
            # CONVERSIÓN OPTIMIZADA
            parquet_result = duckdb_service.convert_file_to_parquet(
                file_path, file_id, file.filename, ext
            )
            
            if not parquet_result["success"]:
                error_msg = parquet_result["error"]
                if "Timeout" in error_msg:
                    raise HTTPException(status_code=413, detail=f"Archivo demasiado grande: {error_msg}")
                else:
                    raise HTTPException(status_code=500, detail=error_msg)
            
            # CARGA LAZY INSTANTÁNEA
            table_name = duckdb_service.load_parquet_lazy(
                file_id, parquet_result["parquet_path"]
            )
            
            # INFORMACIÓN COMPLETA CON HOJAS GARANTIZADAS
            file_info = {
                "path": file_path,
                "parquet_path": parquet_result["parquet_path"],
                "table_name": table_name,
                "ext": ext,
                "original_name": file.filename,
                "sheets": sheets,  # SIEMPRE incluir hojas
                "default_sheet": default_sheet,  # SIEMPRE incluir hoja por defecto
                "columns": parquet_result["columns"],
                "total_rows": parquet_result["total_rows"],
                "strategy": "ULTRA_OPTIMIZED_WITH_SHEETS",
                "engine": "DuckDB + Direct Parquet + Sheet Detection",
                "file_size_mb": os.path.getsize(file_path) / 1024 / 1024,
                "method": parquet_result.get("method"),
                "compression_ratio": parquet_result["compression_ratio"],
                "conversion_time": parquet_result["conversion_time"],
                "from_cache": parquet_result.get("from_cache", False),
                "file_hash": parquet_result.get("file_hash"),
                "cached": parquet_result.get("cached", False),
                "load_type": "lazy",
                "sheet_detection_time": sheet_detection_time
            }
            
            self.storage_manager.store_file_info(file_id, file_info)
            
            # RESPUESTA GARANTIZADA CON HOJAS
            response = {
                "file_id": file_id,
                "columns": parquet_result["columns"],
                "sheets": sheets,  # SIEMPRE devolver hojas
                "default_sheet": default_sheet,  # SIEMPRE devolver hoja por defecto
                "total_rows": parquet_result["total_rows"],
                "ultra_fast": True,
                "engine": "DuckDB + Direct Parquet + Sheet Detection",
                "file_size_mb": os.path.getsize(file_path) / 1024 / 1024,
                "processing_method": parquet_result.get("method"),
                "compression_ratio": parquet_result["compression_ratio"],
                "processing_time": parquet_result["conversion_time"],
                "from_cache": parquet_result.get("from_cache", False),
                "cache_hit": parquet_result.get("from_cache", False),
                "file_hash": parquet_result.get("file_hash"),
                "load_type": "lazy",
                "ultra_optimized": True,
                "has_sheets": len(sheets) > 1,  # Indicar si tiene múltiples hojas
                "sheet_count": len(sheets),  # Número total de hojas
                "sheet_detection_time": sheet_detection_time,
                "is_excel": ext in ['xlsx', 'xls']  # Indicador de Excel
            }            
            return response
                
        except Exception as e:
            # Limpiar archivos en caso de error
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except:
                    pass
            
            raise HTTPException(status_code=500, detail=f"Error procesando archivo: {str(e)}")

    async def _save_file_streaming(self, file: UploadFile, file_path: str):
        """Guarda archivo con verificación completa y progreso"""
        total_size = 0
        
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            await file.seek(0)
            
            with open(file_path, 'wb') as f:
                while True:
                    chunk = await file.read(self.chunk_size)
                    if not chunk:
                        break
                    
                    total_size += len(chunk)
                    
                    if total_size > self.max_file_size:
                        raise HTTPException(
                            status_code=413, 
                            detail="Archivo excede el tamaño máximo permitido (5GB)"
                        )
                    
                    f.write(chunk)
                    
                    # Mostrar progreso para archivos grandes
                    if total_size % (50 * 1024 * 1024) == 0:  # Cada 50MB
                        print(f"📤 Guardado: {total_size/1024/1024:.1f}MB")
            
            if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
                raise HTTPException(
                    status_code=500,
                    detail="ERROR: Archivo no se guardó correctamente"
                )
            
            print(f"Archivo guardado exitosamente: {total_size/1024/1024:.1f}MB")
            
        except Exception as e:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except:
                    pass
            raise e

    def get_data_ultra_fast(
        self, 
        file_id: str, 
        filters: Optional[List[Dict[str, Any]]] = None,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "ASC",
        page: int = 1,
        page_size: int = 1000,
        selected_columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """ULTRA-OPTIMIZADO: Consulta directa usando lazy load"""
        
        return duckdb_service.query_data_ultra_fast(
            file_id=file_id,
            filters=filters,
            search=search,
            sort_by=sort_by,
            sort_order=sort_order,
            page=page,
            page_size=page_size,
            selected_columns=selected_columns
        )

    # Mantener compatibilidad con métodos existentes
    def get_data_adaptive(self, file_id: str, sheet_name: Optional[str] = None, columns_needed: Optional[List[str]] = None):
        """Redirige a método ultra-optimizado"""
        result = self.get_data_ultra_fast(
            file_id=file_id,
            selected_columns=columns_needed,
            page=1,
            page_size=100000  # Obtener muchos registros
        )
        return result["data"]

    def is_supported_file(self, filename: str) -> bool:
        """Verifica si el tipo de archivo es soportado"""
        ext = filename.split('.')[-1].lower()
        return ext in self.file_services

# Instancias globales
storage_manager = FileStorageManager()
upload_handler_instance = UploadHandler(storage_manager)
