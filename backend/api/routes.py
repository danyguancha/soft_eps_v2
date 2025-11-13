import os
import traceback
import threading
from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from typing import Optional, List
from fastapi.responses import FileResponse

from models.schemas import (
    BulkDeleteRequest, DeleteResponse, DeleteRowsByFilterRequest, DeleteRowsRequest, 
    ExportRequest, ExportResponse, FileCrossRequest, FileUploadResponse, DataRequest, 
    TransformRequest
)
from controllers import file_controller
from services.cross_service import CrossService
from services.export_service import ExportService
from controllers.cross_controller import cross_controller

# IMPORTAR WRAPPER SEGURO DE DUCKDB
try:
    from services.duckdb_service_wrapper import safe_duckdb_service as duckdb_service
except ImportError:
    try:
        from services.duckdb_service.duckdb_service import duckdb_service
    except ImportError:
        duckdb_service = None

router = APIRouter()

# ========== CONFIGURACIÓN ==========

class EndpointConfig:
    """Configuración para endpoints seguros"""
    OPERATION_TIMEOUT = 60
    MAX_RETRIES = 3
    RETRY_DELAY = 2
    FALLBACK_ENABLED = True

def execute_with_timeout(func, timeout_seconds=30, *args, **kwargs):
    """Ejecuta función con timeout para prevenir hangs"""
    result_container = [None]
    exception_container = [None]
    
    def target():
        try:
            result_container[0] = func(*args, **kwargs)
        except Exception as e:
            exception_container[0] = e
    
    thread = threading.Thread(target=target)
    thread.daemon = True
    thread.start()
    thread.join(timeout_seconds)
    
    if thread.is_alive():
        raise TimeoutError(f"Operación tomó más de {timeout_seconds}s")
    
    if exception_container[0]:
        raise exception_container[0]
    
    return result_container[0]

# ========== ENDPOINTS PRINCIPALES ==========

@router.post("/upload", response_model=FileUploadResponse)
async def upload_file(file: UploadFile = File(...)):
    """Carga archivo con detección completa de hojas en archivos Excel"""
    try:        
        result = await execute_with_timeout(
            file_controller.upload_file, 
            timeout_seconds=300,
            file=file
        )
        
        response_data = {
            "message": "Archivo cargado exitosamente",
            "file_id": result["file_id"],
            "columns": result["columns"],
            "sheets": result.get("sheets", []),
            "default_sheet": result.get("default_sheet"),
            "total_rows": result["total_rows"],
            "is_excel": result.get("is_excel", False),
            "has_sheets": result.get("has_sheets", False),
            "sheet_count": result.get("sheet_count", 0),
            "sheet_detection_time": result.get("sheet_detection_time"),
            "ultra_fast": result.get("ultra_fast", True),
            "engine": result.get("engine", "Standard"),
            "file_size_mb": result.get("file_size_mb"),
            "processing_method": result.get("processing_method"),
            "from_cache": result.get("from_cache", False)
        }        
        return FileUploadResponse(**response_data)
        
    except TimeoutError:
        raise HTTPException(status_code=408, detail="Upload timeout")
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/data")
def get_data(request: DataRequest):
    """Obtiene datos con filtros, ordenamiento y paginación"""
    try:
        return execute_with_timeout(
            file_controller.get_data,
            timeout_seconds=EndpointConfig.OPERATION_TIMEOUT,
            request=request
        )
    except TimeoutError:
        raise HTTPException(status_code=408, detail="Timeout obteniendo datos")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/transform")
def transform_data(request: TransformRequest):
    """Aplica transformaciones a los datos"""
    try:
        return execute_with_timeout(
            file_controller.transform_data,
            timeout_seconds=EndpointConfig.OPERATION_TIMEOUT * 2,
            request=request
        )
    except TimeoutError:
        raise HTTPException(status_code=408, detail="Timeout en transformación")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/file/{file_id}")
def get_file_info(file_id: str):
    """Obtiene información básica del archivo"""
    try:
        return file_controller.get_file_info(file_id)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.delete("/file/{file_id}")
def delete_file(file_id: str):
    """Elimina un archivo del sistema"""
    try:
        return file_controller.delete_file(file_id)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.get("/files")
def list_files():
    """Lista todos los archivos cargados"""
    try:
        return file_controller.list_all_files()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/columns/{file_id}")
def get_columns(file_id: str, sheet_name: Optional[str] = Query(None)):
    """Obtiene columnas con soporte para hojas específicas"""
    try:
        result = execute_with_timeout(
            file_controller.get_columns,
            timeout_seconds=45,
            file_id=file_id,
            sheet_name=sheet_name
        )
        
        # Agregar información de hojas si es Excel
        try:
            file_info = file_controller.get_file_info(file_id)
            if file_info and file_info.get("is_excel", False):
                result["sheet_info"] = {
                    "available_sheets": file_info.get("sheets", []),
                    "default_sheet": file_info.get("default_sheet"),
                    "selected_sheet": sheet_name,
                    "is_excel": True
                }
        except e:
            pass
        
        return result
        
    except TimeoutError:
        raise HTTPException(status_code=408, detail="Timeout obteniendo columnas")
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

# ========== ENDPOINTS DE CRUCE ==========

@router.post("/cross")
def cross_files(request: FileCrossRequest):
    """Realiza cruce entre dos archivos"""
    try:        
        return execute_with_timeout(
            cross_controller.perform_cross,
            timeout_seconds=EndpointConfig.OPERATION_TIMEOUT * 3,
            request=request
        )
    except TimeoutError:
        raise HTTPException(status_code=408, detail="Timeout en operación de cruce")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

cross_handler_instance = CrossService()

@router.post("/cross-download")
def cross_files_download(request: FileCrossRequest):
    """Realiza cruce y descarga resultado como CSV"""
    try:        
        result = execute_with_timeout(
            cross_handler_instance.perform_cross_for_streaming,
            timeout_seconds=600,
            request=request
        )
        
        if not result["success"]:
            raise HTTPException(status_code=400, detail=result.get("error", "Error en cruce"))
        
        return result["streaming_response"]
        
    except TimeoutError:
        raise HTTPException(status_code=408, detail="Timeout en cruce con descarga")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Para endpoint de cross
def _validate_file_id(file_id: str) -> dict:
    """Valida y obtiene información del archivo"""
    if not file_id or file_id.strip() == "":
        raise HTTPException(status_code=400, detail="File ID no puede estar vacío")
    
    try:
        return file_controller.get_file_info(file_id)
    except ValueError:
        all_files_info = file_controller.list_all_files()
        available_files = [f["file_id"] for f in all_files_info.get("files", [])]
        raise HTTPException(
            status_code=404, 
            detail=f"Archivo no encontrado: {file_id}. Disponibles: {available_files[:5]}"
        )
    
def _try_load_file_to_duckdb(file_id: str, file_info: dict) -> bool:
    """Intenta cargar el archivo en DuckDB si no está cargado"""
    if not duckdb_service:
        return False
    
    loaded_tables = getattr(duckdb_service, 'loaded_tables', {})
    if file_id in loaded_tables:
        return True
    
    file_path = file_info.get("path")
    if not file_path or not os.path.exists(file_path):
        return False
    
    try:
        result = execute_with_timeout(
            duckdb_service.convert_file_to_parquet,
            timeout_seconds=180,
            file_path=file_path,
            file_id=file_id,
            original_name=file_info["original_name"],
            ext=file_info.get("extension", "xlsx")
        )
        
        if result.get("success"):
            duckdb_service.load_parquet_lazy(file_id, result["parquet_path"])
            return True
    except Exception as conv_error:
        print(f"Error cargando en DuckDB: {conv_error}")
    
    return False

def _get_columns_from_duckdb(file_id: str, sheet_name: str, file_info: dict) -> dict:
    """Obtiene columnas usando DuckDB"""
    if not duckdb_service:
        return {}
    
    try:
        _try_load_file_to_duckdb(file_id, file_info)
        
        columns_info = execute_with_timeout(
            duckdb_service.get_file_columns_for_cross,
            timeout_seconds=30,
            file_id=file_id,
            sheet_name=sheet_name
        )
        
        if columns_info.get("success"):
            return {
                "success": True,
                "file_id": file_id,
                "columns": columns_info["columns"],
                "total_columns": len(columns_info["columns"]),
                "sheet_name": sheet_name,
                "file_name": file_info["original_name"],
                "method": "duckdb_ultra_fast"
            }
    except Exception as duckdb_error:
        print(f"Error con DuckDB: {duckdb_error}")
    
    return {}


def _get_columns_from_fallback(file_id: str, sheet_name: str, file_info: dict) -> dict:
    """Obtiene columnas usando el método fallback"""
    existing_columns = execute_with_timeout(
        file_controller.get_columns,
        timeout_seconds=45,
        file_id=file_id,
        sheet_name=sheet_name
    )
    
    columns = existing_columns.get("columns", [])
    if not columns:
        raise HTTPException(status_code=404, detail="No se encontraron columnas en el archivo")
    
    response = {
        "success": True,
        "file_id": file_id,
        "columns": columns,
        "total_columns": len(columns),
        "sheet_name": sheet_name,
        "file_name": file_info["original_name"],
        "method": "fallback_file_controller"
    }
    
    if file_info:
        response["file_info"] = {
            "original_name": file_info.get("original_name", ""),
            "is_excel": file_info.get("is_excel", False),
            "available_sheets": file_info.get("sheets", []),
            "default_sheet": file_info.get("default_sheet"),
            "selected_sheet": sheet_name,
            "total_rows": file_info.get("total_rows", 0)
        }
    
    return response

@router.get("/cross/columns/{file_id}")
def get_columns_for_cross(
    file_id: str, 
    sheet_name: str = Query(None)
):
    """Obtiene columnas de un archivo para realizar cruces"""
    try:
        file_info = _validate_file_id(file_id)
        
        # ESTRATEGIA 1: Intentar con DuckDB
        duckdb_result = _get_columns_from_duckdb(file_id, sheet_name, file_info)
        if duckdb_result:
            return duckdb_result
        
        # ESTRATEGIA 2: Fallback
        try:
            return _get_columns_from_fallback(file_id, sheet_name, file_info)
        except Exception as fallback_error:
            raise HTTPException(
                status_code=500,
                detail=f"Todos los métodos fallaron: {str(fallback_error)}"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")


# ========== ENDPOINTS DE EXPORTACIÓN ==========

@router.post("/export", response_model=ExportResponse)
def export_data(request: ExportRequest):
    """Exporta datos procesados"""
    try:
        result = execute_with_timeout(
            file_controller.export_processed_data,
            timeout_seconds=300,
            request=request
        )
        return ExportResponse(
            message=result["message"],
            filename=result["filename"],
            file_path=result["file_path"],
            rows_exported=result["rows_exported"],
            format=result["format"]
        )
    except TimeoutError:
        raise HTTPException(status_code=408, detail="Timeout en exportación")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/download/{filename}")
def download_exported_file(filename: str):
    """Descarga un archivo exportado"""
    try:
        export_info = ExportService.get_export_info(filename)
        return FileResponse(
            path=export_info["file_path"],
            filename=filename,
            media_type='application/octet-stream'
        )
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Archivo no encontrado")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/exports")
def list_exported_files():
    """Lista archivos exportados disponibles"""
    try:
        export_dir = ExportService.EXPORT_DIR
        if not os.path.exists(export_dir):
            return {"files": []}
        
        files = []
        for filename in os.listdir(export_dir):
            try:
                file_info = ExportService.get_export_info(filename)
                files.append(file_info)
            except Exception as e:
                continue
        
        return {"files": files, "total": len(files)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/cleanup-exports")
def cleanup_old_exports(days_old: int = Query(7, ge=1, le=365)):
    """Limpia archivos de exportación antiguos"""
    try:
        return file_controller.cleanup_exports(days_old)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# ========== ENDPOINTS DE ELIMINACIÓN ==========

@router.delete("/rows", response_model=DeleteResponse)
def delete_rows(request: DeleteRowsRequest):
    """Elimina filas específicas por índices"""
    try:
        result = execute_with_timeout(
            file_controller.delete_specific_rows,
            timeout_seconds=120,
            request=request
        )
        return DeleteResponse(
            message=result["message"],
            rows_deleted=result["rows_deleted"],
            remaining_rows=result["remaining_rows"]
        )
    except TimeoutError:
        raise HTTPException(status_code=408, detail="Timeout eliminando filas")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/rows/filter", response_model=DeleteResponse)
def delete_rows_by_filter(request: DeleteRowsByFilterRequest):
    """Elimina filas que cumplan con filtros específicos"""
    try:
        result = execute_with_timeout(
            file_controller.delete_rows_by_filter,
            timeout_seconds=120,
            request=request
        )
        return DeleteResponse(
            message=result["message"],
            rows_deleted=result["rows_deleted"],
            remaining_rows=result["remaining_rows"]
        )
    except TimeoutError:
        raise HTTPException(status_code=408, detail="Timeout eliminando por filtro")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/rows/bulk", response_model=DeleteResponse)
def bulk_delete(request: BulkDeleteRequest):
    """Eliminación masiva con confirmación obligatoria"""
    try:
        result = execute_with_timeout(
            file_controller.bulk_delete_operation,
            timeout_seconds=180,
            request=request
        )
        return DeleteResponse(
            message=result["message"],
            rows_deleted=result["rows_deleted"],
            remaining_rows=result["remaining_rows"]
        )
    except TimeoutError:
        raise HTTPException(status_code=408, detail="Timeout en eliminación masiva")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/duplicates/{file_id}")
def remove_duplicates(
    file_id: str,
    columns: Optional[List[str]] = Query(None),
    keep: str = Query('first', regex='^(first|last|False)$'),
    sheet_name: Optional[str] = None
):
    """Elimina filas duplicadas"""
    try:
        result = execute_with_timeout(
            file_controller.delete_duplicates,
            timeout_seconds=120,
            file_id=file_id,
            columns=columns,
            keep=keep,
            sheet_name=sheet_name
        )
        return DeleteResponse(
            message=result["message"],
            rows_deleted=result["rows_deleted"],
            remaining_rows=result["remaining_rows"]
        )
    except TimeoutError:
        raise HTTPException(status_code=408, detail="Timeout eliminando duplicados")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

