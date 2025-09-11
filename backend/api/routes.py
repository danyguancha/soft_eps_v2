# api/routes.py - VERSIÓN SEGURA CON FALLBACKS Y MANEJO ROBUSTO
import os
import traceback
import threading
import time
from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from typing import Optional, List
from fastapi.responses import FileResponse

from models.schemas import (
    BulkDeleteRequest, DeleteResponse, DeleteRowsByFilterRequest, DeleteRowsRequest, 
    ExportRequest, ExportResponse, FileCrossRequest, FileUploadResponse, DataRequest, TransformRequest, 
    AIRequest
)
from controllers import file_controller, ai_controller
from services.cross_service import CrossService
from services.export_service import ExportService
from controllers.cross_controller import cross_controller

# ✅ IMPORTAR WRAPPER SEGURO DE DUCKDB
try:
    from services.duckdb_service_wrapper import safe_duckdb_service as duckdb_service
    
except ImportError:
    try:
        from services.duckdb_service.duckdb_service import duckdb_service
    except ImportError:
        duckdb_service = None

router = APIRouter()

# ========== CONFIGURACIÓN Y UTILIDADES ==========

class EndpointConfig:
    """Configuración para endpoints seguros"""
    OPERATION_TIMEOUT = 60  # segundos
    MAX_RETRIES = 3
    RETRY_DELAY = 2  # segundos
    FALLBACK_ENABLED = True

def safe_execute_with_fallback(primary_func, fallback_func, *args, **kwargs):
    """Ejecuta función primaria con fallback automático"""
    try:
        return primary_func(*args, **kwargs)
    except Exception as e:
        if EndpointConfig.FALLBACK_ENABLED and fallback_func:
            return fallback_func(*args, **kwargs)
        raise e

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
    """Carga archivo con detección completa de hojas Excel - VERSIÓN SEGURA"""
    try:        
        result = await execute_with_timeout(
            file_controller.upload_file, 
            timeout_seconds=300,  # 5 minutos para uploads grandes
            file=file
        )
        
        # Validar que todas las propiedades estén presentes
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
        raise HTTPException(
            status_code=408, 
            detail="Upload timeout - El archivo es muy grande o la conexión es lenta"
        )
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/data")
def get_data(request: DataRequest):
    """Obtiene datos con filtros, ordenamiento y paginación - VERSIÓN SEGURA"""
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
    """Aplica transformaciones a los datos - VERSIÓN SEGURA"""
    try:
        return execute_with_timeout(
            file_controller.transform_data,
            timeout_seconds=EndpointConfig.OPERATION_TIMEOUT * 2,  # Más tiempo para transformaciones
            request=request
        )
    except TimeoutError:
        raise HTTPException(status_code=408, detail="Timeout en transformación")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/file/{file_id}")
def get_file_info(file_id: str):
    """Obtiene información básica del archivo - VERSIÓN SEGURA"""
    try:
        return file_controller.get_file_info(file_id)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.delete("/file/{file_id}")
def delete_file(file_id: str):
    """Elimina un archivo del sistema - VERSIÓN SEGURA"""
    try:
        return file_controller.delete_file(file_id)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.post("/cross")
def cross_files(request: FileCrossRequest):
    """Realiza cruce entre dos archivos - VERSIÓN SEGURA"""
    try:        
        return execute_with_timeout(
            cross_controller.perform_cross,
            timeout_seconds=EndpointConfig.OPERATION_TIMEOUT * 3,  # Más tiempo para cruces
            request=request
        )
        
    except TimeoutError:
        raise HTTPException(status_code=408, detail="Timeout en operación de cruce")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/cross/columns/{file_id}")
async def get_columns_for_cross(
    file_id: str, 
    sheet_name: str = Query(None, description="Nombre de la hoja (para Excel)")
):
    """Obtiene columnas de un archivo para realizar cruces - VERSIÓN ULTRA-SEGURA"""
    try:        
        # Validar formato de file_id
        if not file_id or file_id.strip() == "":
            raise HTTPException(status_code=400, detail="File ID no puede estar vacío")
        
        # Obtener información del archivo
        try:
            file_info = file_controller.get_file_info(file_id)
        except ValueError as e:
            all_files_info = file_controller.list_all_files()
            available_files = [f["file_id"] for f in all_files_info.get("files", [])]
            raise HTTPException(
                status_code=404, 
                detail=f"Archivo no encontrado: {file_id}. Disponibles: {available_files[:5]}"
            )
        
        # ESTRATEGIA 1: Intentar con DuckDB Service (si está disponible)
        if duckdb_service:
            try:
                # Verificar si está cargado en DuckDB
                loaded_tables = getattr(duckdb_service, 'loaded_tables', {})
                
                if file_id not in loaded_tables:
                    
                    file_path = file_info.get("path")
                    if file_path and os.path.exists(file_path):
                        try:
                            # Conversión con timeout
                            result = execute_with_timeout(
                                duckdb_service.convert_file_to_parquet,
                                timeout_seconds=180,  # 3 minutos para conversión
                                file_path=file_path,
                                file_id=file_id,
                                original_name=file_info["original_name"],
                                ext=file_info.get("extension", "xlsx")
                            )
                            
                            if result.get("success"):
                                duckdb_service.load_parquet_lazy(file_id, result["parquet_path"])
                            else:
                                raise Exception(f"Error en conversión: {result.get('error')}")
                        except Exception as conv_error:
                            print(f"⚠️ Error cargando en DuckDB: {conv_error}")
                            # Continuar con fallback
                    else:
                        print(f"⚠️ Archivo físico no encontrado: {file_path}")
                        # Continuar con fallback
                
                # Intentar obtener columnas con DuckDB
                try:
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
                    else:
                        print(f"⚠️ DuckDB falló: {columns_info.get('error')}")
                        # Continuar con fallback
                        
                except Exception as duckdb_error:
                    print(f"⚠️ Error con DuckDB: {duckdb_error}")
                    # Continuar con fallback
                    
            except Exception as e:
                print(f"⚠️ Error general con DuckDB: {e}")
                # Continuar con fallback
        
        # ESTRATEGIA 2: Fallback al sistema existente (siempre funciona)
        
        try:
            existing_columns = execute_with_timeout(
                file_controller.get_columns,
                timeout_seconds=45,
                file_id=file_id,
                sheet_name=sheet_name
            )
            
            columns = existing_columns.get("columns", [])
            if not columns:
                raise Exception("No se encontraron columnas")            
            response = {
                "success": True,
                "file_id": file_id,
                "columns": columns,
                "total_columns": len(columns),
                "sheet_name": sheet_name,
                "file_name": file_info["original_name"],
                "method": "fallback_file_controller"
            }
            
            # Agregar información adicional si está disponible
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
            
        except Exception as fallback_error:
            raise HTTPException(
                status_code=500,
                detail=f"Todos los métodos fallaron: {str(fallback_error)}"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error interno del servidor: {str(e)}"
        )

# ========== ENDPOINTS DE DEBUG Y DIAGNÓSTICO ==========

@router.get("/debug/files")
async def debug_files():
    """Endpoint de debug para ver archivos disponibles - VERSIÓN COMPLETA"""
    try:
        # Información del file_controller
        all_files = file_controller.list_all_files()
        
        # Información de DuckDB (si está disponible)
        duckdb_info = {
            "available": duckdb_service is not None,
            "loaded_tables": [],
            "detailed_info": {},
            "service_type": "none"
        }
        
        if duckdb_service:
            try:
                loaded_tables = list(getattr(duckdb_service, 'loaded_tables', {}).keys())
                duckdb_info.update({
                    "loaded_tables": loaded_tables,
                    "service_type": "wrapper" if hasattr(duckdb_service, '_service') else "direct"
                })
                
                # Información detallada de cada tabla
                for file_id in loaded_tables:
                    try:
                        stats = duckdb_service.get_file_stats(file_id)
                        duckdb_info["detailed_info"][file_id] = stats
                    except Exception as e:
                        duckdb_info["detailed_info"][file_id] = {"error": str(e)}
                        
            except Exception as e:
                duckdb_info["error"] = str(e)
        
        # Análisis de archivos
        files_analysis = {
            "in_file_controller": all_files.get("total", 0),
            "in_duckdb": len(duckdb_info["loaded_tables"]),
            "missing_in_duckdb": [],
            "file_controller_files": all_files.get("files", [])
        }
        
        # Encontrar archivos que faltan en DuckDB
        for file_info in all_files.get("files", []):
            file_id = file_info.get("file_id")
            if file_id and file_id not in duckdb_info["loaded_tables"]:
                files_analysis["missing_in_duckdb"].append({
                    "file_id": file_id,
                    "name": file_info.get("original_name", "Unknown"),
                    "exists": file_info.get("file_exists", False)
                })
        
        return {
            "timestamp": time.time(),
            "file_controller": all_files,
            "duckdb": duckdb_info,
            "analysis": files_analysis,
            "system_status": {
                "duckdb_available": duckdb_service is not None,
                "fallback_available": True,
                "timeout_config": EndpointConfig.OPERATION_TIMEOUT
            }
        }
        
    except Exception as e:
        traceback.print_exc()
        return {
            "error": str(e), 
            "traceback": traceback.format_exc(),
            "timestamp": time.time()
        }

@router.get("/debug/health")
async def health_check():
    """Endpoint de salud del sistema"""
    try:
        health_status = {
            "status": "healthy",
            "timestamp": time.time(),
            "components": {}
        }
        
        # Check file_controller
        try:
            file_controller.list_all_files()
            health_status["components"]["file_controller"] = "healthy"
        except Exception as e:
            health_status["components"]["file_controller"] = f"error: {str(e)}"
            health_status["status"] = "degraded"
        
        # Check DuckDB
        if duckdb_service:
            try:
                # Test básico
                if hasattr(duckdb_service, '_service'):
                    # Es wrapper
                    test_result = duckdb_service._service is not None
                else:
                    # Es directo
                    test_result = hasattr(duckdb_service, 'conn')
                
                health_status["components"]["duckdb"] = "healthy" if test_result else "unavailable"
            except Exception as e:
                health_status["components"]["duckdb"] = f"error: {str(e)}"
                health_status["status"] = "degraded"
        else:
            health_status["components"]["duckdb"] = "not_loaded"
        
        return health_status
        
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "timestamp": time.time()
        }

# ========== ENDPOINTS DE EXCEL Y HOJAS ==========

@router.get("/excel/diagnostic/{file_id}")
def excel_diagnostic(file_id: str):
    """Diagnóstico completo de archivo Excel - VERSIÓN SEGURA"""
    try:
        file_info = file_controller.get_file_info(file_id)
        
        if not file_info:
            raise HTTPException(status_code=404, detail="Archivo no encontrado")
        
        file_path = file_info.get("path")
        
        diagnostic = {
            "file_id": file_id,
            "original_name": file_info.get("original_name", ""),
            "file_exists": os.path.exists(file_path) if file_path else False,
            "file_size_mb": os.path.getsize(file_path) / 1024 / 1024 if file_path and os.path.exists(file_path) else 0,
            "extension": file_info.get("extension", ""),
            "stored_sheets": file_info.get("sheets", []),
            "stored_default_sheet": file_info.get("default_sheet"),
            "is_excel_flag": file_info.get("is_excel", False),
            "has_sheets_flag": file_info.get("has_sheets", False),
            "sheet_count": file_info.get("sheet_count", 0)
        }
        
        # Intentar redetección en tiempo real (con timeout)
        if file_path and os.path.exists(file_path) and duckdb_service:
            try:
                live_detection = execute_with_timeout(
                    duckdb_service.get_excel_sheets,
                    timeout_seconds=30,
                    file_path=file_path
                )
                diagnostic["live_detection"] = live_detection
            except Exception as e:
                diagnostic["live_detection"] = {"error": str(e)}
        
        return diagnostic
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en diagnóstico: {str(e)}")

@router.get("/sheets/{file_id}")
def get_file_sheets(file_id: str):
    """Obtiene hojas disponibles de un archivo Excel ya subido - VERSIÓN SEGURA"""
    try:        
        file_info = file_controller.get_file_info(file_id)
        
        if not file_info:
            raise HTTPException(status_code=404, detail=f"Archivo no encontrado: {file_id}")
        
        # Verificar si es Excel
        is_excel = (
            file_info.get("is_excel", False) or 
            file_info.get("extension") in ["xlsx", "xls"] or
            len(file_info.get("sheets", [])) > 0
        )
        
        if not is_excel:
            return {
                "success": True,
                "file_id": file_id,
                "is_excel": False,
                "sheets": [],
                "default_sheet": None,
                "message": "Este archivo no es Excel, no tiene hojas"
            }
        
        sheets = file_info.get("sheets", [])
        default_sheet = file_info.get("default_sheet")
        
        return {
            "success": True,
            "file_id": file_id,
            "is_excel": True,
            "sheets": sheets,
            "default_sheet": default_sheet,
            "has_multiple_sheets": len(sheets) > 1,
            "sheet_count": len(sheets),
            "original_name": file_info.get("original_name", "")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo hojas: {str(e)}")

@router.post("/sheets/{file_id}/redetect")
def redetect_file_sheets(file_id: str):
    """Redetecta hojas de un archivo Excel - VERSIÓN SEGURA"""
    try:        
        file_info = file_controller.get_file_info(file_id)
        if not file_info:
            raise HTTPException(status_code=404, detail=f"Archivo no encontrado: {file_id}")
        
        file_path = file_info.get("path")
        if not file_path or not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="Archivo físico no encontrado")
        
        if not duckdb_service:
            raise HTTPException(status_code=503, detail="DuckDB Service no disponible")
        
        # Redetectar con timeout
        sheet_info = execute_with_timeout(
            duckdb_service.get_excel_sheets,
            timeout_seconds=60,
            file_path=file_path
        )
        
        if sheet_info.get("success"):            
            return {
                "success": True,
                "file_id": file_id,
                "sheets": sheet_info.get("sheets", []),
                "default_sheet": sheet_info.get("default_sheet"),
                "method": sheet_info.get("method", "unknown"),
                "detection_time": sheet_info.get("processing_time", 0),
                "message": "Hojas redetectadas exitosamente"
            }
        else:
            return {
                "success": False,
                "file_id": file_id,
                "error": sheet_info.get("error", "No se pudieron detectar hojas"),
                "sheets": ["Sheet1"],  # Fallback
                "default_sheet": "Sheet1"
            }
            
    except HTTPException:
        raise
    except TimeoutError:
        raise HTTPException(status_code=408, detail="Timeout redetectando hojas")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

# ========== RESTO DE ENDPOINTS (CON TIMEOUTS) ==========

@router.post("/ai")
def ask_ai(request: AIRequest):
    """Consulta al asistente IA - VERSIÓN SEGURA"""
    try:
        return execute_with_timeout(
            ai_controller.ask_ai,
            timeout_seconds=120,  # 2 minutos para IA
            request=request
        )
    except TimeoutError:
        raise HTTPException(status_code=408, detail="Timeout en consulta IA")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/files")
def list_files():
    """Lista todos los archivos cargados - VERSIÓN SEGURA"""
    try:
        return file_controller.list_all_files()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/columns/{file_id}")
def get_columns(file_id: str, sheet_name: Optional[str] = Query(None, description="Hoja específica de Excel")):
    """Obtiene columnas con soporte para hojas específicas - VERSIÓN SEGURA"""
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
        except:
            pass  # No es crítico si falla
        
        return result
        
    except TimeoutError:
        raise HTTPException(status_code=408, detail="Timeout obteniendo columnas")
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

# ========== ENDPOINTS DE EXPORTACIÓN (CON TIMEOUTS) ==========

@router.post("/export", response_model=ExportResponse)
def export_data(request: ExportRequest):
    """Exporta datos procesados - VERSIÓN SEGURA"""
    try:
        result = execute_with_timeout(
            file_controller.export_processed_data,
            timeout_seconds=300,  # 5 minutos para exportación
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
    """Descarga un archivo exportado - VERSIÓN SEGURA"""
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

# ========== ENDPOINTS DE ELIMINACIÓN (CON TIMEOUTS) ==========

@router.delete("/rows", response_model=DeleteResponse)
def delete_rows(request: DeleteRowsRequest):
    """Elimina filas específicas por índices - VERSIÓN SEGURA"""
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
    """Elimina filas que cumplan con filtros específicos - VERSIÓN SEGURA"""
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
    """Eliminación masiva con confirmación obligatoria - VERSIÓN SEGURA"""
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
    """Elimina filas duplicadas - VERSIÓN SEGURA"""
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

# ========== ENDPOINTS DE UTILIDADES ==========

@router.post("/cleanup-exports")
def cleanup_old_exports(days_old: int = Query(7, ge=1, le=365)):
    """Limpia archivos de exportación antiguos"""
    try:
        return file_controller.cleanup_exports(days_old)
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
            except:
                continue
        
        return {"files": files, "total": len(files)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# ========== ENDPOINTS DE CRUCE AVANZADO ==========

cross_handler_instance = CrossService()

@router.post("/cross-download")
def cross_files_download(request: FileCrossRequest):
    """Realiza cruce y descarga resultado como CSV - VERSIÓN SEGURA"""
    try:        
        result = execute_with_timeout(
            cross_handler_instance.perform_cross_for_streaming,
            timeout_seconds=600,  # 10 minutos para cruces grandes
            request=request
        )
        
        if not result["success"]:
            raise HTTPException(status_code=400, detail=result.get("error", "Error en cruce"))
        
        return result["streaming_response"]
        
    except TimeoutError:
        raise HTTPException(status_code=408, detail="Timeout en cruce con descarga")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# ========== ENDPOINTS ESPECÍFICOS DE EXCEL ==========

@router.get("/columns/{file_id}/sheet/{sheet_name}")
def get_columns_from_specific_sheet(file_id: str, sheet_name: str):
    """Obtiene columnas de una hoja específica de Excel - VERSIÓN SEGURA"""
    try:        
        file_info = file_controller.get_file_info(file_id)
        if not file_info:
            raise HTTPException(status_code=404, detail="Archivo no encontrado")
        
        file_path = file_info.get("path")
        if not file_path or not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="Archivo físico no encontrado")
        
        if not file_info.get("is_excel", False):
            raise HTTPException(status_code=400, detail="Este endpoint es solo para archivos Excel")
        
        available_sheets = file_info.get("sheets", [])
        if sheet_name not in available_sheets:
            raise HTTPException(
                status_code=400, 
                detail=f"La hoja '{sheet_name}' no existe. Hojas disponibles: {available_sheets}"
            )
        
        if not duckdb_service:
            raise HTTPException(status_code=503, detail="DuckDB Service no disponible")
        
        # Obtener columnas con timeout
        result = execute_with_timeout(
            duckdb_service.get_columns_from_sheet,
            timeout_seconds=60,
            file_path=file_path,
            sheet_name=sheet_name
        )
        
        if result["success"]:            
            return {
                "success": True,
                "file_id": file_id,
                "sheet_name": sheet_name,
                "columns": result["columns"],
                "total_columns": len(result["columns"]),
                "header_row_detected": result.get("header_row_detected", 0),
                "available_sheets": available_sheets,
                "method": "specific_sheet_analysis"
            }
        else:
            raise HTTPException(status_code=500, detail=result["error"])
            
    except HTTPException:
        raise
    except TimeoutError:
        raise HTTPException(status_code=408, detail="Timeout obteniendo columnas de hoja")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.get("/preview/{file_id}/sheet/{sheet_name}")
def get_sheet_preview(
    file_id: str, 
    sheet_name: str,
    max_rows: int = Query(5, ge=1, le=20, description="Máximo número de filas para preview")
):
    """Preview de datos de una hoja específica - VERSIÓN SEGURA"""
    try:
        file_info = file_controller.get_file_info(file_id)
        if not file_info:
            raise HTTPException(status_code=404, detail="Archivo no encontrado")
        
        file_path = file_info.get("path")
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="Archivo físico no encontrado")
        
        if not duckdb_service:
            raise HTTPException(status_code=503, detail="DuckDB Service no disponible")
        
        # Obtener preview con timeout
        result = execute_with_timeout(
            duckdb_service.get_sheet_preview,
            timeout_seconds=45,
            file_path=file_path,
            sheet_name=sheet_name,
            max_rows=max_rows
        )
        
        if result["success"]:
            return {
                "success": True,
                "file_id": file_id,
                "sheet_name": sheet_name,
                "columns": result["columns"],
                "preview_data": result["preview_data"],
                "header_row": result["header_row"],
                "sample_rows": result["sample_rows"]
            }
        else:
            raise HTTPException(status_code=500, detail=result["error"])
            
    except HTTPException:
        raise
    except TimeoutError:
        raise HTTPException(status_code=408, detail="Timeout obteniendo preview")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
