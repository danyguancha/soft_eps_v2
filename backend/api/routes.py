# routers/file_router.py
from asyncio.log import logger
import os
import traceback
from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from typing import Optional, List
from fastapi.responses import FileResponse

from models.schemas import (
    BulkDeleteRequest, CrossPreviewRequest, DeleteResponse, DeleteRowsByFilterRequest, DeleteRowsRequest, 
    ExportRequest, ExportResponse, FileCrossRequest, FileUploadResponse, DataRequest, TransformRequest, 
    AIRequest, FilterCondition
)
from controllers import file_controller, cross_controller, ai_controller
from services.export_service import ExportService

router = APIRouter()

@router.post("/upload", response_model=FileUploadResponse)
async def upload_file(file: UploadFile = File(...)):
    """Carga un archivo Excel o CSV"""
    try:
        result = await file_controller.upload_file(file)
        return FileUploadResponse(
            message="Archivo cargado exitosamente",
            file_id=result["file_id"],
            columns=result["columns"],
            sheets=result["sheets"],
            total_rows=result["total_rows"]
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/data")
def get_data(request: DataRequest):
    """Obtiene datos con filtros, ordenamiento y paginación"""
    try:
        return file_controller.get_data(request)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/transform")
def transform_data(request: TransformRequest):
    """Aplica transformaciones a los datos"""
    try:
        return file_controller.transform_data(request)
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

@router.post("/cross")
def cross_files(request: FileCrossRequest):
    """Realiza cruce entre dos archivos"""
    try:
        print(f"📥 Request recibido correctamente:")
        print(f"   - cross_type: {request.cross_type}")
        print(f"   - columns_to_include: {request.columns_to_include}")
        
        return cross_controller.perform_cross(request)
    except Exception as e:
        print(f"❌ Error en endpoint: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/cross/columns/{file_id}")
def get_columns_for_cross(file_id: str, sheet_name: str = None):
    """Obtiene las columnas de un archivo para configurar el cruce"""
    try:
        return cross_controller.get_file_columns_for_cross(file_id, sheet_name)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/ai")
def ask_ai(request: AIRequest):
    """Consulta al asistente IA"""
    try:
        return ai_controller.ask_ai(request)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/files")
def list_files():
    """Lista todos los archivos cargados"""
    try:
        return file_controller.list_all_files()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/columns/{file_id}")
def get_columns(file_id: str, sheet_name: Optional[str] = None):
    """Obtiene las columnas de un archivo específico"""
    try:
        return file_controller.get_columns(file_id, sheet_name)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))
    
@router.post("/export", response_model=ExportResponse)
def export_data(request: ExportRequest):
    """Exporta datos procesados con filtros y transformaciones aplicadas"""
    try:
        result = file_controller.export_processed_data(request)
        return ExportResponse(
            message=result["message"],
            filename=result["filename"],
            file_path=result["file_path"],
            rows_exported=result["rows_exported"],
            format=result["format"]
        )
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

@router.delete("/rows", response_model=DeleteResponse)
def delete_rows(request: DeleteRowsRequest):
    """Elimina filas específicas por índices"""
    try:
        result = file_controller.delete_specific_rows(request)
        return DeleteResponse(
            message=result["message"],
            rows_deleted=result["rows_deleted"],
            remaining_rows=result["remaining_rows"]
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/rows/filter", response_model=DeleteResponse)
def delete_rows_by_filter(request: DeleteRowsByFilterRequest):
    """Elimina filas que cumplan con filtros específicos"""
    try:
        result = file_controller.delete_rows_by_filter(request)
        return DeleteResponse(
            message=result["message"],
            rows_deleted=result["rows_deleted"],
            remaining_rows=result["remaining_rows"]
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/rows/bulk", response_model=DeleteResponse)
def bulk_delete(request: BulkDeleteRequest):
    """Eliminación masiva con confirmación obligatoria"""
    try:
        result = file_controller.bulk_delete_operation(request)
        return DeleteResponse(
            message=result["message"],
            rows_deleted=result["rows_deleted"],
            remaining_rows=result["remaining_rows"]
        )
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
        result = file_controller.delete_duplicates(file_id, columns, keep, sheet_name)
        return DeleteResponse(
            message=result["message"],
            rows_deleted=result["rows_deleted"],
            remaining_rows=result["remaining_rows"]
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

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
