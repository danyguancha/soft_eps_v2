"""
Controlador para exportación de datos procesados
"""
from models.schemas import ExportRequest
from services.export_service import ExportService
from .base_controller import storage, data_cache, file_services, validate_file_exists, get_cache_key

def export_processed_data(request: ExportRequest) -> dict:
    """Exporta datos procesados según filtros y transformaciones"""
    file_info = validate_file_exists(request.file_id)
    
    # Obtener DataFrame desde cache
    sheet_name = request.sheet_name or file_info.get("default_sheet")
    cache_key = get_cache_key(request.file_id, sheet_name)
    
    df = _get_dataframe_for_export(file_info, cache_key, sheet_name)
    
    # Exportar usando el servicio de exportación
    export_result = ExportService.export_data(df, request)
    
    return {
        "message": "Datos exportados exitosamente",
        **export_result
    }

def _get_dataframe_for_export(file_info: dict, cache_key: str, sheet_name: str):
    """Obtiene DataFrame para exportación, cargándolo si no está en cache"""
    if cache_key not in data_cache:
        # Si no está en cache, cargar desde archivo
        service = file_services[file_info["ext"]]
        file_obj = service.load(file_info["path"])
        df = service.get_data(file_obj, sheet_name)
        data_cache[cache_key] = df.copy()
        return df.copy()
    else:
        return data_cache[cache_key].copy()

def get_export_formats() -> dict:
    """Retorna los formatos de exportación disponibles"""
    return {
        "formats": [
            {
                "format": "csv",
                "description": "Comma Separated Values",
                "supports_headers": True,
                "supports_multiple_sheets": False
            },
            {
                "format": "excel", 
                "description": "Microsoft Excel",
                "supports_headers": True,
                "supports_multiple_sheets": True
            },
            {
                "format": "json",
                "description": "JavaScript Object Notation",
                "supports_headers": False,
                "supports_multiple_sheets": False
            }
        ]
    }

def cleanup_old_exports(days_old: int = 7) -> dict:
    """Limpia archivos de exportación antiguos"""
    return ExportService.cleanup_old_exports(days_old)

def get_export_history() -> dict:
    """Obtiene historial de exportaciones"""
    import os
    from datetime import datetime
    
    export_dir = ExportService.EXPORT_DIR
    if not os.path.exists(export_dir):
        return {"exports": [], "total": 0}
    
    exports = []
    for filename in os.listdir(export_dir):
        try:
            file_info = ExportService.get_export_info(filename)
            exports.append(file_info)
        except:
            continue
    
    # Ordenar por fecha de creación (más recientes primero)
    exports.sort(key=lambda x: x["created_at"], reverse=True)
    
    return {"exports": exports, "total": len(exports)}
