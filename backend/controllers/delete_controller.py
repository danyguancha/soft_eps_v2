"""
Controlador para eliminación de filas y registros
"""
from models.schemas import DeleteRowsRequest, DeleteRowsByFilterRequest, BulkDeleteRequest
from services.delete_service import DeleteService
from .base_controller import storage, data_cache, validate_file_exists, get_cache_key

def delete_specific_rows(request: DeleteRowsRequest) -> dict:
    """Elimina filas específicas por índices"""
    file_info = validate_file_exists(request.file_id)
    
    # Obtener DataFrame desde cache
    sheet_name = request.sheet_name or file_info.get("default_sheet")
    cache_key = get_cache_key(request.file_id, sheet_name)
    
    if cache_key not in data_cache:
        raise ValueError("Datos no encontrados en cache")
    
    df = data_cache[cache_key]
    
    # Eliminar filas
    result = DeleteService.delete_rows_by_indices(df, request.row_indices)
    
    # Actualizar cache con datos modificados
    data_cache[cache_key] = result["dataframe"]
    
    # Actualizar información del archivo
    storage[request.file_id]["total_rows"] = result["remaining_count"]
    
    return {
        "message": "Filas eliminadas exitosamente",
        "rows_deleted": result["deleted_count"],
        "remaining_rows": result["remaining_count"],
        "invalid_indices": result.get("invalid_indices", [])
    }

def delete_rows_by_filter(request: DeleteRowsByFilterRequest) -> dict:
    """Elimina filas que cumplan con filtros específicos"""
    file_info = validate_file_exists(request.file_id)
    
    sheet_name = request.sheet_name or file_info.get("default_sheet")
    cache_key = get_cache_key(request.file_id, sheet_name)
    
    if cache_key not in data_cache:
        raise ValueError("Datos no encontrados en cache")
    
    df = data_cache[cache_key]
    
    # Eliminar filas por filtro
    result = DeleteService.delete_rows_by_filters(df, request.filters)
    
    # Actualizar cache
    data_cache[cache_key] = result["dataframe"]
    
    # Actualizar información del archivo
    storage[request.file_id]["total_rows"] = result["remaining_count"]
    
    return {
        "message": "Filas eliminadas por filtro exitosamente",
        "rows_deleted": result["deleted_count"],
        "remaining_rows": result["remaining_count"],
        "deleted_indices": result["deleted_indices"]
    }

def preview_delete_operation(file_id: str, filters: list, sheet_name: str = None) -> dict:
    """Previsualiza qué filas serían eliminadas"""
    file_info = validate_file_exists(file_id)
    
    sheet_name = sheet_name or file_info.get("default_sheet")
    cache_key = get_cache_key(file_id, sheet_name)
    
    if cache_key not in data_cache:
        raise ValueError("Datos no encontrados en cache")
    
    df = data_cache[cache_key]
    
    return DeleteService.preview_delete_by_filters(df, filters)

def bulk_delete_operation(request: BulkDeleteRequest) -> dict:
    """Operación de eliminación masiva con confirmación"""
    if not request.confirm_delete:
        raise ValueError("Operación de eliminación masiva requiere confirmación explícita")
    
    file_info = validate_file_exists(request.file_id)
    
    sheet_name = request.sheet_name or file_info.get("default_sheet")
    cache_key = get_cache_key(request.file_id, sheet_name)
    
    if cache_key not in data_cache:
        raise ValueError("Datos no encontrados en cache")
    
    df = data_cache[cache_key]
    
    # Verificar que la operación no elimine más del 90% de los datos
    preview = DeleteService.preview_delete_by_filters(df, request.conditions)
    if preview["rows_to_delete_count"] > len(df) * 0.9:
        raise ValueError("Operación eliminaría más del 90% de los datos. Verifique los filtros.")
    
    # Proceder con la eliminación
    result = DeleteService.delete_rows_by_filters(df, request.conditions)
    
    # Actualizar cache
    data_cache[cache_key] = result["dataframe"]
    
    # Actualizar información del archivo
    storage[request.file_id]["total_rows"] = result["remaining_count"]
    
    return {
        "message": "Eliminación masiva completada exitosamente",
        "rows_deleted": result["deleted_count"],
        "remaining_rows": result["remaining_count"]
    }

def delete_duplicates(file_id: str, columns: list = None, keep: str = 'first', sheet_name: str = None) -> dict:
    """Elimina filas duplicadas"""
    file_info = validate_file_exists(file_id)
    
    sheet_name = sheet_name or file_info.get("default_sheet")
    cache_key = get_cache_key(file_id, sheet_name)
    
    if cache_key not in data_cache:
        raise ValueError("Datos no encontrados en cache")
    
    df = data_cache[cache_key]
    
    result = DeleteService.delete_duplicates(df, columns, keep)
    
    # Actualizar cache
    data_cache[cache_key] = result["dataframe"]
    
    # Actualizar información del archivo
    storage[file_id]["total_rows"] = result["remaining_count"]
    
    return {
        "message": "Duplicados eliminados exitosamente",
        "rows_deleted": result["deleted_count"],
        "remaining_rows": result["remaining_count"],
        "columns_checked": result["columns_checked"]
    }
