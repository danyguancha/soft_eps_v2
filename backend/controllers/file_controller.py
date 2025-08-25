import uuid
import os
from fastapi import UploadFile
import pandas as pd
from services.csv_service import CSVService
from services.delete_service import DeleteService
from services.excel_service import ExcelService
from services.export_service import ExportService
from services.filter_service import FilterService
from services.sort_service import SortService
from services.pagination_service import PaginationService
from services.transformation_service import TransformationService
from models.schemas import BulkDeleteRequest, DataRequest, DeleteRowsByFilterRequest, DeleteRowsRequest, ExportRequest, TransformRequest

# Servicios de archivo por extensión
file_services = {
    "csv": CSVService(),
    "xlsx": ExcelService(),
    "xls": ExcelService()
}

# Almacenamiento en memoria (en producción usar base de datos)
storage = {}
data_cache = {}  # Cache para DataFrames procesados

def ensure_upload_directory():
    """Asegura que el directorio de uploads exista"""
    upload_dir = "uploads"
    if not os.path.exists(upload_dir):
        os.makedirs(upload_dir)
    return upload_dir

def upload_file(file: UploadFile):
    """Procesa la carga de archivo y devuelve metadatos"""
    ext = file.filename.split('.')[-1].lower()
    if ext not in file_services:
        raise ValueError(f"Tipo de archivo no soportado: {ext}")
    
    # Generar ID único para el archivo
    file_id = str(uuid.uuid4())
    upload_dir = ensure_upload_directory()
    file_path = os.path.join(upload_dir, f"{file_id}.{ext}")
    
    # Guardar archivo físicamente
    with open(file_path, "wb") as f:
        f.write(file.file.read())
    
    # Procesar archivo
    service = file_services[ext]
    file_obj = service.load(file_path)
    
    # Obtener metadatos
    sheets = service.get_sheets(file_obj) if ext != "csv" else None
    default_sheet = sheets[0] if sheets else None
    columns = service.get_columns(file_obj, default_sheet)
    
    # Obtener datos para contar filas
    df = service.get_data(file_obj, default_sheet)
    total_rows = len(df)
    
    # Almacenar información del archivo
    storage[file_id] = {
        "path": file_path,
        "ext": ext,
        "original_name": file.filename,
        "sheets": sheets,
        "default_sheet": default_sheet,
        "columns": columns,
        "total_rows": total_rows
    }
    
    # Cachear el DataFrame inicial
    data_cache[f"{file_id}_{default_sheet}"] = df
    
    return {
        "file_id": file_id,
        "columns": columns,
        "sheets": sheets,
        "total_rows": total_rows
    }

def get_data(request: DataRequest):
    """Obtiene datos con filtros, ordenamiento y paginación"""
    file_info = storage.get(request.file_id)
    if not file_info:
        raise ValueError("Archivo no encontrado")
    
    # Determinar hoja a usar
    sheet_name = request.sheet_name or file_info.get("default_sheet")
    cache_key = f"{request.file_id}_{sheet_name}"
    
    # Obtener DataFrame desde cache o cargar
    if cache_key in data_cache:
        df = data_cache[cache_key].copy()
    else:
        service = file_services[file_info["ext"]]
        file_obj = service.load(file_info["path"])
        df = service.get_data(file_obj, sheet_name)
        data_cache[cache_key] = df.copy()
    
    # Aplicar búsqueda global si está presente
    if request.search:
        df = FilterService.apply_search(df, request.search)
    
    # Aplicar filtros
    if request.filters:
        df = FilterService.apply_filters(df, request.filters)
    
    # Aplicar ordenamiento
    if request.sort:
        df = SortService.apply_sort(df, request.sort)
    
    # Aplicar paginación
    return PaginationService.paginate(df, request.page, request.page_size)

def transform_data(request: TransformRequest):
    """Aplica transformación a los datos"""
    file_info = storage.get(request.file_id)
    if not file_info:
        raise ValueError("Archivo no encontrado")
    
    # Obtener DataFrame actual
    cache_key = f"{request.file_id}_{file_info.get('default_sheet')}"
    if cache_key not in data_cache:
        raise ValueError("Datos no encontrados en cache")
    
    df = data_cache[cache_key]
    
    # Aplicar transformación
    transformed_df = TransformationService.apply_transformation(
        df, request.operation, request.params
    )
    
    # Actualizar cache
    data_cache[cache_key] = transformed_df
    
    # Actualizar columnas en storage
    storage[request.file_id]["columns"] = transformed_df.columns.tolist()
    
    return {
        "message": "Transformación aplicada exitosamente",
        "new_columns": transformed_df.columns.tolist()
    }

def get_file_info(file_id: str):
    """Obtiene información básica del archivo"""
    file_info = storage.get(file_id)
    if not file_info:
        raise ValueError("Archivo no encontrado")
    
    return {
        "file_id": file_id,
        "original_name": file_info["original_name"],
        "columns": file_info["columns"],
        "sheets": file_info["sheets"],
        "total_rows": file_info["total_rows"]
    }

def delete_file(file_id: str):
    """Elimina archivo del sistema"""
    file_info = storage.get(file_id)
    if not file_info:
        raise ValueError("Archivo no encontrado")
    
    # Eliminar archivo físico
    try:
        os.remove(file_info["path"])
    except:
        pass
    
    # Limpiar cache
    cache_keys_to_remove = [key for key in data_cache.keys() if key.startswith(file_id)]
    for key in cache_keys_to_remove:
        del data_cache[key]
    
    # Eliminar de storage
    del storage[file_id]
    
    return {"message": "Archivo eliminado exitosamente"}

# Agregar estas funciones al final del archivo file_controller.py

def list_all_files():
    """Lista todos los archivos cargados"""
    files = []
    for file_id, info in storage.items():
        files.append({
            "file_id": file_id,
            "original_name": info["original_name"],
            "total_rows": info["total_rows"],
            "columns_count": len(info["columns"]),
            "sheets": info.get("sheets")
        })
    
    return {"files": files, "total": len(files)}

def get_columns(file_id: str, sheet_name: str = None):
    """Obtiene columnas específicas de un archivo y hoja"""
    file_info = storage.get(file_id)
    if not file_info:
        raise ValueError("Archivo no encontrado")
    
    if sheet_name and sheet_name not in (file_info.get("sheets") or []):
        # Si se especifica una hoja que no existe, cargar esa hoja
        service = file_services[file_info["ext"]]
        file_obj = service.load(file_info["path"])
        try:
            columns = service.get_columns(file_obj, sheet_name)
            return {"columns": columns, "sheet_name": sheet_name}
        except Exception as e:
            raise ValueError(f"Error al acceder a la hoja '{sheet_name}': {str(e)}")
    
    return {
        "columns": file_info["columns"],
        "sheet_name": sheet_name or file_info.get("default_sheet")
    }

def export_filtered_data(file_id: str, request: DataRequest, format: str = "csv"):
    """Exporta datos filtrados a archivo"""
    # Obtener datos filtrados (sin paginación)
    request.page_size = 1000000  # Obtener todos los datos
    result = get_data(request)
    
    df = pd.DataFrame(result.data)
    
    if format.lower() == "csv":
        filename = f"export_{file_id}.csv"
        df.to_csv(filename, index=False)
    elif format.lower() == "excel":
        filename = f"export_{file_id}.xlsx"
        df.to_excel(filename, index=False)
    else:
        raise ValueError("Formato no soportado")
    
    return {"filename": filename, "rows_exported": len(df)}


def export_processed_data(request: ExportRequest):
    """Exporta datos procesados según filtros y transformaciones"""
    file_info = storage.get(request.file_id)
    if not file_info:
        raise ValueError("Archivo no encontrado")
    
    # Obtener DataFrame desde cache
    sheet_name = request.sheet_name or file_info.get("default_sheet")
    cache_key = f"{request.file_id}_{sheet_name}"
    
    if cache_key not in data_cache:
        # Si no está en cache, cargar desde archivo
        service = file_services[file_info["ext"]]
        file_obj = service.load(file_info["path"])
        df = service.get_data(file_obj, sheet_name)
        data_cache[cache_key] = df.copy()
    else:
        df = data_cache[cache_key].copy()
    
    # Exportar usando el servicio de exportación
    export_result = ExportService.export_data(df, request)
    
    return {
        "message": "Datos exportados exitosamente",
        **export_result
    }

def delete_specific_rows(request: DeleteRowsRequest):
    """Elimina filas específicas por índices"""
    file_info = storage.get(request.file_id)
    if not file_info:
        raise ValueError("Archivo no encontrado")
    
    # Obtener DataFrame desde cache
    sheet_name = request.sheet_name or file_info.get("default_sheet")
    cache_key = f"{request.file_id}_{sheet_name}"
    
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

def delete_rows_by_filter(request: DeleteRowsByFilterRequest):
    """Elimina filas que cumplan con filtros específicos"""
    file_info = storage.get(request.file_id)
    if not file_info:
        raise ValueError("Archivo no encontrado")
    
    sheet_name = request.sheet_name or file_info.get("default_sheet")
    cache_key = f"{request.file_id}_{sheet_name}"
    
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

def preview_delete_operation(file_id: str, filters: list, sheet_name: str = None):
    """Previsualiza qué filas serían eliminadas"""
    file_info = storage.get(file_id)
    if not file_info:
        raise ValueError("Archivo no encontrado")
    
    sheet_name = sheet_name or file_info.get("default_sheet")
    cache_key = f"{file_id}_{sheet_name}"
    
    if cache_key not in data_cache:
        raise ValueError("Datos no encontrados en cache")
    
    df = data_cache[cache_key]
    
    return DeleteService.preview_delete_by_filters(df, filters)

def bulk_delete_operation(request: BulkDeleteRequest):
    """Operación de eliminación masiva con confirmación"""
    if not request.confirm_delete:
        raise ValueError("Operación de eliminación masiva requiere confirmación explícita")
    
    file_info = storage.get(request.file_id)
    if not file_info:
        raise ValueError("Archivo no encontrado")
    
    sheet_name = request.sheet_name or file_info.get("default_sheet")
    cache_key = f"{request.file_id}_{sheet_name}"
    
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

def delete_duplicates(file_id: str, columns: list = None, keep: str = 'first', sheet_name: str = None):
    """Elimina filas duplicadas"""
    file_info = storage.get(file_id)
    if not file_info:
        raise ValueError("Archivo no encontrado")
    
    sheet_name = sheet_name or file_info.get("default_sheet")
    cache_key = f"{file_id}_{sheet_name}"
    
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

def cleanup_exports(days_old: int = 7):
    """Limpia archivos de exportación antiguos"""
    return ExportService.cleanup_old_exports(days_old)
