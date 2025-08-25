"""
Controlador para consulta de datos con filtros, ordenamiento y paginación
"""
import pandas as pd
from models.schemas import DataRequest
from services.filter_service import FilterService
from services.sort_service import SortService
from services.pagination_service import PaginationService
from .base_controller import (
    storage, data_cache, file_services, validate_file_exists, get_cache_key
)

def get_data(request: DataRequest):
    """Obtiene datos con filtros, ordenamiento y paginación"""
    file_info = validate_file_exists(request.file_id)
    
    # Determinar hoja a usar
    sheet_name = request.sheet_name or file_info.get("default_sheet")
    cache_key = get_cache_key(request.file_id, sheet_name)
    
    # Obtener DataFrame desde cache o cargar
    df = _get_dataframe_from_cache_or_load(file_info, cache_key, sheet_name)
    
    # Aplicar procesamiento de datos
    df = _apply_data_processing(df, request)
    
    # Aplicar paginación
    return PaginationService.paginate(df, request.page, request.page_size)

def _get_dataframe_from_cache_or_load(file_info: dict, cache_key: str, sheet_name: str) -> pd.DataFrame:
    """Obtiene DataFrame desde cache o lo carga desde archivo"""
    if cache_key in data_cache:
        return data_cache[cache_key].copy()
    else:
        service = file_services[file_info["ext"]]
        file_obj = service.load(file_info["path"])
        df = service.get_data(file_obj, sheet_name)
        data_cache[cache_key] = df.copy()
        return df

def _apply_data_processing(df: pd.DataFrame, request: DataRequest) -> pd.DataFrame:
    """Aplica filtros, búsqueda y ordenamiento al DataFrame"""
    # Aplicar búsqueda global si está presente
    if request.search:
        df = FilterService.apply_search(df, request.search)
    
    # Aplicar filtros
    if request.filters:
        df = FilterService.apply_filters(df, request.filters)
    
    # Aplicar ordenamiento
    if request.sort:
        df = SortService.apply_sort(df, request.sort)
    
    return df

def get_data_sample(file_id: str, sheet_name: str = None, sample_size: int = 10) -> dict:
    """Obtiene una muestra de datos para previsualización"""
    file_info = validate_file_exists(file_id)
    
    sheet_name = sheet_name or file_info.get("default_sheet")
    cache_key = get_cache_key(file_id, sheet_name)
    
    df = _get_dataframe_from_cache_or_load(file_info, cache_key, sheet_name)
    
    sample_df = df.head(sample_size)
    
    return {
        "sample_data": sample_df.fillna("").to_dict(orient="records"),
        "columns": df.columns.tolist(),
        "total_rows": len(df),
        "sample_size": len(sample_df)
    }

def get_column_statistics(file_id: str, column_name: str, sheet_name: str = None) -> dict:
    """Obtiene estadísticas de una columna específica"""
    file_info = validate_file_exists(file_id)
    
    sheet_name = sheet_name or file_info.get("default_sheet")
    cache_key = get_cache_key(file_id, sheet_name)
    
    df = _get_dataframe_from_cache_or_load(file_info, cache_key, sheet_name)
    
    if column_name not in df.columns:
        raise ValueError(f"Columna '{column_name}' no encontrada")
    
    column_data = df[column_name]
    
    stats = {
        "column_name": column_name,
        "total_values": len(column_data),
        "null_count": column_data.isnull().sum(),
        "unique_count": column_data.nunique(),
        "data_type": str(column_data.dtype)
    }
    
    # Estadísticas adicionales para columnas numéricas
    if pd.api.types.is_numeric_dtype(column_data):
        stats.update({
            "min_value": column_data.min(),
            "max_value": column_data.max(),
            "mean_value": column_data.mean(),
            "median_value": column_data.median(),
            "std_dev": column_data.std()
        })
    
    # Valores más frecuentes
    value_counts = column_data.value_counts().head(10)
    stats["top_values"] = value_counts.to_dict()
    
    return stats
