"""
Controlador para transformaciones de datos
"""
from models.schemas import TransformRequest
from services.transformation_service import TransformationService
from .base_controller import storage, data_cache, validate_file_exists, get_cache_key

def transform_data(request: TransformRequest) -> dict:
    """Aplica transformación a los datos"""
    file_info = validate_file_exists(request.file_id)
    
    # Obtener DataFrame actual
    cache_key = get_cache_key(request.file_id, file_info.get('default_sheet'))
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
    storage[request.file_id]["total_rows"] = len(transformed_df)
    
    return {
        "message": "Transformación aplicada exitosamente",
        "new_columns": transformed_df.columns.tolist(),
        "total_rows": len(transformed_df)
    }

def preview_transformation(request: TransformRequest, preview_rows: int = 10) -> dict:
    """Previsualiza el resultado de una transformación sin aplicarla"""
    file_info = validate_file_exists(request.file_id)
    
    cache_key = get_cache_key(request.file_id, file_info.get('default_sheet'))
    if cache_key not in data_cache:
        raise ValueError("Datos no encontrados en cache")
    
    df = data_cache[cache_key].copy()
    
    # Aplicar transformación a una copia
    try:
        transformed_df = TransformationService.apply_transformation(
            df, request.operation, request.params
        )
        
        preview_data = transformed_df.head(preview_rows).fillna("").to_dict(orient="records")
        
        return {
            "preview_data": preview_data,
            "new_columns": transformed_df.columns.tolist(),
            "total_rows_after": len(transformed_df),
            "columns_added": list(set(transformed_df.columns) - set(df.columns)),
            "columns_removed": list(set(df.columns) - set(transformed_df.columns))
        }
    except Exception as e:
        raise ValueError(f"Error en previsualización de transformación: {str(e)}")

def get_available_transformations() -> dict:
    """Retorna las transformaciones disponibles con sus parámetros requeridos"""
    return {
        "concatenate": {
            "description": "Concatena múltiples columnas",
            "required_params": ["columns", "new_column"],
            "optional_params": ["separator"]
        },
        "split_column": {
            "description": "Divide una columna en múltiples columnas",
            "required_params": ["column", "separator", "new_columns"],
            "optional_params": []
        },
        "replace_values": {
            "description": "Reemplaza valores en una columna",
            "required_params": ["column", "old_value", "new_value"],
            "optional_params": []
        },
        "create_calculated": {
            "description": "Crea columna calculada con expresión",
            "required_params": ["new_column", "expression"],
            "optional_params": []
        },
        "rename_column": {
            "description": "Renombra una columna",
            "required_params": ["old_name", "new_name"],
            "optional_params": []
        },
        "delete_column": {
            "description": "Elimina una columna",
            "required_params": ["column"],
            "optional_params": []
        },
        "fill_null": {
            "description": "Llena valores nulos",
            "required_params": ["column"],
            "optional_params": ["fill_value"]
        },
        "to_uppercase": {
            "description": "Convierte texto a mayúsculas",
            "required_params": ["column"],
            "optional_params": []
        },
        "to_lowercase": {
            "description": "Convierte texto a minúsculas",
            "required_params": ["column"],
            "optional_params": []
        },
        "extract_substring": {
            "description": "Extrae substring de una columna",
            "required_params": ["column", "new_column"],
            "optional_params": ["start", "end", "pattern"]
        }
    }
