"""
Controlador especializado en carga de archivos
"""
import os
from fastapi import UploadFile
from .base_controller import (
    file_services, storage, data_cache, ensure_upload_directory, 
    generate_unique_file_id, get_file_service
)

def upload_file(file: UploadFile) -> dict:
    """Procesa la carga de archivo y devuelve metadatos"""
    ext = file.filename.split('.')[-1].lower()
    service = get_file_service(ext)
    
    # Generar ID único para el archivo
    file_id = generate_unique_file_id()
    upload_dir = ensure_upload_directory()
    file_path = os.path.join(upload_dir, f"{file_id}.{ext}")
    
    # Guardar archivo físicamente
    with open(file_path, "wb") as f:
        f.write(file.file.read())
    
    # Procesar archivo
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
    cache_key = f"{file_id}_{default_sheet}"
    data_cache[cache_key] = df
    
    return {
        "file_id": file_id,
        "columns": columns,
        "sheets": sheets,
        "total_rows": total_rows
    }

def validate_upload_file(file: UploadFile) -> str:
    """Valida que el archivo es de un tipo soportado"""
    if not file.filename:
        raise ValueError("Nombre de archivo requerido")
    
    ext = file.filename.split('.')[-1].lower()
    if ext not in file_services:
        raise ValueError(f"Tipo de archivo no soportado: {ext}. Tipos válidos: {', '.join(file_services.keys())}")
    
    return ext

def get_file_metadata(file_id: str, sheet_name: str = None) -> dict:
    """Obtiene metadatos específicos de un archivo y hoja"""
    from .base_controller import validate_file_exists
    
    file_info = validate_file_exists(file_id)
    
    if sheet_name and sheet_name not in (file_info.get("sheets") or []):
        # Cargar hoja específica si no está en metadatos principales
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
