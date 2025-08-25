"""
Controlador base con configuraciones y utilidades comunes
"""
import os
import uuid
from typing import Dict, Any
from services.csv_service import CSVService
from services.excel_service import ExcelService

# Servicios de archivo por extensión
file_services = {
    "csv": CSVService(),
    "xlsx": ExcelService(),
    "xls": ExcelService()
}

# Almacenamiento en memoria (en producción usar base de datos)
storage: Dict[str, Any] = {}
data_cache: Dict[str, Any] = {}  # Cache para DataFrames procesados

def ensure_upload_directory() -> str:
    """Asegura que el directorio de uploads exista"""
    upload_dir = "uploads"
    if not os.path.exists(upload_dir):
        os.makedirs(upload_dir)
    return upload_dir

def generate_unique_file_id() -> str:
    """Genera un ID único para archivos"""
    return str(uuid.uuid4())

def get_file_service(ext: str):
    """Obtiene el servicio apropiado para una extensión de archivo"""
    if ext not in file_services:
        raise ValueError(f"Tipo de archivo no soportado: {ext}")
    return file_services[ext]

def validate_file_exists(file_id: str) -> Dict[str, Any]:
    """Valida que un archivo existe y retorna su información"""
    file_info = storage.get(file_id)
    if not file_info:
        raise ValueError("Archivo no encontrado")
    return file_info

def get_cache_key(file_id: str, sheet_name: str = None) -> str:
    """Genera clave para cache de DataFrame"""
    return f"{file_id}_{sheet_name}"

def cleanup_file_cache(file_id: str):
    """Limpia entradas de cache relacionadas con un archivo"""
    cache_keys_to_remove = [key for key in data_cache.keys() if key.startswith(file_id)]
    for key in cache_keys_to_remove:
        del data_cache[key]
