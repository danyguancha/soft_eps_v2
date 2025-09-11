
import os
from typing import Optional, List

def find_csv_file(filename: str, search_paths: List[str]) -> Optional[str]:
    """Busca un archivo CSV en las rutas especificadas"""
    possible_paths = []
    for base_path in search_paths:
        possible_paths.extend([
            os.path.join(base_path, filename),
            os.path.join(base_path, ".", filename),
            filename if base_path == "." else None
        ])
    
    possible_paths = [p for p in possible_paths if p]
    
    for path in possible_paths:
        if os.path.exists(path):
            print(f"📁 CSV encontrado: {path}")
            return os.path.abspath(path)
    
    print(f"❌ CSV no encontrado en rutas: {possible_paths}")
    return None

def generate_file_key(filename: str, prefix: str = "technical") -> str:
    """Genera una clave única para un archivo"""
    clean_name = filename.replace('.', '_').replace(' ', '_').replace('-', '_')
    return f"{prefix}_{clean_name}"

def get_supported_extensions() -> set:
    """Retorna las extensiones de archivo soportadas"""
    return {'.csv', '.xlsx', '.xls'}

def is_supported_file(filename: str) -> bool:
    """Verifica si un archivo tiene una extensión soportada"""
    _, ext = os.path.splitext(filename)
    return ext.lower() in get_supported_extensions()
