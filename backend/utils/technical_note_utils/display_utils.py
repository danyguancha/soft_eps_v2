
from typing import Dict

def get_display_name_mapping() -> Dict[str, str]:
    """Mapeo de nombres de archivo a nombres display"""
    return {
        "AdolescenciaNueva.csv": "Adolescencia",
        "AdultezNueva.csv": "Adultez", 
        "InfanciaNueva.csv": "Infancia",
        "JuventudNueva.csv": "Juventud",
        "PrimeraInfanciaNueva.csv": "Primera Infancia",
        "VejezNueva.csv": "Vejez"
    }

def get_description_mapping() -> Dict[str, str]:
    """Mapeo de nombres de archivo a descripciones"""
    return {
        "AdolescenciaNueva.csv": "Datos de población adolescente",
        "AdultezNueva.csv": "Datos de población adulta",
        "InfanciaNueva.csv": "Datos de población infantil", 
        "JuventudNueva.csv": "Datos de población joven",
        "PrimeraInfanciaNueva.csv": "Datos de primera infancia",
        "VejezNueva.csv": "Datos de población adulto mayor"
    }

def generate_display_name(filename: str) -> str:
    """Genera nombre display amigable"""
    mapping = get_display_name_mapping()
    if filename in mapping:
        return mapping[filename]
    
    # Fallback para archivos no mapeados
    return filename.replace('.csv', '').replace('.xlsx', '').replace('.xls', '')

def generate_description(filename: str) -> str:
    """Genera descripción del archivo"""
    mapping = get_description_mapping()
    if filename in mapping:
        return mapping[filename]
    
    return f"Archivo técnico: {filename}"
