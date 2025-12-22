# config.py
"""
Configuración central del sistema con gestión de cache TTL
"""
import os
from typing import Dict, Any
from pathlib import Path

# 🔥 IMPORTAR Y CARGAR .env ANTES DE TODO
from dotenv import load_dotenv

# Buscar el archivo .env en el directorio raíz del proyecto
env_path = Path(__file__).parent / '.env'

# Cargar variables de entorno desde .env
load_dotenv(dotenv_path=env_path, override=True)
# override=True asegura que los valores del .env sobrescriban las variables de sistema

print(f"📝 Cargando configuración desde: {env_path}")
print(f"   ¿Archivo .env existe? {env_path.exists()}")


class CacheConfig:
    """Configuración de limpieza automática de cache"""
    
    # ========== TIEMPOS DE EXPIRACIÓN (TTL) ==========
    # Tiempo en MINUTOS antes de considerar un archivo como "inactivo"
    
    # Cache de archivos convertidos (Parquet)
    PARQUET_CACHE_TTL_MINUTES = int(os.getenv("PARQUET_CACHE_TTL_MINUTES", "120"))
    
    # Cache de metadatos
    METADATA_CACHE_TTL_MINUTES = int(os.getenv("METADATA_CACHE_TTL_MINUTES", "120"))
    
    # Archivos NT RPMS procesados
    NT_RPMS_CACHE_TTL_MINUTES = int(os.getenv("NT_RPMS_CACHE_TTL_MINUTES", "240"))
    
    # Archivos técnicos precargados
    TECHNICAL_NOTE_TTL_MINUTES = int(os.getenv("TECHNICAL_NOTE_TTL_MINUTES", "180"))
    
    # DuckDB storage
    DUCKDB_STORAGE_TTL_MINUTES = int(os.getenv("DUCKDB_STORAGE_TTL_MINUTES", "120"))
    
    
    # ========== FRECUENCIA DE LIMPIEZA ==========
    # Cada cuántos MINUTOS se ejecuta el proceso de limpieza automática
    CLEANUP_INTERVAL_MINUTES = int(os.getenv("CLEANUP_INTERVAL_MINUTES", "30"))
    
    
    # ========== PROTECCIÓN DE ARCHIVOS ==========
    # Archivos que NUNCA se eliminan automáticamente
    PROTECTED_FILES = {
        "extract_info_nt": [
            "departamentos.xlsx",
        ],
        "technical_note": [
            # Agregar archivos específicos que no se deben eliminar
        ],
        "duckdb_storage": [
            'main.duckdb',
        ],
        "metadata_cache": [
            # Metadatos críticos
        ],
        "parquet_cache": [
            # Archivos Parquet permanentes
        ]
    }
    
    
    # ========== CONFIGURACIÓN DE TRACKING ==========
    # Archivo donde se guarda el registro de accesos
    ACCESS_TRACKING_FILE = "cache_access_tracking.json"
    
    # Habilitar logging detallado de limpieza
    VERBOSE_CLEANUP_LOGGING = os.getenv("VERBOSE_CLEANUP_LOGGING", "true").lower() == "true"
    
    
    # ========== DIRECTORIOS ==========
    # 🔥 IMPORTANTE: Estos valores se evalúan DESPUÉS de cargar el .env
    DIRECTORIES = {}
    
    @classmethod
    def _initialize_directories(cls):
        """Inicializa DIRECTORIES después de cargar variables de entorno"""
        cls.DIRECTORIES = {
            "parquet_cache": {
                "path": "parquet_cache",
                "ttl_minutes": cls.PARQUET_CACHE_TTL_MINUTES,
                "description": "Archivos Parquet convertidos"
            },
            "metadata_cache": {
                "path": "metadata_cache",
                "ttl_minutes": cls.METADATA_CACHE_TTL_MINUTES,
                "description": "Metadatos de archivos"
            },
            "extract_info_nt": {
                "path": "extract_info_nt",
                "ttl_minutes": cls.NT_RPMS_CACHE_TTL_MINUTES,
                "description": "Archivos NT RPMS procesados"
            },
            "technical_note": {
                "path": "technical_note",
                "ttl_minutes": cls.TECHNICAL_NOTE_TTL_MINUTES,
                "description": "Archivos técnicos precargados"
            },
            "duckdb_storage": {
                "path": "duckdb_storage",
                "ttl_minutes": cls.DUCKDB_STORAGE_TTL_MINUTES,
                "description": "Almacenamiento DuckDB"
            }
        }
    
    
    @classmethod
    def get_ttl_for_directory(cls, directory_name: str) -> int:
        """Obtiene el TTL en minutos para un directorio específico"""
        if not cls.DIRECTORIES:
            cls._initialize_directories()
        
        dir_config = cls.DIRECTORIES.get(directory_name)
        if dir_config:
            return dir_config["ttl_minutes"]
        return cls.PARQUET_CACHE_TTL_MINUTES  # Default
    
    
    @classmethod
    def is_protected_file(cls, directory: str, filename: str) -> bool:
        """Verifica si un archivo está protegido contra eliminación automática"""
        protected_list = cls.PROTECTED_FILES.get(directory, [])
        return filename in protected_list
    
    
    @classmethod
    def get_config_summary(cls) -> Dict[str, Any]:
        """Retorna un resumen de la configuración actual"""
        if not cls.DIRECTORIES:
            cls._initialize_directories()
        
        return {
            "ttl_configuration": {
                dir_name: f"{config['ttl_minutes']} minutos"
                for dir_name, config in cls.DIRECTORIES.items()
            },
            "cleanup_interval": f"{cls.CLEANUP_INTERVAL_MINUTES} minutos",
            "protected_files_count": sum(len(files) for files in cls.PROTECTED_FILES.values()),
            "verbose_logging": cls.VERBOSE_CLEANUP_LOGGING
        }
    
    
    @classmethod
    def print_loaded_config(cls):
        """Imprime la configuración cargada (útil para debugging)"""
        print("\n" + "="*60)
        print("⚙️  CONFIGURACIÓN DE CACHE CARGADA")
        print("="*60)
        print(f"📁 Parquet Cache TTL: {cls.PARQUET_CACHE_TTL_MINUTES} minutos")
        print(f"📁 Metadata Cache TTL: {cls.METADATA_CACHE_TTL_MINUTES} minutos")
        print(f"📁 NT RPMS TTL: {cls.NT_RPMS_CACHE_TTL_MINUTES} minutos")
        print(f"📁 Technical Note TTL: {cls.TECHNICAL_NOTE_TTL_MINUTES} minutos")
        print(f"📁 DuckDB Storage TTL: {cls.DUCKDB_STORAGE_TTL_MINUTES} minutos")
        print(f"⏱️  Intervalo de limpieza: {cls.CLEANUP_INTERVAL_MINUTES} minutos")
        print(f"📝 Logging detallado: {cls.VERBOSE_CLEANUP_LOGGING}")
        print("="*60 + "\n")


class Config:
    """Configuración general de la aplicación"""
    
    # Configuración para producción
    MAX_FILE_SIZE = 5 * 1024 * 1024 * 1024  # 5GB
    UPLOAD_DIR = "uploads"
    REQUEST_TIMEOUT = 300  # 5 minutos
    
    # Puerto FIJO para producción
    PORT = int(os.getenv("PORT", "8000"))
    HOST = os.getenv("HOST", "0.0.0.0")
    
    # Ambiente
    ENVIRONMENT = os.getenv("ENVIRONMENT", "production")
    
    # Workers (para Gunicorn/Uvicorn workers)
    WORKERS = int(os.getenv("WORKERS", "2"))
    
    # CORS - Ajustar según tu red
    ALLOWED_ORIGINS = os.getenv(
        "ALLOWED_ORIGINS",
        "*"  # En producción, especifica IPs: "http://192.168.1.100,http://nt.local"
    ).split(",")
    
    # Cache configuration
    cache = CacheConfig


# 🔥 Inicializar directorios después de cargar variables
CacheConfig._initialize_directories()

# Instancia global de configuración
config = Config()
cache_config = CacheConfig()
