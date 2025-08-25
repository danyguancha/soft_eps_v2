"""
Controladores del sistema de procesamiento de archivos
"""

# Importar controladores específicos
from .file_upload_controller import upload_file, validate_upload_file, get_file_metadata
from .data_controller import get_data, get_data_sample, get_column_statistics
from .transform_controller import (
    transform_data, preview_transformation, get_available_transformations
)
from .export_controller import (
    export_processed_data, get_export_formats, cleanup_old_exports, get_export_history
)
from .delete_controller import (
    delete_specific_rows, delete_rows_by_filter, preview_delete_operation,
    bulk_delete_operation, delete_duplicates
)
from .file_controller import (
    get_file_info, delete_file, list_all_files, get_columns,
    get_system_stats, cleanup_system
)

# Exportar todas las funciones para fácil importación
__all__ = [
    # File upload
    'upload_file', 'validate_upload_file', 'get_file_metadata',
    
    # Data operations
    'get_data', 'get_data_sample', 'get_column_statistics',
    
    # Transformations
    'transform_data', 'preview_transformation', 'get_available_transformations',
    
    # Export
    'export_processed_data', 'get_export_formats', 'cleanup_old_exports', 'get_export_history',
    
    # Delete operations
    'delete_specific_rows', 'delete_rows_by_filter', 'preview_delete_operation',
    'bulk_delete_operation', 'delete_duplicates',
    
    # File management
    'get_file_info', 'delete_file', 'list_all_files', 'get_columns',
    'get_system_stats', 'cleanup_system'
]
