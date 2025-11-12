# utils/duckdb_utils/validation_utils.py
from typing import Dict, Any

def validate_connection_health(conn) -> bool:
    """Valida que la conexión DuckDB esté funcionando"""
    try:
        if not conn:
            return False
        conn.execute("SELECT 1").fetchone()
        return True
    except Exception:
        return False

def validate_file_id_format(file_id: str) -> bool:
    """Valida que el file_id tenga formato correcto"""
    if not file_id or not isinstance(file_id, str):
        return False
    return len(file_id.strip()) > 0

def build_availability_response(available: bool, fallback_required: bool = False) -> Dict[str, Any]:
    """Construye respuesta estándar de disponibilidad"""
    return {
        "success": available,
        "error": "DuckDB no disponible" if not available else None,
        "requires_fallback": fallback_required if not available else False
    }
