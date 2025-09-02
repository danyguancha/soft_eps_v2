# api/technical_note_routes.py - VERSIÓN COMPLETA CON FILTROS DEL SERVIDOR
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import json
from controllers.technical_note_controller.technical_note import technical_note_controller

router = APIRouter()

@router.get("/available")
def get_available_technical_files():
    """✅ ULTRA-RÁPIDO: Lista archivos sin leer contenido"""
    try:
        available_files = technical_note_controller.get_available_static_files()
        return available_files
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.get("/data/{filename}")
def get_technical_file_data_with_filters(
    filename: str, 
    page: int = Query(1, ge=1, description="Número de página"),
    page_size: int = Query(1000, ge=10, le=2000, description="Registros por página"),
    sheet_name: Optional[str] = Query(None, description="Hoja de Excel"),
    # ✅ NUEVOS PARÁMETROS PARA FILTRADO DEL SERVIDOR
    search: Optional[str] = Query(None, description="Búsqueda global en todos los campos"),
    sort_by: Optional[str] = Query(None, description="Columna para ordenar"),
    sort_order: Optional[str] = Query("asc", regex="^(asc|desc)$", description="Dirección de ordenamiento"),
    # Filtros como JSON string
    filters: Optional[str] = Query(None, description="Filtros JSON: [{'column':'nombre','operator':'contains','value':'juan'}]")
):
    """✅ ENDPOINT CON FILTRADO DEL SERVIDOR - Lee toda la base de datos y aplica filtros"""
    try:
        # Parsear filtros JSON
        parsed_filters = None
        if filters:
            try:
                parsed_filters = json.loads(filters)
                print(f"📋 Filtros parseados: {parsed_filters}")
            except json.JSONDecodeError as e:
                print(f"⚠️ Error parseando filtros JSON: {e}")
                parsed_filters = None
        
        print(f"🌐 Request con filtros del servidor: {filename}, página {page}, filtros: {len(parsed_filters or [])}")
        
        result = technical_note_controller.read_technical_file_data_paginated(
            filename=filename,
            page=page, 
            page_size=page_size, 
            sheet_name=sheet_name,
            filters=parsed_filters,
            search=search,
            sort_by=sort_by,
            sort_order=sort_order
        )
        
        print(f"✅ Respuesta del servidor: {result['pagination']['rows_in_page']} de {result['pagination']['total_rows']} registros")
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en endpoint con filtros: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

# ✅ ENDPOINT LEGACY PARA COMPATIBILIDAD
@router.get("/data-legacy/{filename}")
def get_technical_file_data_paginated_legacy(
    filename: str, 
    page: int = Query(1, ge=1, description="Número de página"),
    page_size: int = Query(1000, ge=10, le=2000, description="Registros por página"),
    sheet_name: Optional[str] = Query(None, description="Hoja de Excel")
):
    """✅ ENDPOINT LEGACY: Sin filtros para compatibilidad"""
    try:
        result = technical_note_controller.read_technical_file_data_paginated_legacy(
            filename, page, page_size, sheet_name
        )
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.get("/metadata/{filename}")
def get_technical_file_metadata(filename: str):
    """✅ Metadatos bajo demanda"""
    try:
        result = technical_note_controller.get_technical_file_metadata(filename)
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

# ✅ ENDPOINT PARA OBTENER COLUMNAS DISPONIBLES (UTIL PARA FILTROS)
@router.get("/columns/{filename}")
def get_file_columns(filename: str):
    """✅ Obtiene solo las columnas de un archivo (para construir filtros)"""
    try:
        metadata = technical_note_controller.get_technical_file_metadata(filename)
        return {
            "filename": filename,
            "columns": metadata["columns"],
            "total_columns": len(metadata["columns"]),
            "display_name": metadata["display_name"]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
