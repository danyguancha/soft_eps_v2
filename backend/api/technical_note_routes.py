# api/technical_note_routes.py (VERSIÓN CORREGIDA)
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import json
from controllers.technical_note_controller.technical_note import technical_note_controller

router = APIRouter()

@router.get("/available")
def get_available_technical_files():
    """✅ Lista archivos técnicos disponibles"""
    try:
        available_files = technical_note_controller.get_available_static_files()
        return available_files
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.get("/data/{filename}")
def get_technical_file_data_with_excel_filters(
    filename: str, 
    page: int = Query(1, ge=1, description="Número de página"),
    page_size: int = Query(1000, ge=10, le=2000, description="Registros por página"),
    sheet_name: Optional[str] = Query(None, description="Hoja de Excel"),
    search: Optional[str] = Query(None, description="Búsqueda global en todos los campos"),
    sort_by: Optional[str] = Query(None, description="Columna para ordenar"),
    sort_order: Optional[str] = Query("asc", regex="^(asc|desc)$", description="Dirección de ordenamiento"),
    filters: Optional[str] = Query(None, description="Filtros JSON estilo Excel")
):
    """✅ ENDPOINT CON FILTROS ESTILO EXCEL - Ultra-optimizado"""
    try:
        # Parsear filtros JSON
        parsed_filters = None
        if filters:
            try:
                parsed_filters = json.loads(filters)
                print(f"📋 Filtros estilo Excel parseados: {parsed_filters}")
            except json.JSONDecodeError as e:
                print(f"⚠️ Error parseando filtros JSON: {e}")
                parsed_filters = None
        
        print(f"🌐 Request con filtros estilo Excel: {filename}, página {page}, filtros: {len(parsed_filters or [])}")
        
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
        
        print(f"✅ Respuesta con filtros estilo Excel: {result['pagination']['rows_in_page']} de {result['pagination']['total_rows']} registros")
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en endpoint con filtros estilo Excel: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.get("/unique-values/{filename}/{column_name}")
def get_column_unique_values(
    filename: str,
    column_name: str,
    sheet_name: Optional[str] = Query(None, description="Hoja de Excel"),
    limit: int = Query(1000, ge=10, le=5000, description="Límite de valores únicos")
):
    """✅ Obtiene valores únicos de una columna (estilo Excel)"""
    try:
        result = technical_note_controller.get_column_unique_values(
            filename, column_name, sheet_name, limit
        )
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.get("/metadata/{filename}")
def get_technical_file_metadata(filename: str):
    """✅ Metadatos del archivo"""
    try:
        result = technical_note_controller.get_technical_file_metadata(filename)
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.get("/columns/{filename}")
def get_file_columns(filename: str):
    """✅ Obtiene solo las columnas de un archivo"""
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
