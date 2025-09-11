# api/technical_note_routes.py - COMPLETO CON DEBUG
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import json
from controllers.technical_note_controller.technical_note import technical_note_controller

router = APIRouter()

@router.get("/available")
def get_available_technical_files():
    """Lista archivos técnicos disponibles"""
    try:
        available_files = technical_note_controller.get_available_static_files()
        return available_files
        
    except Exception as e:
        print(f"❌ Error en /available: {e}")
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
    """ENDPOINT CON FILTROS ESTILO EXCEL - Ultra-optimizado"""
    try:
        print(f"📡 GET /data/{filename} - página {page}, tamaño {page_size}")
        
        # Parsear filtros JSON
        parsed_filters = None
        if filters:
            try:
                parsed_filters = json.loads(filters)
                print(f"🔍 Filtros parseados: {parsed_filters}")
            except json.JSONDecodeError as e:
                print(f"⚠️ Error parseando filtros JSON: {e}")
                parsed_filters = None
        
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
        
        print(f"✅ Respuesta preparada: {len(result.get('data', []))} registros")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en /data/{filename}: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    
@router.get("/geographic/{filename}/departamentos")
def get_departamentos(filename: str):
    """Obtiene lista de departamentos únicos"""
    try:
        print(f"🗺️ GET /geographic/{filename}/departamentos")
        
        result = technical_note_controller.get_geographic_values(
            filename=filename,
            geo_type='departamento'
        )
        
        print(f"✅ Departamentos obtenidos: {len(result.get('values', []))}")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en /departamentos: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.get("/geographic/{filename}/municipios")
def get_municipios(
    filename: str,
    departamento: Optional[str] = Query(None, description="Departamento padre para filtrar municipios")
):
    """Obtiene municipios filtrados por departamento"""
    try:
        print(f"🗺️ GET /geographic/{filename}/municipios?departamento={departamento}")
        
        result = technical_note_controller.get_geographic_values(
            filename=filename,
            geo_type='municipios',  # Plural
            departamento=departamento
        )
        
        print(f"✅ Municipios obtenidos: {len(result.get('values', []))}")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en /municipios: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.get("/geographic/{filename}/ips")
def get_ips(
    filename: str,
    departamento: Optional[str] = Query(None, description="Departamento padre"),
    municipio: Optional[str] = Query(None, description="Municipio padre para filtrar IPS")
):
    """Obtiene IPS filtradas por municipio y departamento"""
    try:
        print(f"🗺️ GET /geographic/{filename}/ips?departamento={departamento}&municipio={municipio}")
        
        result = technical_note_controller.get_geographic_values(
            filename=filename,
            geo_type='ips',  # Singular
            departamento=departamento,
            municipio=municipio
        )
        
        print(f"✅ IPS obtenidas: {len(result.get('values', []))}")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en /ips: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.get("/report/{filename}")
def get_keyword_age_report(
    filename: str,
    keywords: Optional[str] = Query(None, description="Lista separada por comas, ej: medicina,enfermeria"),
    min_count: int = Query(0, ge=0, description="Filtra ítems con conteo menor a este valor"),
    include_temporal: bool = Query(True, description="Incluir análisis temporal por año/mes"),
    # ✅ NUEVOS PARÁMETROS GEOGRÁFICOS
    departamento: Optional[str] = Query(None, description="Filtrar por departamento específico"),
    municipio: Optional[str] = Query(None, description="Filtrar por municipio específico"),
    ips: Optional[str] = Query(None, description="Filtrar por IPS específica")
):
    """Genera reporte de palabras clave + rangos de edad CON FILTROS GEOGRÁFICOS"""
    try:
        print(f"📊 GET /report/{filename} con filtros geográficos")
        print(f"🗺️ Filtros: Dept={departamento}, Mun={municipio}, IPS={ips}")
        
        # Procesar keywords
        kw_list = None
        if keywords and keywords.strip():
            kw_list = [k.strip().lower() for k in keywords.split(",") if k.strip()]
        
        # ✅ LLAMAR CON FILTROS GEOGRÁFICOS
        result = technical_note_controller.get_keyword_age_report(
            filename=filename,
            keywords=kw_list,
            min_count=min_count,
            include_temporal=include_temporal,
            departamento=departamento,
            municipio=municipio,
            ips=ips
        )
        
        items_count = len(result.get('items', []))
        print(f"✅ Reporte geográfico generado: {items_count} items")
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en /report/{filename}: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.get("/unique-values/{filename}/{column_name}")
def get_column_unique_values(
    filename: str,
    column_name: str,
    sheet_name: Optional[str] = Query(None, description="Hoja de Excel"),
    limit: int = Query(1000, ge=10, le=5000, description="Límite de valores únicos")
):
    """Obtiene valores únicos de una columna (estilo Excel)"""
    try:
        result = technical_note_controller.get_column_unique_values(
            filename, column_name, sheet_name, limit
        )
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en /unique-values/{filename}/{column_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.get("/metadata/{filename}")
def get_technical_file_metadata(filename: str):
    """Metadatos del archivo"""
    try:
        result = technical_note_controller.get_technical_file_metadata(filename)
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en /metadata/{filename}: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.get("/columns/{filename}")
def get_file_columns(filename: str):
    """Obtiene solo las columnas de un archivo"""
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
        print(f"❌ Error en /columns/{filename}: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

# Endpoint de debug adicional
@router.get("/debug/{filename}")
def debug_file_state(filename: str):
    """Debug del estado interno del archivo"""
    try:
        from services.duckdb_service.duckdb_service import duckdb_service
        
        file_key = f"technical_{filename}"
        
        debug_info = {
            "file_key": file_key,
            "loaded_in_controller": file_key in technical_note_controller.loaded_technical_files,
            "loaded_in_duckdb": file_key in duckdb_service.loaded_tables,
            "available_tables": duckdb_service.list_tables(),
            "controller_cache": technical_note_controller.loaded_technical_files.get(file_key),
            "duckdb_cache": duckdb_service.get_table_info(file_key)
        }
        
        return debug_info
        
    except Exception as e:
        print(f"❌ Error en debug: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

