# api/technical_note_routes.py - COMPLETO CON DEBUG
from datetime import datetime
import os
import tempfile
import uuid
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from typing import Any, Dict, List, Optional
import json

from fastapi.responses import FileResponse
from pydantic import BaseModel, Field, validator
from controllers.technical_note_controller.age_range_extractor import AgeRangeExtractor
from controllers.technical_note_controller.technical_note import technical_note_controller
from services.technical_note_services.report_service_aux.report_exporter import ReportExporter


report_exporter = ReportExporter()
age_extractor = AgeRangeExtractor()

router = APIRouter()
reports_router = APIRouter(prefix="/reports", tags=["Advanced Reports"])

temp_reports: Dict[str, Dict[str, Any]] = {}
temp_files: Dict[str, Dict[str, Any]] = {}

class GeographicFiltersModel(BaseModel):
    """🗺️ Filtros geográficos para el reporte"""
    departamento: Optional[str] = Field(None, description="Departamento específico o 'Todos'")
    municipio: Optional[str] = Field(None, description="Municipio específico o 'Todos'")
    ips: Optional[str] = Field(None, description="IPS específica o 'Todos'")

class AdvancedReportRequestModel(BaseModel):
    """📄 Modelo de solicitud para generar reporte avanzado"""
    data_source: str = Field(..., description="Nombre de la tabla/fuente de datos (filename)")
    filename: str = Field(..., description="Nombre base del archivo de salida")
    keywords: Optional[List[str]] = Field(default=[], description="Lista de palabras clave a buscar")
    min_count: int = Field(default=0, description="Conteo mínimo para incluir resultados")
    include_temporal: bool = Field(default=True, description="Incluir análisis temporal")
    geographic_filters: Optional[GeographicFiltersModel] = Field(default=None, description="Filtros geográficos")
    corte_fecha: str = Field(default="2025-07-31", description="Fecha de corte en formato YYYY-MM-DD")
    
    @validator('corte_fecha')
    def validate_corte_fecha(cls, v):
        try:
            datetime.strptime(v, '%Y-%m-%d')
            return v
        except ValueError:
            raise ValueError('corte_fecha debe estar en formato YYYY-MM-DD')

class ExportOptionsModel(BaseModel):
    """📤 Opciones de exportación"""
    export_csv: bool = Field(default=True, description="Exportar en formato CSV")
    export_pdf: bool = Field(default=True, description="Exportar en formato PDF")
    include_temporal: bool = Field(default=True, description="Incluir datos temporales en la exportación")

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
        
        print(f"Respuesta preparada: {len(result.get('data', []))} registros")
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
        
        print(f"Departamentos obtenidos: {len(result.get('values', []))}")
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
        
        print(f"Municipios obtenidos: {len(result.get('values', []))}")
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
        
        print(f"IPS obtenidas: {len(result.get('values', []))}")
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
    # PARÁMETROS GEOGRÁFICOS EXISTENTES
    departamento: Optional[str] = Query(None, description="Filtrar por departamento específico"),
    municipio: Optional[str] = Query(None, description="Filtrar por municipio específico"),
    ips: Optional[str] = Query(None, description="Filtrar por IPS específica"),
    # ✅ NUEVO PARÁMETRO PARA NUMERADOR/DENOMINADOR
    corte_fecha: str = Query("2025-07-31", description="Fecha de corte para cálculo de edades (YYYY-MM-DD)")
):
    """
    🆕 Genera reporte CON NUMERADOR/DENOMINADOR POR RANGO DE EDAD ESPECÍFICO
    
    NUEVA LÓGICA:
    - Busca columnas que coincidan con keywords (medicina, enfermería, etc.)
    - Para cada columna: extrae rango de edad del nombre
    - DENOMINADOR: población total en ese rango específico
    - NUMERADOR: población en rango que SÍ tiene datos en la columna
    - COBERTURA: (numerador/denominador) * 100
    
    Parámetros:
    - keywords: Lista de palabras clave separadas por comas
    - min_count: Filtro mínimo de conteo (aplicado al numerador)
    - corte_fecha: Fecha para cálculo de edades en formato YYYY-MM-DD
    - departamento, municipio, ips: Filtros geográficos
    """
    try:
        print(f"\n📊 ========== GET /report/{filename} NUMERADOR/DENOMINADOR ==========")
        print(f"🗓️ Fecha corte: {corte_fecha}")
        print(f"🔍 Keywords: {keywords}")
        print(f"🗺️ Filtros: Dept={departamento}, Mun={municipio}, IPS={ips}")
        print(f"📊 Min count: {min_count}, Temporal: {include_temporal}")
        
        # Validar formato de fecha
        try:
            from datetime import datetime
            datetime.strptime(corte_fecha, '%Y-%m-%d')
        except ValueError:
            raise HTTPException(
                status_code=400, 
                detail=f"Formato de fecha inválido: {corte_fecha}. Use YYYY-MM-DD"
            )
        
        # Procesar keywords
        kw_list = None
        if keywords and keywords.strip():
            kw_list = [k.strip().lower() for k in keywords.split(",") if k.strip()]
            print(f"🎯 Keywords procesadas: {kw_list}")
        
        # ✅ LLAMAR CON PARÁMETRO corte_fecha
        result = technical_note_controller.get_keyword_age_report(
            filename=filename,
            keywords=kw_list,
            min_count=min_count,
            include_temporal=include_temporal,
            departamento=departamento,
            municipio=municipio,
            ips=ips,
            corte_fecha=corte_fecha  # ✅ NUEVO PARÁMETRO
        )
        
        # ✅ LOGGING EXTENDIDO CON NUMERADOR/DENOMINADOR
        items_count = len(result.get('items', []))
        global_stats = result.get('global_statistics', {})
        total_denominador = global_stats.get('total_denominador_global', 0)
        total_numerador = global_stats.get('total_numerador_global', 0)
        cobertura_global = global_stats.get('cobertura_global_porcentaje', 0.0)
        
        print(f"✅ ========== REPORTE COMPLETADO ==========")
        print(f"📊 Items encontrados: {items_count}")
        print(f"📊 DENOMINADOR GLOBAL: {total_denominador:,}")
        print(f"✅ NUMERADOR GLOBAL: {total_numerador:,}")  
        print(f"📈 COBERTURA GLOBAL: {cobertura_global}%")
        print(f"🎯 Método: {result.get('metodo', 'No especificado')}")
        print(f"============================================")
        
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
    
@router.get("/age-ranges/{filename}")
def get_age_ranges(
    filename: str,
    corte_fecha: Optional[str] = Query("2025-07-31", description="Fecha de corte en formato YYYY-MM-DD")
):
    """Obtiene rangos de edades únicos para filtros de inasistentes"""
    try:
        print(f"📅 GET /age-ranges/{filename} con fecha corte: {corte_fecha}")
        
        # Validar formato de fecha
        try:
            from datetime import datetime
            datetime.strptime(corte_fecha, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400, 
                detail="Fecha de corte debe tener formato YYYY-MM-DD"
            )
        
        result = technical_note_controller.get_age_ranges(
            filename=filename,
            corte_fecha=corte_fecha
        )
        
        if not result.get("success"):
            raise HTTPException(status_code=500, detail=result.get("error", "Error obteniendo rangos"))
        
        years_count = len(result["age_ranges"]["years"])
        months_count = len(result["age_ranges"]["months"])
        print(f"Rangos enviados: {years_count} años únicos, {months_count} meses únicos")
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en /age-ranges/{filename}: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.post("/inasistentes-report/{filename}")
def get_inasistentes_report(
    filename: str,
    request: Dict[str, Any],
    corte_fecha: Optional[str] = Query("2025-07-31", description="Fecha de corte en formato YYYY-MM-DD")
):
    """Genera reporte de inasistentes dinámico con descubrimiento automático de actividades"""
    try:
        print(f"🏥 POST /inasistentes-report/{filename}")
        
        # EXTRAER PARÁMETROS
        selected_months = request.get("selectedMonths", [])
        selected_years = request.get("selectedYears", [])
        selected_keywords = request.get("selectedKeywords", [])
        departamento = request.get("departamento")
        municipio = request.get("municipio")
        ips = request.get("ips")
        
        # VALIDACIONES
        if not selected_months and not selected_years:
            raise HTTPException(
                status_code=400, 
                detail="Debe seleccionar al menos una edad en meses o años"
            )
        
        if not isinstance(selected_keywords, list):
            raise HTTPException(
                status_code=400, 
                detail="selectedKeywords debe ser un array"
            )
        
        result = technical_note_controller.get_inasistentes_report(
            filename=filename,
            selected_months=selected_months,
            selected_years=selected_years,
            selected_keywords=selected_keywords,
            corte_fecha=corte_fecha,
            departamento=departamento,
            municipio=municipio,
            ips=ips
        )
        
        if not result.get("success"):
            raise HTTPException(status_code=500, detail=result.get("error", "Error generando reporte"))
        
        # ACTUALIZAR CONTADORES PARA NUEVA ESTRUCTURA
        total_inasistentes = result.get("resumen_general", {}).get("total_inasistentes_global", 0)
        actividades_evaluadas = result.get("resumen_general", {}).get("total_actividades_evaluadas", 0)
        print(f"Reporte dinámico generado: {total_inasistentes} inasistentes, {actividades_evaluadas} actividades")
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en /inasistentes-report/{filename}: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# export reporte de inasistentes

@router.post("/inasistentes-report/{filename}/export-csv")
def export_inasistentes_csv(
    filename: str,
    request: Dict[str, Any],
    corte_fecha: Optional[str] = Query("2025-07-31", description="Fecha de corte en formato YYYY-MM-DD")
):
    """Exporta reporte de inasistentes a CSV"""
    try:
        print(f"📥 POST /inasistentes-report/{filename}/export-csv")
        
        # EXTRAER PARÁMETROS
        selected_months = request.get("selectedMonths", [])
        selected_years = request.get("selectedYears", [])
        selected_keywords = request.get("selectedKeywords", [])
        departamento = request.get("departamento")
        municipio = request.get("municipio")
        ips = request.get("ips")
        
        # VALIDACIONES
        if not selected_months and not selected_years:
            raise HTTPException(
                status_code=400, 
                detail="Debe seleccionar al menos una edad en meses o años"
            )
        
        # EXPORTAR CSV
        csv_response = technical_note_controller.export_inasistentes_csv(
            filename=filename,
            selected_months=selected_months,
            selected_years=selected_years,
            selected_keywords=selected_keywords,
            corte_fecha=corte_fecha,
            departamento=departamento,
            municipio=municipio,
            ips=ips
        )
        
        return csv_response
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en /export-csv/{filename}: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@reports_router.post("/generate-and-export")
async def generate_and_export_advanced_report(
    request_data: dict,
    background_tasks: BackgroundTasks,
):
    try:
        start_time = datetime.now()
        
        # ✅ Extraer parámetros
        data_source = request_data.get('data_source')
        filename = request_data.get('filename', 'reporte')
        export_csv = request_data.get('export_csv', True)
        export_pdf = request_data.get('export_pdf', True)
        
        # ✅ Generar reporte con controlador
        report_data = technical_note_controller.get_keyword_age_report(
            filename=data_source,
            keywords=request_data.get('keywords'),
            min_count=request_data.get('min_count', 0),
            include_temporal=request_data.get('include_temporal', True),
            departamento=request_data.get('geographic_filters', {}).get('departamento'),
            municipio=request_data.get('geographic_filters', {}).get('municipio'),
            ips=request_data.get('geographic_filters', {}).get('ips'),
            corte_fecha=request_data.get('corte_fecha', '2025-07-31')
        )
        
        # ✅ USAR SERVICIO DE EXPORTACIÓN
        export_result = report_exporter.export_report(
            report_data=report_data,
            base_filename=filename,
            export_csv=export_csv,
            export_pdf=export_pdf,
            include_temporal=True
        )
        
        # ✅ Programar limpieza
        background_tasks.add_task(report_exporter.cleanup_old_temp_files, 30)
        
        return export_result
        
    except Exception as e:
        print(f"❌ Error en endpoint: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.get("/reports/download/{file_id}")
async def download_report_file(file_id: str):
    """Descargar archivo usando el servicio"""
    try:
        file_info = report_exporter.get_temp_file(file_id)
        
        if not file_info:
            raise HTTPException(status_code=404, detail="Archivo no encontrado")
        
        return FileResponse(
            path=file_info['file_path'],
            filename=file_info['original_name'],
            media_type="application/octet-stream"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")



router.include_router(reports_router)