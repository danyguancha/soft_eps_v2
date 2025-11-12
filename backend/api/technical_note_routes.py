# api/technical_note_routes.py - REFACTORIZADO CON LIMPIEZA DE CACHE
from datetime import datetime
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from typing import Any, Dict, List, Optional
import json
import os
import shutil
import pandas as pd


from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, validator


from controllers.technical_note_controller.technical_note import technical_note_controller
from services.technical_note_services.report_service_aux.report_exporter import ReportExporter
from services.duckdb_service.duckdb_service import duckdb_service


report_exporter = ReportExporter()
router = APIRouter()


# ========== MODELOS PYDANTIC ==========
mandatory_date = "Fecha de corte OBLIGATORIA (YYYY-MM-DD)"

class GeographicFiltersModel(BaseModel):
    """Filtros geográficos para el reporte"""
    departamento: Optional[str] = Field(None, description="Departamento específico")
    municipio: Optional[str] = Field(None, description="Municipio específico")
    ips: Optional[str] = Field(None, description="IPS específica")


class AdvancedReportRequestModel(BaseModel):
    """Modelo de solicitud para generar reporte avanzado"""
    data_source: str = Field(..., description="Nombre de la tabla/fuente de datos")
    filename: str = Field(..., description="Nombre base del archivo de salida")
    keywords: Optional[List[str]] = Field(default=[], description="Lista de palabras clave")
    min_count: int = Field(default=0, description="Conteo mínimo para incluir resultados")
    include_temporal: bool = Field(default=True, description="Incluir análisis temporal")
    geographic_filters: Optional[GeographicFiltersModel] = Field(default=None)
    corte_fecha: str = Field(..., description=mandatory_date)
    
    @validator('corte_fecha')
    def validate_corte_fecha(cls, v):
        try:
            datetime.strptime(v, '%Y-%m-%d')
            return v
        except ValueError:
            raise ValueError('corte_fecha debe estar en formato YYYY-MM-DD')


# ========== ENDPOINTS DE LIMPIEZA DE CACHE ==========


@router.post("/cache/cleanup-all")
async def cleanup_all_cache() -> Dict[str, Any]:
    """
    Endpoint para limpiar todos los directorios de cache y archivos precargados.
    Llamado desde el frontend al iniciar la aplicación.
    """
    try:
        print("🧹 Iniciando limpieza completa de cache...")
        
        # Directorios a limpiar
        directories_to_clean = [
            "duckdb_storage",
            "metadata_cache",
            "parquet_cache",
            "technical_note"
        ]
        
        cleaned_dirs = []
        errors = []
        
        # Limpiar cada directorio
        for directory in directories_to_clean:
            try:
                if os.path.exists(directory):
                    # Eliminar directorio completo
                    shutil.rmtree(directory)
                    print(f"✓ Directorio eliminado: {directory}")
                
                # Recrear directorio vacío
                os.makedirs(directory, exist_ok=True)
                print(f"✓ Directorio recreado: {directory}")
                cleaned_dirs.append(directory)
                
            except Exception as e:
                error_msg = f"Error limpiando {directory}: {str(e)}"
                print(f"✗ {error_msg}")
                errors.append(error_msg)
                # Asegurar que el directorio exista
                os.makedirs(directory, exist_ok=True)
        
        # Limpiar tablas cargadas en memoria de DuckDB
        tables_count = 0
        if hasattr(duckdb_service, 'loaded_tables'):
            tables_count = len(duckdb_service.loaded_tables)
            duckdb_service.loaded_tables.clear()
            print(f"✓ {tables_count} tablas eliminadas de memoria DuckDB")
        
        # Limpiar archivos técnicos cargados
        tech_files_count = 0
        if hasattr(technical_note_controller, 'loaded_technical_files'):
            tech_files_count = len(technical_note_controller.loaded_technical_files)
            technical_note_controller.loaded_technical_files.clear()
            print(f"✓ {tech_files_count} archivos técnicos eliminados de memoria")
        
        # Reiniciar conexión DuckDB para liberar recursos
        try:
            if hasattr(duckdb_service, 'restart_connection'):
                duckdb_service.restart_connection()
                print("✓ Conexión DuckDB reiniciada")
        except Exception as e:
            error_msg = f"Error reiniciando conexión DuckDB: {str(e)}"
            print(f"✗ {error_msg}")
            errors.append(error_msg)
        
        success_message = "Cache limpiado completamente" if len(errors) == 0 else "Cache limpiado con algunos errores"
        print(f"✓ {success_message}")
        
        return {
            "success": len(errors) == 0,
            "message": success_message,
            "cleaned_directories": cleaned_dirs,
            "tables_cleared": tables_count,
            "technical_files_cleared": tech_files_count,
            "errors": errors if errors else None,
            "timestamp": str(pd.Timestamp.now())
        }
        
    except Exception as e:
        print(f"✗ Error crítico en cleanup_all_cache: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error limpiando cache: {str(e)}"
        )


@router.get("/cache/status")
async def get_cache_status() -> Dict[str, Any]:
    """
    Obtiene el estado actual del cache (útil para debugging y monitoreo)
    """
    try:
        directories_status = {}
        
        # Verificar estado de cada directorio
        for directory in ["duckdb_storage", "metadata_cache", "parquet_cache", "technical_note"]:
            if os.path.exists(directory):
                file_count = sum(len(files) for _, _, files in os.walk(directory))
                dir_size = sum(
                    os.path.getsize(os.path.join(root, file))
                    for root, _, files in os.walk(directory)
                    for file in files
                )
                directories_status[directory] = {
                    "exists": True,
                    "file_count": file_count,
                    "size_mb": round(dir_size / (1024 * 1024), 2)
                }
            else:
                directories_status[directory] = {
                    "exists": False,
                    "file_count": 0,
                    "size_mb": 0
                }
        
        # Estado de memoria
        loaded_tables_count = len(duckdb_service.loaded_tables) if hasattr(duckdb_service, 'loaded_tables') else 0
        loaded_technical_count = len(technical_note_controller.loaded_technical_files) if hasattr(technical_note_controller, 'loaded_technical_files') else 0
        
        return {
            "success": True,
            "directories": directories_status,
            "memory_state": {
                "loaded_tables_count": loaded_tables_count,
                "loaded_technical_files_count": loaded_technical_count,
                "duckdb_available": duckdb_service.is_available()
            },
            "timestamp": str(pd.Timestamp.now())
        }
        
    except Exception as e:
        print(f"Error obteniendo estado del cache: {e}")
        raise HTTPException(status_code=500, detail=f"Error obteniendo estado: {str(e)}")


# ========== ENDPOINTS PRINCIPALES ==========


@router.get("/available")
def get_available_technical_files():
    """Lista archivos técnicos disponibles"""
    try:
        return technical_note_controller.get_available_static_files()
    except Exception as e:
        print(f"Error en /available: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.get("/data/{filename}")
def get_technical_file_data_with_excel_filters(
    filename: str, 
    page: int = Query(1, ge=1),
    page_size: int = Query(1000, ge=10, le=2000),
    sheet_name: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    sort_by: Optional[str] = Query(None),
    sort_order: Optional[str] = Query("asc", regex="^(asc|desc)$"),
    filters: Optional[str] = Query(None)
):
    """Obtiene datos con filtros estilo Excel"""
    try:
        print(f"GET /data/{filename} - página {page}")
        
        parsed_filters = None
        if filters:
            try:
                parsed_filters = json.loads(filters)
            except json.JSONDecodeError as e:
                print(f"Error parseando filtros: {e}")
        
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
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error en /data/{filename}: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.get("/metadata/{filename}")
def get_technical_file_metadata(filename: str):
    """Metadatos del archivo"""
    try:
        return technical_note_controller.get_technical_file_metadata(filename)
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error en /metadata/{filename}: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.get("/columns/{filename}")
def get_file_columns(filename: str):
    """Obtiene columnas de un archivo"""
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
        print(f"Error en /columns/{filename}: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# ========== ENDPOINTS GEOGRÁFICOS ==========


@router.get("/geographic/{filename}/departamentos")
def get_departamentos(filename: str):
    """Obtiene departamentos únicos"""
    try:
        result = technical_note_controller.get_geographic_values(
            filename=filename,
            geo_type='departamento'
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.get("/geographic/{filename}/municipios")
def get_municipios(
    filename: str,
    departamento: Optional[str] = Query(None)
):
    """Obtiene municipios filtrados por departamento"""
    try:
        result = technical_note_controller.get_geographic_values(
            filename=filename,
            geo_type='municipios',
            departamento=departamento
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.get("/geographic/{filename}/ips")
def get_ips(
    filename: str,
    departamento: Optional[str] = Query(None),
    municipio: Optional[str] = Query(None)
):
    """Obtiene IPS filtradas"""
    try:
        result = technical_note_controller.get_geographic_values(
            filename=filename,
            geo_type='ips',
            departamento=departamento,
            municipio=municipio
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# ========== ENDPOINT DE REPORTE PRINCIPAL ==========


@router.get("/report/{filename}")
def get_keyword_age_report(
    filename: str,
    keywords: Optional[str] = Query(None),
    min_count: int = Query(0, ge=0),
    include_temporal: bool = Query(True),
    departamento: Optional[str] = Query(None),
    municipio: Optional[str] = Query(None),
    ips: Optional[str] = Query(None),
    corte_fecha: str = Query(..., description=mandatory_date)
):
    """Genera reporte con numerador/denominador y fecha de corte dinámica"""
    try:
        print(f"\n========== GET /report/{filename} ==========")
        print(f"Fecha corte: {corte_fecha}")
        
        # Validar formato de fecha
        try:
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
        
        result = technical_note_controller.get_keyword_age_report(
            filename=filename,
            keywords=kw_list,
            min_count=min_count,
            include_temporal=include_temporal,
            departamento=departamento,
            municipio=municipio,
            ips=ips,
            corte_fecha=corte_fecha
        )
        
        items_count = len(result.get('items', []))
        global_stats = result.get('global_statistics', {})
        
        print(f"Reporte completado: {items_count} items")
        print(f"Cobertura global: {global_stats.get('cobertura_global_porcentaje', 0)}%")
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error en /report/{filename}: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# ========== ENDPOINTS DE VALORES ÚNICOS ==========


@router.get("/unique-values/{filename}/{column_name}")
def get_column_unique_values(
    filename: str,
    column_name: str,
    sheet_name: Optional[str] = Query(None),
    limit: int = Query(1000, ge=10, le=5000)
):
    """Obtiene valores únicos de una columna"""
    try:
        result = technical_note_controller.get_column_unique_values(
            filename, column_name, sheet_name, limit
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# ========== ENDPOINTS DE RANGOS DE EDAD ==========


@router.get("/age-ranges/{filename}")
def get_age_ranges(
    filename: str,
    corte_fecha: str = Query(..., description=mandatory_date)
):
    """Obtiene rangos de edades únicos con fecha dinámica"""
    try:
        print(f"GET /age-ranges/{filename} con fecha: {corte_fecha}")
        
        # Validar formato
        try:
            datetime.strptime(corte_fecha, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400, 
                detail="Fecha debe tener formato YYYY-MM-DD"
            )
        
        result = technical_note_controller.get_age_ranges(
            filename=filename,
            corte_fecha=corte_fecha
        )
        
        if not result.get("success"):
            raise HTTPException(status_code=500, detail=result.get("error"))
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# ========== ENDPOINTS DE INASISTENTES ==========


@router.post("/inasistentes-report/{filename}")
def get_inasistentes_report(
    filename: str,
    request: Dict[str, Any],
    corte_fecha: str = Query(..., description=mandatory_date)
):
    """Genera reporte de inasistentes con fecha dinámica"""
    try:
        print(f"POST /inasistentes-report/{filename}")
        
        selected_months = request.get("selectedMonths", [])
        selected_years = request.get("selectedYears", [])
        selected_keywords = request.get("selectedKeywords", [])
        
        # Validar formato de fecha
        try:
            datetime.strptime(corte_fecha, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Fecha debe tener formato YYYY-MM-DD"
            )
        
        if not selected_months and not selected_years:
            raise HTTPException(
                status_code=400, 
                detail="Debe seleccionar al menos una edad"
            )
        
        result = technical_note_controller.get_inasistentes_report(
            filename=filename,
            selected_months=selected_months,
            selected_years=selected_years,
            selected_keywords=selected_keywords,
            corte_fecha=corte_fecha,
            departamento=request.get("departamento"),
            municipio=request.get("municipio"),
            ips=request.get("ips")
        )
        
        if not result.get("success"):
            raise HTTPException(status_code=500, detail=result.get("error"))
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.post("/inasistentes-report/{filename}/export-csv")
def export_inasistentes_csv(
    filename: str,
    request: Dict[str, Any],
    corte_fecha: str = Query(..., description=mandatory_date)
):
    """Exporta reporte de inasistentes a CSV"""
    try:
        print(f"POST /inasistentes-report/{filename}/export-csv")
        
        selected_months = request.get("selectedMonths", [])
        selected_years = request.get("selectedYears", [])
        selected_keywords = request.get("selectedKeywords", [])
        
        if not selected_months and not selected_years:
            raise HTTPException(
                status_code=400, 
                detail="Debe seleccionar al menos una edad"
            )
        
        csv_response = technical_note_controller.export_inasistentes_csv(
            filename=filename,
            selected_months=selected_months,
            selected_years=selected_years,
            selected_keywords=selected_keywords,
            corte_fecha=corte_fecha,
            departamento=request.get("departamento"),
            municipio=request.get("municipio"),
            ips=request.get("ips")
        )
        
        return csv_response
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# ========== ENDPOINTS DE EXPORTACIÓN ==========


@router.get("/reports/download/{file_id}")
async def download_report_file(file_id: str):
    """Descargar archivo desde memoria"""
    try:
        file_info = report_exporter.get_temp_file(file_id)
        
        if not file_info:
            raise HTTPException(status_code=404, detail="Archivo no encontrado")
        
        content = file_info['content']
        filename = file_info['filename']
        content_type = file_info['content_type']
        
        content.seek(0)
        
        return StreamingResponse(
            content,
            media_type=content_type,
            headers={
                'Content-Disposition': f'attachment; filename="{filename}"'
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error descargando archivo: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.post("/reports/export-current")
async def export_current_report(
    request_data: dict,
    background_tasks: BackgroundTasks,
):
    """
    ENDPOINT CRÍTICO: Exporta el reporte actual visible en el frontend
    """
    try:
        print("📤 Exportando reporte actual del frontend")
        
        report_data = request_data.get('report_data')
        filename = request_data.get('filename', 'reporte')
        request_data.get('export_type', 'all')
        export_options = request_data.get('export_options', {})
        
        if not report_data:
            raise HTTPException(
                status_code=400,
                detail="report_data es obligatorio"
            )
        
        corte_fecha = report_data.get('corte_fecha')
        if not corte_fecha:
            raise HTTPException(
                status_code=400,
                detail="corte_fecha es obligatorio en report_data"
            )
        
        print(f"Exportando: {len(report_data.get('items', []))} items")
        print(f"Fecha corte: {corte_fecha}")
        
        export_result = report_exporter.export_report(
            report_data=report_data,
            base_filename=filename,
            export_csv=export_options.get('export_csv', True),
            export_pdf=export_options.get('export_pdf', False),
            include_temporal=export_options.get('include_temporal', True)
        )
        
        background_tasks.add_task(report_exporter.cleanup_old_temp_files, 30)
        
        print(f"Exportación completada: {len(export_result.get('files', {}))} archivos")
        
        return export_result
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error en export-current: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
