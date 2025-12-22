# api/technical_note_routes.py - SIN ENDPOINTS DE LIMPIEZA MANUAL
from datetime import datetime
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from typing import Any, Dict, Optional
import json
import os
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, StreamingResponse
from controllers.nt_rpms_controller import nt_rpms_controller
from controllers.technical_note_controller.technical_note import technical_note_controller
from models.schemas import LocalPathRequest, NTRPMSProcessRequest, NetworkPathRequest
from services.technical_note_services.report_service_aux.report_exporter import ReportExporter
from services.technical_note_services.cache_cleanup_service import cache_access_tracker

report_exporter = ReportExporter()
router = APIRouter()


mandatory_date = "Fecha de corte OBLIGATORIA (YYYY-MM-DD)"


# 🔥 NOTA: Los endpoints de limpieza manual fueron ELIMINADOS
# La limpieza ahora es 100% automática desde el backend basada en TTL


# ========== ENDPOINTS NT RPMS - RED COMPARTIDA ==========

@router.post("/nt-rpms/process-network", tags=["NT RPMS"])
async def process_network_nt_rpms(request: NetworkPathRequest) -> Dict[str, Any]:
    """
    Procesa archivos NT RPMS desde una carpeta compartida en red
    
    **Ejemplos de rutas válidas:**
    - Windows UNC: `\\\\192.168.1.100\\NT_RPMS_Share`
    - Drive mapeado: `Z:\\NT_RPMS`
    - Linux mount: `/mnt/smb/nt_rpms`
    
    **Requisitos:**
    1. La carpeta debe estar compartida en el cliente
    2. El servidor debe tener permisos de lectura
    3. El firewall debe permitir SMB/CIFS
    4. Ambos equipos en la misma red
    
    Args:
        request: Objeto con network_path (ruta UNC)
    
    Returns:
        Resultado del procesamiento con información de red
    """
    try:
        print(f"\n{'='*60}")
        print("ENDPOINT: POST /nt-rpms/process-network")
        print(f"{'='*60}")
        print(f"Ruta de red solicitada: {request.network_path}")
        
        # Procesar usando el método de red del controlador
        result = nt_rpms_controller.process_network_path(request.network_path)
        
        if not result.get("success"):
            raise HTTPException(
                status_code=400,
                detail={
                    "error": result.get("error"),
                    "suggestion": result.get("suggestion"),
                    "total_time": result.get("total_time")
                }
            )
        
        # 🔥 TRACKING: Registrar archivos generados
        if result.get("csv_path"):
            cache_access_tracker.track_access(result["csv_path"], "extract_info_nt")
        if result.get("parquet_path"):
            cache_access_tracker.track_access(result["parquet_path"], "parquet_cache")
        
        print(f"\n✓ Procesamiento desde red completado exitosamente")
        print(f"  - Carpeta red: {request.network_path}")
        print(f"  - CSV: {result['csv_path']}")
        print(f"  - Parquet: {result['parquet_path']}")
        print(f"  - Registros: {result['total_rows']:,}")
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"✗ Error inesperado en /nt-rpms/process-network: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error inesperado: {str(e)}"
        )


@router.post("/nt-rpms/process-local", tags=["NT RPMS"])
async def process_local_nt_rpms(request: LocalPathRequest) -> Dict[str, Any]:
    """
    Procesa archivos NT RPMS desde una carpeta local del servidor
    
    **Nota:** Esta ruta solo funciona si la carpeta está físicamente en el servidor
    
    Args:
        request: Objeto con folder_path (ruta local del servidor)
    
    Returns:
        Resultado del procesamiento
    """
    try:
        print(f"\n{'='*60}")
        print("ENDPOINT: POST /nt-rpms/process-local")
        print(f"{'='*60}")
        print(f"Carpeta local solicitada: {request.folder_path}")
        
        # Validar que la carpeta existe
        if not os.path.isdir(request.folder_path):
            raise HTTPException(
                status_code=400,
                detail=f"La carpeta no existe: {request.folder_path}"
            )
        
        # Contar archivos Excel en la carpeta
        excel_files = [f for f in os.listdir(request.folder_path) 
                      if f.lower().endswith(('.xlsx', '.xls')) and not f.startswith('~$')]
        
        if not excel_files:
            raise HTTPException(
                status_code=400,
                detail=f"No se encontraron archivos Excel en la carpeta: {request.folder_path}"
            )
        
        print(f"📁 Archivos Excel encontrados: {len(excel_files)}")
        
        # Procesar carpeta usando el controlador
        result = nt_rpms_controller.process_nt_rpms_folder(request.folder_path)
        
        if not result.get("success"):
            raise HTTPException(
                status_code=500,
                detail=result.get("error", "Error desconocido en procesamiento")
            )
        
        # 🔥 TRACKING: Registrar archivos generados
        if result.get("csv_path"):
            cache_access_tracker.track_access(result["csv_path"], "extract_info_nt")
        if result.get("parquet_path"):
            cache_access_tracker.track_access(result["parquet_path"], "parquet_cache")
        
        print(f"\n✓ Procesamiento local completado exitosamente")
        print(f"  - CSV: {result['csv_path']}")
        print(f"  - Parquet: {result['parquet_path']}")
        print(f"  - Registros: {result['total_rows']:,}")
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"✗ Error inesperado en /nt-rpms/process-local: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error inesperado: {str(e)}"
        )


# ========== ENDPOINT ANTIGUO (MANTENER POR COMPATIBILIDAD) ==========

@router.post("/nt-rpms/process", tags=["NT RPMS"])
async def process_nt_rpms_folder(request: NTRPMSProcessRequest) -> Dict[str, Any]:
    """
    **[DEPRECATED]** Usar /nt-rpms/process-local o /nt-rpms/process-network
    
    Procesa archivos NT RPMS de una carpeta y los convierte a Parquet
    
    Args:
        request: Contiene folder_path con la ruta de la carpeta
    
    Returns:
        Resultado del procesamiento con rutas de archivos generados
    """
    try:
        print(f"\n{'='*60}")
        print("ENDPOINT: POST /nt-rpms/process [DEPRECATED]")
        print(f"⚠️  ADVERTENCIA: Usa /nt-rpms/process-local o /nt-rpms/process-network")
        print(f"{'='*60}")
        print(f"Carpeta solicitada: {request.folder_path}")
        
        # Validar que la carpeta existe
        if not os.path.isdir(request.folder_path):
            raise HTTPException(
                status_code=400,
                detail=f"La carpeta no existe: {request.folder_path}"
            )
        
        # Contar archivos Excel en la carpeta
        excel_files = [f for f in os.listdir(request.folder_path) 
                      if f.lower().endswith(('.xlsx', '.xls')) and not f.startswith('~$')]
        
        if not excel_files:
            raise HTTPException(
                status_code=400,
                detail=f"No se encontraron archivos Excel en la carpeta: {request.folder_path}"
            )
        
        print(f"📁 Archivos Excel encontrados: {len(excel_files)}")
        
        # Procesar carpeta usando el controlador global
        result = nt_rpms_controller.process_nt_rpms_folder(request.folder_path)
        
        if not result.get("success"):
            raise HTTPException(
                status_code=500,
                detail=result.get("error", "Error desconocido en procesamiento")
            )
        
        # 🔥 TRACKING: Registrar archivos generados
        if result.get("csv_path"):
            cache_access_tracker.track_access(result["csv_path"], "extract_info_nt")
        if result.get("parquet_path"):
            cache_access_tracker.track_access(result["parquet_path"], "parquet_cache")
        
        print(f"\n✓ Procesamiento completado exitosamente")
        print(f"  - CSV: {result['csv_path']}")
        print(f"  - Parquet: {result['parquet_path']}")
        print(f"  - Registros: {result['total_rows']:,}")
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"✗ Error inesperado en /nt-rpms/process: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error inesperado: {str(e)}"
        )


@router.get("/nt-rpms/status/{file_hash}", tags=["NT RPMS"])
async def get_nt_rpms_processing_status(file_hash: str) -> Dict[str, Any]:
    """
    Obtiene el estado de un procesamiento NT RPMS por su hash
    
    Args:
        file_hash: Hash del archivo procesado
    
    Returns:
        Estado del procesamiento y metadata
    """
    try:
        result = nt_rpms_controller.get_processing_status(file_hash)
        
        if not result.get("success"):
            raise HTTPException(
                status_code=404,
                detail=result.get("error", "Procesamiento no encontrado")
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error obteniendo estado: {str(e)}"
        )


@router.get("/nt-rpms/list-processed", tags=["NT RPMS"])
async def list_processed_nt_rpms() -> Dict[str, Any]:
    """
    Lista todos los archivos NT RPMS procesados disponibles
    
    Returns:
        Lista de archivos procesados con metadata
    """
    try:
        result = nt_rpms_controller.list_processed_files()
        
        if not result.get("success"):
            raise HTTPException(
                status_code=500,
                detail=result.get("error", "Error listando archivos")
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error listando archivos procesados: {str(e)}"
        )


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
        
        # 🔥 TRACKING: Registrar acceso al archivo técnico
        file_path = os.path.join("technical_note", filename)
        if os.path.exists(file_path):
            cache_access_tracker.track_access(file_path, "technical_note")
        
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
        # 🔥 TRACKING: Registrar acceso
        file_path = os.path.join("technical_note", filename)
        if os.path.exists(file_path):
            cache_access_tracker.track_access(file_path, "technical_note")
        
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
    try:
        print(f"\n{'='*60}")
        print(f"GET /report/{filename}")
        print(f"Fecha corte: {corte_fecha}")
        print(f"{'='*60}")
        
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
        
        # Convertir a formato JSON-serializable
        try:
            encoded_result = jsonable_encoder(result)
            
            return JSONResponse(
                content=encoded_result,
                status_code=200,
                headers={
                    "Content-Type": "application/json; charset=utf-8",
                    "Cache-Control": "no-cache, no-store, must-revalidate"
                }
            )
            
        except Exception as encode_error:
            print(f"✗ Error codificando respuesta: {encode_error}")
            import traceback
            traceback.print_exc()
            
            # Intento alternativo
            try:
                json_str = json.dumps(result, default=str, ensure_ascii=False)
                json_data = json.loads(json_str)
                
                return JSONResponse(
                    content=json_data,
                    status_code=200
                )
            except Exception as fallback_error:
                print(f"✗ Error en fallback: {fallback_error}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Error serializando respuesta: {str(fallback_error)}"
                )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"\n✗ ERROR EN ENDPOINT:")
        print(f"   Tipo: {type(e)}")
        print(f"   Mensaje: {str(e)}")
        import traceback
        traceback.print_exc()
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
    corte_fecha: str = Query(..., description="Fecha de corte en formato YYYY-MM-DD")
):
    """Genera reporte de inasistentes mes a mes"""
    try:
        print(f"POST /inasistentes-report/{filename}")
        print(f"Fecha de corte: {corte_fecha}")
        
        # Validar formato de fecha
        try:
            datetime.strptime(corte_fecha, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Fecha debe tener formato YYYY-MM-DD"
            )
        
        # Extraer keywords del request
        selected_keywords = request.get("selectedKeywords", ["medicina"])
        
        # Construir resultado
        result = technical_note_controller.get_inasistentes_report(
            filename=filename,
            keywords=selected_keywords,
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
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.post("/inasistentes-report/{filename}/export-csv")
def export_inasistentes_csv(
    filename: str,
    request: Dict[str, Any],
    corte_fecha: str = Query(..., description="Fecha de corte en formato YYYY-MM-DD"),
    encoding: str = Query(default="utf-8-sig", description="Encoding del CSV")
):
    """Exporta reporte de inasistentes a CSV"""
    try:
        print(f"POST /inasistentes-report/{filename}/export-csv")
        print(f"Fecha de corte: {corte_fecha}")
        
        # Validar formato de fecha
        try:
            datetime.strptime(corte_fecha, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Fecha debe tener formato YYYY-MM-DD"
            )
        
        # Extraer keywords del request
        selected_keywords = request.get("selectedKeywords", ["medicina"])
        
        # Exportar
        csv_response = technical_note_controller.export_inasistentes_csv(
            filename=filename,
            keywords=selected_keywords,
            corte_fecha=corte_fecha,
            departamento=request.get("departamento"),
            municipio=request.get("municipio"),
            ips=request.get("ips"),
            encoding=encoding
        )
        
        return csv_response
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
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
            include_detailed=export_options.get('include_detailed', True)
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
