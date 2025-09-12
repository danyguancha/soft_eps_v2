import io
from typing import Any, Dict, List, Optional
import unicodedata
import re

from fastapi.responses import StreamingResponse
import pandas as pd

from services.duckdb_service.duckdb_service import duckdb_service
from services.technical_note_services.data_source_service import DataSourceService
from controllers.technical_note_controller.absent_user.reports_activity import ReportActivity
from controllers.technical_note_controller.absent_user.activity_column import ActivityColumn

class AbsentUserController:

    def _normalize_keyword(self, keyword: str) -> str:
        """Normaliza palabra clave removiendo tildes y convirtiendo a minúsculas"""
        normalized = ''.join(c for c in unicodedata.normalize('NFD', keyword)
                            if unicodedata.category(c) != 'Mn').lower().strip()
        
        if normalized.endswith('ica'):
            normalized = normalized[:-3] + 'ia'
        
        return normalized

    def _discover_activity_columns(self, data_source: str, keywords: List[str]) -> Dict[str, List[str]]:
        """Descubre dinámicamente columnas de actividades basadas en palabras clave"""
        try:
            # EXTRAER RUTA LIMPIA DEL DATA_SOURCE
            if data_source.startswith("read_parquet('") and data_source.endswith("')"):
                clean_path = data_source[14:-2]
            elif data_source.startswith("'") and data_source.endswith("'"):
                clean_path = data_source[1:-1]
            else:
                clean_path = data_source
            
            # SINTAXIS OFICIAL DE DUCKDB PARA DESCRIBIR ARCHIVOS
            columns_sql = f"DESCRIBE SELECT * FROM '{clean_path}'"
            
            print(f"🔧 SQL generado: {columns_sql}")
            
            columns_result = duckdb_service.conn.execute(columns_sql).fetchall()
            available_columns = [row[0] for row in columns_result]  
            
            print(f"🔍 DESCUBRIENDO COLUMNAS DINÁMICAMENTE ({len(available_columns)} total)")
            print(f"📋 Primeras 10 columnas: {available_columns[:10]}")
            
            discovered_mapping = {}
            
            for keyword in keywords:
                normalized_keyword = self._normalize_keyword(keyword)
                print(f"🔎 Buscando columnas para: '{keyword}' (normalizado: '{normalized_keyword}')")
                
                # DEBUG: Buscar TODAS las columnas que contienen la palabra clave
                matching_columns = []
                activity_columns = []
                
                for col in available_columns:
                    col_normalized = self._normalize_keyword(col)
                    
                    # Verificar si la columna contiene la palabra clave
                    if normalized_keyword in col_normalized:
                        matching_columns.append(col)
                        
                        # Verificar si es una columna de actividad
                        if ActivityColumn().is_activity_column(col):
                            activity_columns.append(f'"{col}"')
                            print(f"  Encontrada y válida: {col}")
                        else:
                            print(f"  ❓ Encontrada pero filtrada: {col}")
                
                if matching_columns and not activity_columns:
                    print(f"  ⚠️ Se encontraron {len(matching_columns)} columnas con '{normalized_keyword}' pero fueron filtradas")
                    print(f"  📝 Columnas encontradas:")
                    for i, col in enumerate(matching_columns[:5]):  # Mostrar primeras 5
                        print(f"    {i+1}. {col}")
                    if len(matching_columns) > 5:
                        print(f"    ... y {len(matching_columns) - 5} más")
                
                if activity_columns:
                    discovered_mapping[normalized_keyword] = activity_columns
                    print(f"📋 {keyword}: {len(activity_columns)} columnas de actividad encontradas")
                else:
                    print(f"⚠️ No se encontraron columnas de actividad para: {keyword}")
            
            return discovered_mapping
            
        except Exception as e:
            print(f"❌ Error descubriendo columnas: {e}")
            import traceback
            traceback.print_exc()
            return {} 

    def get_inasistentes_report(
        self,
        filename: str,
        selected_months: List[int],
        selected_years: List[int] = None,
        selected_keywords: List[str] = None,  
        corte_fecha: str = "2025-07-31",
        departamento: Optional[str] = None,
        municipio: Optional[str] = None,
        ips: Optional[str] = None,
        path_technical_note = ''
    ) -> Dict[str, Any]:
        """Genera reporte de inasistentes dinámico con descubrimiento automático de actividades"""
        try:
            file_key = f"technical_{filename.replace('.', '_').replace(' ', '_').replace('-', '_')}"
            data_source = DataSourceService(path_technical_note).ensure_data_source_available(filename, file_key)
            
            # DESCUBRIMIENTO DINÁMICO DE COLUMNAS
            if not selected_keywords:
                selected_keywords = ['medicina']  # Fallback por defecto
                
            discovered_columns = self._discover_activity_columns(data_source, selected_keywords)
            
            # Recopilar todas las columnas encontradas
            all_activity_columns = []
            for keyword, columns in discovered_columns.items():
                all_activity_columns.extend(columns)
            
            if not all_activity_columns:
                return {
                    "success": False,
                    "error": f"No se encontraron actividades para las palabras clave: {selected_keywords}",
                    "inasistentes_por_actividad": [],
                    "resumen_general": {}
                }
            
            print(f"🎯 TOTAL ACTIVIDADES ENCONTRADAS: {len(all_activity_columns)}")
            
            # CONSTRUIR FILTROS DE EDAD
            age_conditions = []
            
            if selected_months:
                months_list = ','.join(map(str, selected_months))
                age_conditions.append(f"""
                    date_diff('month', strptime("Fecha Nacimiento", '%d/%m/%Y'), DATE '{corte_fecha}') IN ({months_list})
                """)
            
            if selected_years:
                years_list = ','.join(map(str, selected_years))
                age_conditions.append(f"""
                    TRY_CAST(edad AS INTEGER) IN ({years_list})
                """)
            
            if not age_conditions:
                return {
                    "success": False,
                    "error": "Debe seleccionar al menos una edad en meses o años",
                    "inasistentes_por_actividad": []
                }
            
            age_filter = " OR ".join(age_conditions)
            
            # CONSTRUIR FILTROS GEOGRÁFICOS
            geo_conditions = []
            
            if departamento:
                geo_conditions.append(f'"Departamento" = \'{departamento}\'')
            
            if municipio:
                geo_conditions.append(f'"Municipio" = \'{municipio}\'')
                
            if ips:
                geo_conditions.append(f'"Nombre IPS" = \'{ips}\'')
            
            geo_filter = " AND ".join(geo_conditions) if geo_conditions else "1=1"
            
            # GENERAR REPORTES POR CADA ACTIVIDAD
            print(f"🔧 Generando reportes individuales por actividad...")
            activity_reports = ReportActivity().generate_activity_reports(
                data_source, all_activity_columns, age_filter, geo_filter, corte_fecha
            )
            
            # CALCULAR RESUMEN GENERAL
            total_inasistentes = sum(report["statistics"]["total_inasistentes"] for report in activity_reports)
            
            # Obtener todos los departamentos, municipios, IPS únicos
            all_departamentos = set()
            all_municipios = set()
            all_ips = set()
            
            for report in activity_reports:
                for inasistente in report["inasistentes"]:
                    if inasistente["departamento"]:
                        all_departamentos.add(inasistente["departamento"])
                    if inasistente["municipio"]:
                        all_municipios.add(inasistente["municipio"])
                    if inasistente["nombre_ips"]:
                        all_ips.add(inasistente["nombre_ips"])
            
            resumen_general = {
                "total_actividades_evaluadas": len(all_activity_columns),
                "total_inasistentes_global": total_inasistentes,
                "departamentos_afectados": len(all_departamentos),
                "municipios_afectados": len(all_municipios),
                "ips_afectadas": len(all_ips),
                "actividades_con_inasistentes": len([r for r in activity_reports if r["statistics"]["total_inasistentes"] > 0]),
                "actividades_sin_inasistentes": len([r for r in activity_reports if r["statistics"]["total_inasistentes"] == 0])
            }            
            return {
                "success": True,
                "filename": filename,
                "corte_fecha": corte_fecha,
                "metodo": "DESCUBRIMIENTO_DINAMICO",
                "filtros_aplicados": {
                    "selected_months": selected_months,
                    "selected_years": selected_years or [],
                    "selected_keywords": selected_keywords,
                    "departamento": departamento,
                    "municipio": municipio,
                    "ips": ips
                },
                "columnas_descubiertas": {
                    keyword: [col.replace('"', '') for col in columns] 
                    for keyword, columns in discovered_columns.items()
                },
                "inasistentes_por_actividad": activity_reports,
                "resumen_general": resumen_general,
                "engine": "DuckDB_Dynamic_Discovery"
            }
            
        except Exception as e:
            print(f"❌ Error generando reporte dinámico: {e}")
            import traceback
            traceback.print_exc()
            return {
                "success": False,
                "error": str(e),
                "inasistentes_por_actividad": []
            }

    # MÉTODO SIMPLIFICADO - Ya no es necesario mantener mapeos manuales
    def _get_keyword_column_mapping(self) -> Dict[str, List[str]]:
        """Mapeo legacy - ahora se usa descubrimiento dinámico"""
        return {
            'medicina': ['"Consulta por Medicina 1 mes"'],  # Solo como fallback
        }
    
    # exportar reporte
    def export_inasistentes_to_csv(
        self,
        filename: str,
        selected_months: List[int],
        selected_years: List[int] = None,
        selected_keywords: List[str] = None,  
        corte_fecha: str = "2025-07-31",
        departamento: Optional[str] = None,
        municipio: Optional[str] = None,
        ips: Optional[str] = None,
        path_technical_note = ''
    ) -> StreamingResponse:
        """Exporta reporte de inasistentes a formato CSV"""
        try:
            print(f"📥 Exportando reporte a CSV: {filename}")
            
            # ✅ GENERAR REPORTE COMPLETO
            report_data = self.get_inasistentes_report(
                filename, selected_months, selected_years, selected_keywords,
                corte_fecha, departamento, municipio, ips, path_technical_note
            )
            
            if not report_data.get("success"):
                raise Exception(report_data.get("error", "Error generando reporte"))
            
            # ✅ PROCESAR DATOS PARA CSV
            all_records = []
            
            # Agregar datos de cada actividad
            for activity_report in report_data["inasistentes_por_actividad"]:
                for inasistente in activity_report["inasistentes"]:
                    record = {
                        # Información personal
                        "Departamento": inasistente["departamento"],
                        "Municipio": inasistente["municipio"], 
                        "Nombre IPS": inasistente["nombre_ips"],
                        "Número Identificación": inasistente["nro_identificacion"],
                        "Primer Apellido": inasistente["primer_apellido"],
                        "Segundo Apellido": inasistente["segundo_apellido"],
                        "Primer Nombre": inasistente["primer_nombre"],
                        "Segundo Nombre": inasistente["segundo_nombre"],
                        "Fecha Nacimiento": inasistente["fecha_nacimiento"],
                        "Edad Años": inasistente["edad_anos"],
                        "Edad Meses": inasistente["edad_meses"],
                        
                        # Información de la actividad
                        "Actividad Faltante": inasistente["columna_evaluada"],
                        "Estado Actividad": inasistente["actividad_valor"],
                        "Grupo Actividad": activity_report["actividad"],
                        
                        # Información del reporte
                        "Fecha Corte": report_data["corte_fecha"],
                        "Método Generación": report_data["metodo"]
                    }
                    all_records.append(record)
            
            # ✅ CREAR DATAFRAME Y EXPORTAR A CSV
            if not all_records:
                # CSV vacío con headers
                df = pd.DataFrame(columns=[
                    "Departamento", "Municipio", "Nombre IPS", "Número Identificación",
                    "Primer Apellido", "Segundo Apellido", "Primer Nombre", "Segundo Nombre",
                    "Fecha Nacimiento", "Edad Años", "Edad Meses", "Actividad Faltante",
                    "Estado Actividad", "Grupo Actividad", "Fecha Corte", "Método Generación"
                ])
            else:
                df = pd.DataFrame(all_records)
            
            # ✅ GENERAR CSV EN MEMORIA
            buffer = io.StringIO()
            df.to_csv(buffer, index=False, encoding='utf-8')
            buffer.seek(0)
            
            # ✅ GENERAR NOMBRE DE ARCHIVO DESCRIPTIVO
            filters_info = []
            if selected_keywords:
                filters_info.append(f"palabras-{'-'.join(selected_keywords)}")
            if selected_months:
                filters_info.append(f"meses-{'-'.join(map(str, selected_months))}")
            if selected_years:
                filters_info.append(f"años-{'-'.join(map(str, selected_years))}")
            if departamento:
                filters_info.append(f"dept-{departamento.replace(' ', '-')}")
            
            filter_suffix = "_" + "_".join(filters_info) if filters_info else ""
            csv_filename = f"inasistentes_{filename.replace('.csv', '')}{filter_suffix}_{corte_fecha}.csv"
            
            print(f"✅ CSV generado: {len(all_records)} registros en {csv_filename}")
            
            # ✅ RETORNAR STREAMING RESPONSE
            return StreamingResponse(
                io.StringIO(buffer.getvalue()),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={csv_filename}"}
            )
            
        except Exception as e:
            print(f"❌ Error exportando CSV: {e}")
            import traceback
            traceback.print_exc()
            raise Exception(f"Error exportando reporte: {str(e)}")
