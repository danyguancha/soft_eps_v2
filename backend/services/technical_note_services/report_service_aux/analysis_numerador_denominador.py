from typing import Any, Dict, List, Optional
from services.duckdb_service.duckdb_service import duckdb_service
from services.technical_note_services.report_service_aux.corrected_months import CorrectedMonths
from services.technical_note_services.report_service_aux.corrected_years import CorrectedYear
from services.technical_note_services.report_service_aux.identity_document import IdentityDocument

class AnalysisNumeratorDenominator:
    def execute_numerator_denominator_analysis(
        self, data_source: str, matches: List[Dict], 
        departamento: Optional[str], municipio: Optional[str], ips: Optional[str],
        min_count: int, corte_fecha: str, age_extractor
    ) -> List[Dict[str, Any]]:
        """
        COMPLETAMENTE CORREGIDO: Calcula numerador y denominador por rango de edad específico
        """
        items_with_numerator_denominator = []
        
        # DETECTAR CAMPO DE DOCUMENTO CORRECTO
        try:
            document_field = IdentityDocument().get_document_field(data_source)
        except Exception as e:
            print(f"❌ Error detectando campo documento: {e}")
            return []
        
        # DETECTAR CAMPOS DE EDAD CORRECTOS  
        try:
            edad_meses_field = CorrectedMonths().get_age_months_field_corrected(data_source, corte_fecha)
            edad_años_field = CorrectedYear().get_age_years_field_corrected(data_source, corte_fecha)
        except Exception as e:
            print(f"❌ Error detectando campos de edad: {e}")
            return []
        
        # CONSTRUIR FILTROS GEOGRÁFICOS CORREGIDOS
        geo_conditions = []
        if departamento and departamento != 'Todos': 
            geo_conditions.append(f'"Departamento" = \'{departamento}\'')
        if municipio and municipio != 'Todos': 
            geo_conditions.append(f'"Municipio" = \'{municipio}\'')  
        if ips and ips != 'Todos': 
            geo_conditions.append(f'"Nombre IPS" = \'{ips}\'')
        geo_filter = " AND ".join(geo_conditions) if geo_conditions else "1=1"
                
        for match in matches:
            try:
                column_name = match['column']
                keyword = match['keyword'] 
                original_age_range = match['age_range']
                
                # EXTRAER RANGO DE EDAD ESPECÍFICO
                age_range_obj = age_extractor.extract_age_range(column_name)
                
                if not age_range_obj:
                    print(f"   ⚠️ No se pudo extraer rango de edad específico del nombre")
                    # Usar análisis tradicional como fallback
                    traditional_count = self._get_traditional_count(data_source, column_name, geo_filter)
                    if traditional_count >= min_count:
                        items_with_numerator_denominator.append({
                            "column": column_name,
                            "keyword": keyword,
                            "age_range": original_age_range,
                            "count": traditional_count,
                            "numerador": traditional_count,
                            "denominador": traditional_count,
                            "cobertura_porcentaje": 100.0,
                            "sin_datos": 0,
                            "metodo": "FALLBACK_TRADICIONAL"
                        })
                    continue
                
                print(f"   RANGO EXTRAÍDO: {age_range_obj.get_description()}")
                print(f"   📏 Min: {age_range_obj.min_age}, Max: {age_range_obj.max_age}, Unit: {age_range_obj.unit}")
                
                # GENERAR FILTRO SQL USANDO EL MÉTODO DEL AGERANGE
                specific_age_filter = age_range_obj.get_age_filter_sql(edad_meses_field, edad_años_field)
                age_description = age_range_obj.get_description()
                
                # CALCULAR DENOMINADOR CON CAMPOS CORRECTOS
                denominator_sql = f"""
                SELECT COUNT(DISTINCT {document_field}) as total_poblacion_rango
                FROM {data_source}
                WHERE 
                    ({specific_age_filter})
                    AND "Fecha Nacimiento" IS NOT NULL 
                    AND TRIM("Fecha Nacimiento") != ''
                    AND LENGTH(TRIM("Fecha Nacimiento")) >= 8
                    AND TRY_CAST(strptime("Fecha Nacimiento", '%d/%m/%Y') AS DATE) IS NOT NULL
                    AND strptime("Fecha Nacimiento", '%d/%m/%Y') <= DATE '{corte_fecha}'
                    AND {document_field} IS NOT NULL
                    AND TRIM({document_field}) != ''
                    AND {geo_filter}
                """
                
                print(f"   🔍 QUERY DENOMINADOR (primeros 150 chars): {denominator_sql[:150]}...")
                
                try:
                    denominator_result = duckdb_service.conn.execute(denominator_sql).fetchone()
                    total_denominator = int(denominator_result[0]) if denominator_result and denominator_result[0] else 0
                except Exception as e:
                    print(f"   ❌ Error ejecutando denominador: {e}")
                    continue
                
                print(f"   📊 DENOMINADOR: {total_denominator:,} personas en {age_description}")
                
                if total_denominator == 0:
                    print(f"   ⚠️ DENOMINADOR = 0 - Sin población en este rango")
                    continue
                
                # CALCULAR NUMERADOR CON CAMPOS CORRECTOS
                escaped_column = duckdb_service.escape_identifier(column_name)
                
                numerator_sql = f"""
                SELECT COUNT(DISTINCT {document_field}) as total_con_datos
                FROM {data_source}
                WHERE 
                    ({specific_age_filter})
                    AND {escaped_column} IS NOT NULL 
                    AND TRIM(CAST({escaped_column} AS VARCHAR)) != ''
                    AND TRIM(CAST({escaped_column} AS VARCHAR)) NOT IN ('NULL', 'null', 'None', 'none', 'NaN', 'nan', 'N/A', 'n/a', '-')
                    AND "Fecha Nacimiento" IS NOT NULL 
                    AND TRIM("Fecha Nacimiento") != ''
                    AND LENGTH(TRIM("Fecha Nacimiento")) >= 8
                    AND TRY_CAST(strptime("Fecha Nacimiento", '%d/%m/%Y') AS DATE) IS NOT NULL
                    AND strptime("Fecha Nacimiento", '%d/%m/%Y') <= DATE '{corte_fecha}'
                    AND {document_field} IS NOT NULL
                    AND TRIM({document_field}) != ''
                    AND {geo_filter}
                """
                
                print(f"   🔍 QUERY NUMERADOR (primeros 150 chars): {numerator_sql[:150]}...")
                
                try:
                    numerator_result = duckdb_service.conn.execute(numerator_sql).fetchone()
                    total_numerator = int(numerator_result[0]) if numerator_result and numerator_result[0] else 0
                except Exception as e:
                    print(f"   ❌ Error ejecutando numerador: {e}")
                    continue
                
                print(f"   NUMERADOR: {total_numerator:,} con datos")
                
                # VALIDAR CONSISTENCIA
                if total_numerator > total_denominator:
                    print(f"   🔧 Corrigiendo: numerador > denominador")
                    total_numerator = total_denominator
                
                # CALCULAR MÉTRICAS
                cobertura_porcentaje = (total_numerator / total_denominator) * 100 if total_denominator > 0 else 0.0
                sin_datos = total_denominator - total_numerator
                
                # APLICAR FILTRO min_count
                if total_numerator >= min_count:
                    items_with_numerator_denominator.append({
                        "column": column_name,
                        "keyword": keyword,
                        "age_range": age_description,
                        "count": total_numerator,
                        
                        # CAMPOS NUMERADOR/DENOMINADOR
                        "numerador": total_numerator,
                        "denominador": total_denominator,
                        "cobertura_porcentaje": round(cobertura_porcentaje, 2),
                        "sin_datos": sin_datos,
                        "metodo": f"NUMERADOR_DENOMINADOR_RANGO_{age_range_obj.unit.upper()}",
                        
                        # Campos adicionales
                        "age_range_extracted": {
                            "min_age": age_range_obj.min_age,
                            "max_age": age_range_obj.max_age,
                            "unit": age_range_obj.unit,
                            "sql_filter": specific_age_filter
                        },
                        "corte_fecha": corte_fecha
                    })
                    
                    # DEBUGGING: Verificar si el rango incluye múltiples edades
                    if age_range_obj.min_age != age_range_obj.max_age:
                        print(f"   🔍 VERIFICACIÓN RANGO MÚLTIPLE:")
                        self._debug_age_range_coverage(
                            data_source, age_range_obj, edad_meses_field, edad_años_field, 
                            geo_filter, corte_fecha, document_field
                        )
                else:
                    print(f"   ⚠️ FILTRADO: numerador ({total_numerator}) < min_count ({min_count})")
                
            except Exception as e:
                print(f"   ❌ ERROR procesando {match.get('column', 'columna desconocida')}: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        return items_with_numerator_denominator
    
    def calculate_totals_with_numerator_denominator(self, items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Calcula totales por palabra clave CON numerador/denominador"""
        totals_by_keyword = {}
        
        for item in items:
            keyword = item["keyword"]
            
            if keyword not in totals_by_keyword:
                totals_by_keyword[keyword] = {
                    "count": 0,
                    "numerador": 0,
                    "denominador": 0,
                    "actividades": 0,
                    "cobertura_promedio": 0.0
                }
            
            totals_by_keyword[keyword]["count"] += item.get("count", 0)
            totals_by_keyword[keyword]["numerador"] += item.get("numerador", 0)
            totals_by_keyword[keyword]["denominador"] += item.get("denominador", 0)
            totals_by_keyword[keyword]["actividades"] += 1
        
        # Calcular coberturas promedio
        for keyword, totals in totals_by_keyword.items():
            if totals["denominador"] > 0:
                totals["cobertura_promedio"] = round((totals["numerador"] / totals["denominador"]) * 100, 2)
        
        return totals_by_keyword
    
    def build_success_report_with_numerator_denominator(
        self, filename: str, keywords: Optional[List[str]], 
        geographic_filters: Dict[str, Optional[str]], 
        items: List[Dict[str, Any]], 
        totals_by_keyword: Dict[str, Any], 
        temporal_data: Dict[str, Any], 
        data_source: str,
        global_statistics: Dict[str, Any],
        corte_fecha: str,
        temporal_breakdown_data: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Construye reporte exitoso CON numerador/denominador Y DESGLOSE TEMPORAL"""
        return {
            "success": True,
            "filename": filename,
            "corte_fecha": corte_fecha,
            "rules": {"keywords": keywords or []},
            "geographic_filters": {**geographic_filters, "filter_type": "numerador_denominador"},
            "items": items,
            "totals_by_keyword": totals_by_keyword,
            "temporal_data": temporal_data,
            "temporal_breakdown_data": temporal_breakdown_data or {},  # 🆕 NUEVO CAMPO
            "global_statistics": global_statistics,
            "ultra_fast": True,
            "engine": "DuckDB_NumeradorDenominador_Keywords_v2",
            "data_source_used": data_source,
            "metodo": "NUMERADOR_DENOMINADOR_CON_DESGLOSE_TEMPORAL_POR_MES_Y_AÑO",
            "version": "2.0.0",
            "caracteristicas": [
                "extraccion_automatica_rango_edad",
                "numerador_denominador_por_columna", 
                "calculo_cobertura_especifica",
                "compatibilidad_keywords_tradicional",
                "analisis_temporal_integrado",
                "verificacion_rangos_multiples",
                "deteccion_automatica_campos",
                "desglose_temporal_numerador_denominador_mes_año",
                "extraccion_fechas_desde_columnas_consulta",
                "calculo_cobertura_por_periodo_temporal"
            ]
        }
    
    def _get_traditional_count(self, data_source: str, column_name: str, geo_filter: str) -> int:
        """Obtiene conteo tradicional como fallback"""
        try:
            escaped_column = duckdb_service.escape_identifier(column_name)
            count_sql = f"""
            SELECT COUNT(*) FROM {data_source}
            WHERE {escaped_column} IS NOT NULL 
            AND TRIM(CAST({escaped_column} AS VARCHAR)) != ''
            AND {geo_filter}
            """
            result = duckdb_service.conn.execute(count_sql).fetchone()
            return int(result[0]) if result[0] else 0
        except:
            return 0