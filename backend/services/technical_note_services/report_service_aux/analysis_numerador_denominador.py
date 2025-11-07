# services/technical_note_services/report_service_aux/analysis_numerador_denominador.py
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
        COMPLETAMENTE CORREGIDO: Calcula numerador y denominador CON FECHA DINÁMICA
        """
        items_with_numerator_denominator = []
        
        # DETECTAR CAMPO DE DOCUMENTO CORRECTO
        try:
            document_field = IdentityDocument().get_document_field(data_source)
        except Exception as e:
            print(f"Error detectando campo documento: {e}")
            return []
        
        # DETECTAR CAMPOS DE EDAD CON FECHA DINÁMICA
        try:
            edad_meses_field = CorrectedMonths().get_age_months_field_corrected(data_source, corte_fecha)
            edad_años_field = CorrectedYear().get_age_years_field_corrected(data_source, corte_fecha)
            
            # IMPRIMIR LOS CAMPOS DE EDAD CALCULADOS
            print(f"Campo edad meses: {edad_meses_field}")
            print(f"Campo edad años: {edad_años_field}")
            
        except Exception as e:
            print(f"Error detectando campos de edad: {e}")
            return []
        
        # CONSTRUIR FILTROS GEOGRÁFICOS
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
                    print(f"   No se pudo extraer rango de edad específico")
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
                            "metodo": "FALLBACK_TRADICIONAL",
                            "corte_fecha": corte_fecha
                        })
                    continue
                
                print(f"   RANGO EXTRAÍDO: {age_range_obj.get_description()}")
                print(f"   Min: {age_range_obj.min_age}, Max: {age_range_obj.max_age}, Unit: {age_range_obj.unit}")
                
                # USAR FILTRO EXACTO CON FECHA DINÁMICA
                specific_age_filter = self._build_exact_age_filter(age_range_obj, edad_meses_field, edad_años_field)
                age_description = age_range_obj.get_description()
                
                print(f"   FILTRO EDAD EXACTO: {specific_age_filter}")
                
                # CALCULAR DENOMINADOR CON FECHA DINÁMICA
                # IMPORTANTE: El denominador debe contar TODOS los registros del rango de edad
                # calculado con la fecha de corte, sin importar si tienen o no consulta
                denominador_sql = f"""
                SELECT COUNT({document_field}) as total_registros
                FROM {data_source}
                WHERE 
                    ({specific_age_filter})
                    AND "Fecha Nacimiento" IS NOT NULL 
                    AND TRIM("Fecha Nacimiento") != ''
                    AND TRY_CAST(strptime("Fecha Nacimiento", '%d/%m/%Y') AS DATE) IS NOT NULL
                    AND strptime("Fecha Nacimiento", '%d/%m/%Y') <= DATE '{corte_fecha}'
                    AND {document_field} IS NOT NULL
                    AND TRIM({document_field}) != ''
                    AND {geo_filter}
                """
                
                print(f"   SQL DENOMINADOR:")
                print(f"      {denominador_sql[:300]}...")
                
                try:
                    denominator_result = duckdb_service.conn.execute(denominador_sql).fetchone()
                    total_denominator = int(denominator_result[0]) if denominator_result and denominator_result[0] else 0
                except Exception as e:
                    print(f"   Error ejecutando denominador: {e}")
                    import traceback
                    traceback.print_exc()
                    continue
                
                print(f"   DENOMINADOR: {total_denominator:,} registros con {age_description}")
                
                if total_denominator == 0:
                    print(f"   DENOMINADOR = 0 - Sin población en este rango exacto")
                    continue
                
                # CALCULAR NUMERADOR CON FECHA DINÁMICA
                escaped_column = duckdb_service.escape_identifier(column_name)
                
                # NUMERADOR: Solo los que tienen datos válidos en la columna
                numerator_sql = f"""
                SELECT COUNT({document_field}) as total_con_datos
                FROM {data_source}
                WHERE 
                    ({specific_age_filter})
                    AND {escaped_column} IS NOT NULL 
                    AND TRIM(CAST({escaped_column} AS VARCHAR)) != ''
                    AND TRIM(CAST({escaped_column} AS VARCHAR)) NOT IN ('NULL', 'null', 'None', 'none', 'NaN', 'nan', 'N/A', 'n/a', '-', 'No')
                    AND "Fecha Nacimiento" IS NOT NULL 
                    AND TRIM("Fecha Nacimiento") != ''
                    AND TRY_CAST(strptime("Fecha Nacimiento", '%d/%m/%Y') AS DATE) IS NOT NULL
                    AND strptime("Fecha Nacimiento", '%d/%m/%Y') <= DATE '{corte_fecha}'
                    AND {document_field} IS NOT NULL
                    AND TRIM({document_field}) != ''
                    AND {geo_filter}
                """
                
                print(f"   SQL NUMERADOR:")
                print(f"      {numerator_sql[:300]}...")
                
                try:
                    numerator_result = duckdb_service.conn.execute(numerator_sql).fetchone()
                    total_numerator = int(numerator_result[0]) if numerator_result and numerator_result[0] else 0
                except Exception as e:
                    print(f"   Error ejecutando numerador: {e}")
                    import traceback
                    traceback.print_exc()
                    continue
                
                print(f"   NUMERADOR: {total_numerator:,} registros con datos")
                
                # VALIDAR CONSISTENCIA
                if total_numerator > total_denominator:
                    print(f"   ADVERTENCIA: numerador ({total_numerator}) > denominador ({total_denominator})")
                    print(f"   Ajustando numerador = denominador")
                    total_numerator = total_denominator
                
                # CALCULAR MÉTRICAS
                cobertura_porcentaje = (total_numerator / total_denominator) * 100 if total_denominator > 0 else 0.0
                sin_datos = total_denominator - total_numerator
                
                print(f"   COBERTURA: {cobertura_porcentaje:.2f}%")
                print(f"   SIN DATOS: {sin_datos:,} registros")
                
                # AGREGAR DEBUG: Verificar con una consulta de muestra
                if age_range_obj.unit == 'months':
                    debug_sql = f"""
                    SELECT 
                        "Fecha Nacimiento",
                        {edad_meses_field} as edad_calculada,
                        {escaped_column} as valor_consulta,
                        CASE 
                            WHEN {escaped_column} IS NOT NULL 
                                 AND TRIM(CAST({escaped_column} AS VARCHAR)) != ''
                                 AND TRIM(CAST({escaped_column} AS VARCHAR)) NOT IN ('NULL', 'null', 'None', 'none', 'NaN', 'nan', 'N/A', 'n/a', '-', 'No')
                            THEN 'CON_DATO'
                            ELSE 'SIN_DATO'
                        END as estado
                    FROM {data_source}
                    WHERE 
                        ({specific_age_filter})
                        AND "Fecha Nacimiento" IS NOT NULL 
                        AND {geo_filter}
                    LIMIT 10
                    """
                    
                    try:
                        debug_result = duckdb_service.conn.execute(debug_sql).fetchall()
                        print(f"   MUESTRA DE DATOS (primeros 10):")
                        for idx, row in enumerate(debug_result, 1):
                            print(f"      {idx}. Nac: {row[0]}, Edad: {row[1]} meses, Valor: {row[2]}, Estado: {row[3]}")
                    except Exception as debug_error:
                        print(f"   No se pudo ejecutar debug: {debug_error}")
                
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
                        "metodo": f"REGISTROS_TOTALES_{age_range_obj.unit.upper()}",
                        
                        # Campos adicionales
                        "age_range_extracted": {
                            "min_age": age_range_obj.min_age,
                            "max_age": age_range_obj.max_age,
                            "unit": age_range_obj.unit,
                            "sql_filter": specific_age_filter
                        },
                        "corte_fecha": corte_fecha
                    })
                    
                    print(f"   AGREGADO AL REPORTE")
                    
                    # DEBUG para rangos múltiples
                    if age_range_obj.min_age != age_range_obj.max_age:
                        print(f"   DESGLOSE POR EDAD (rango múltiple):")
                        self._debug_age_range_coverage(
                            data_source, age_range_obj, edad_meses_field, edad_años_field, 
                            geo_filter, corte_fecha, document_field
                        )
                else:
                    print(f"   FILTRADO: numerador ({total_numerator}) < min_count ({min_count})")
                
            except Exception as e:
                print(f"   ERROR procesando {match.get('column', 'columna desconocida')}: {e}")
                import traceback
                traceback.print_exc()
                continue        
        return items_with_numerator_denominator
    
    def _build_exact_age_filter(self, age_range_obj, edad_meses_field: str, edad_años_field: str) -> str:
        """
        Construye filtro de edad EXACTA usando los campos calculados dinámicamente
        """
        if age_range_obj.unit == 'months':
            if age_range_obj.min_age == age_range_obj.max_age:
                # Un solo mes
                filter_sql = f"{edad_meses_field} = {age_range_obj.min_age}"
            else:
                # Rango de meses
                filter_sql = f"{edad_meses_field} BETWEEN {age_range_obj.min_age} AND {age_range_obj.max_age}"
        
        elif age_range_obj.unit == 'years':
            if age_range_obj.min_age == age_range_obj.max_age:
                # Un solo año: convertir a meses (ej: 3 años = 36-47 meses)
                min_months = age_range_obj.min_age * 12
                max_months = (age_range_obj.min_age + 1) * 12 - 1
                filter_sql = f"{edad_meses_field} BETWEEN {min_months} AND {max_months}"
            else:
                # Rango de años: convertir a meses
                min_months = age_range_obj.min_age * 12
                max_months = (age_range_obj.max_age + 1) * 12 - 1
                filter_sql = f"{edad_meses_field} BETWEEN {min_months} AND {max_months}"
        
        else:
            # Fallback al método original
            filter_sql = age_range_obj.get_age_filter_sql(edad_meses_field, edad_años_field)
        
        return filter_sql
    
    def _debug_age_range_coverage(
        self, data_source: str, age_range_obj, edad_meses_field: str, 
        edad_años_field: str, geo_filter: str, corte_fecha: str, document_field: str
    ):
        """Debug: Verificar distribución de edades en el denominador CON FECHA DINÁMICA"""
        try:
            print(f"\n      DEBUG: Distribución por edad en el rango")
            
            age_filter = self._build_exact_age_filter(age_range_obj, edad_meses_field, edad_años_field)
            
            debug_sql = f"""
            SELECT 
                {edad_meses_field} as edad_meses,
                COUNT({document_field}) as registros
            FROM {data_source}
            WHERE 
                ({age_filter})
                AND "Fecha Nacimiento" IS NOT NULL 
                AND TRY_CAST(strptime("Fecha Nacimiento", '%d/%m/%Y') AS DATE) IS NOT NULL
                AND strptime("Fecha Nacimiento", '%d/%m/%Y') <= DATE '{corte_fecha}'
                AND {document_field} IS NOT NULL
                AND {geo_filter}
            GROUP BY {edad_meses_field}
            ORDER BY edad_meses
            LIMIT 20
            """
            
            debug_result = duckdb_service.conn.execute(debug_sql).fetchall()
            
            total_verification = 0
            for row in debug_result:
                edad_mes = row[0]
                registros = row[1]
                total_verification += registros
                print(f"         {edad_mes} meses: {registros:,} registros")
            
            print(f"      TOTAL VERIFICADO: {total_verification:,}")
            
        except Exception as e:
            print(f"      Error en debug: {e}")
    
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
        """Construye reporte exitoso CON numerador/denominador Y FECHA DINÁMICA"""
        return {
            "success": True,
            "filename": filename,
            "corte_fecha": corte_fecha,
            "rules": {"keywords": keywords or []},
            "geographic_filters": {**geographic_filters, "filter_type": "numerador_denominador"},
            "items": items,
            "totals_by_keyword": totals_by_keyword,
            "temporal_data": temporal_data,
            "temporal_breakdown_data": temporal_breakdown_data or {},
            "global_statistics": global_statistics,
            "ultra_fast": True,
            "engine": "DuckDB_DateDiff_FechaDinamica_v6",
            "data_source_used": data_source,
            "metodo": "REGISTROS_TOTALES_DATE_DIFF",
            "version": "6.0.0",
            "logica_denominador": "COUNT(*) con date_diff() equivalente a SIFECHA de Excel",
            "caracteristicas": [
                "date_diff_equivalente_sifecha",
                "edad_exacta_fecha_dinamica",
                "numerador_con_datos_reales",
                "cobertura_por_edad_especifica",
                "compatibilidad_100_excel_sifecha"
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
