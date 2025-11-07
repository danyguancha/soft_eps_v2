# services/technical_note_services/report_service_aux/analysis_breakdown_temporal.py - FECHA DINÁMICA
from typing import Any, Dict, List, Optional
from services.duckdb_service.duckdb_service import duckdb_service
from services.technical_note_services.report_service_aux.corrected_months import CorrectedMonths
from services.technical_note_services.report_service_aux.corrected_years import CorrectedYear
from services.technical_note_services.report_service_aux.identity_document import IdentityDocument

class AnalysisBreakdownTemporal:
    def execute_temporal_breakdown_analysis(
        self, data_source: str, matches: List[Dict], 
        departamento: Optional[str], municipio: Optional[str], ips: Optional[str],
        corte_fecha: str,  # FECHA DINÁMICA
        age_extractor
    ) -> Dict[str, Any]:
        """MODIFICADO: Ejecuta análisis temporal CON FECHA DINÁMICA"""
        temporal_breakdown = {}
        
        try:
            print(f"Análisis temporal usando fecha de corte DINÁMICA: {corte_fecha}")
            document_field = IdentityDocument().get_document_field(data_source)
            edad_meses_field = CorrectedMonths().get_age_months_field_corrected(data_source, corte_fecha)
            edad_años_field = CorrectedYear().get_age_years_field_corrected(data_source, corte_fecha)
        except Exception as e:
            print(f"Error detectando campos para análisis temporal: {e}")
            return {}
        
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
                
                age_range_obj = age_extractor.extract_age_range(column_name)
                if not age_range_obj:
                    continue
                
                specific_age_filter = age_range_obj.get_age_filter_sql(edad_meses_field, edad_años_field)
                age_description = age_range_obj.get_description()
                
                # EXTRAER DATOS TEMPORALES CON FECHA DINÁMICA
                temporal_data = self._extract_temporal_data_from_column_correct(
                    data_source, column_name, document_field, specific_age_filter, 
                    edad_meses_field, edad_años_field, geo_filter, corte_fecha
                )
                
                if temporal_data:
                    column_key = f"{column_name}|{keyword}|{age_description}"
                    temporal_breakdown[column_key] = {
                        "column": column_name,
                        "keyword": keyword,
                        "age_range": age_description,
                        "temporal_breakdown": temporal_data,
                        "metodo": "TEMPORAL_NUMERADOR_DENOMINADOR_FECHA_DINAMICA",
                        "corte_fecha": corte_fecha  # FECHA DINÁMICA
                    }
                
            except Exception as e:
                print(f"   ERROR procesando temporal {column_name}: {e}")
                continue
        
        return temporal_breakdown
    
    def _extract_temporal_data_from_column_correct(
        self, data_source: str, column_name: str, document_field: str,
        specific_age_filter: str, edad_meses_field: str, edad_años_field: str,
        geo_filter: str, corte_fecha: str  # FECHA DINÁMICA
    ) -> Dict[str, Any]:
        """
        VERSIÓN FINAL: Extrae datos temporales CON FECHA DINÁMICA
        """
        try:
            escaped_column = duckdb_service.escape_identifier(column_name)
            date_format = self._detect_date_format(data_source, column_name)
            
            print(f"      Formato detectado: {date_format}")
            print(f"      Usando fecha de corte: {corte_fecha}")
            
            # PASO 1: OBTENER AÑOS/MESES CON FILTRO DE EDAD Y FECHA DINÁMICA
            fechas_con_edad_sql = f"""
            SELECT DISTINCT
                date_part('year', TRY_CAST(strptime(TRIM({escaped_column}), '{date_format}') AS DATE)) as año,
                date_part('month', TRY_CAST(strptime(TRIM({escaped_column}), '{date_format}') AS DATE)) as mes
            FROM {data_source}
            WHERE 
                -- FILTRO DE EDAD PRIMERO
                ({specific_age_filter})
                AND "Fecha Nacimiento" IS NOT NULL 
                AND TRIM("Fecha Nacimiento") != ''
                AND TRY_CAST(strptime("Fecha Nacimiento", '%d/%m/%Y') AS DATE) IS NOT NULL
                AND strptime("Fecha Nacimiento", '%d/%m/%Y') <= DATE '{corte_fecha}'
                AND {document_field} IS NOT NULL
                AND {geo_filter}
                -- DESPUÉS VALIDAR CONSULTA
                AND {escaped_column} IS NOT NULL 
                AND TRIM(CAST({escaped_column} AS VARCHAR)) != ''
                AND TRIM(CAST({escaped_column} AS VARCHAR)) NOT IN ('NULL', 'null', 'None', 'none', 'NaN', 'nan', 'N/A', 'n/a', '-')
                AND LENGTH(TRIM(CAST({escaped_column} AS VARCHAR))) >= 8
                AND TRY_CAST(strptime(TRIM({escaped_column}), '{date_format}') AS DATE) IS NOT NULL
                AND TRY_CAST(strptime(TRIM({escaped_column}), '{date_format}') AS DATE) <= DATE '{corte_fecha}'
            ORDER BY año, mes
            """
            
            fechas_result = duckdb_service.conn.execute(fechas_con_edad_sql).fetchall()
            
            if not fechas_result:
                print(f"      No hay consultas para la edad específica en {column_name}")
                return {}
            
            # PASO 2: CALCULAR NUMERADOR/DENOMINADOR PARA CADA PERÍODO
            temporal_data = {}
            month_names = {
                1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
                5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto", 
                9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
            }
            
            for row in fechas_result:
                año = int(row[0]) if row[0] else None
                mes = int(row[1]) if row[1] else None
                
                if año and mes:
                    año_str = str(año)
                    mes_nombre = month_names.get(mes, f"Mes {mes}")
                    
                    if año_str not in temporal_data:
                        temporal_data[año_str] = {
                            "year": año,
                            "total_numerador": 0,
                            "total_denominador": 0,
                            "months": {}
                        }
                    
                    # CALCULAR NUMERADOR/DENOMINADOR CON FECHA DINÁMICA
                    mes_data = self._calculate_temporal_numerator_denominator_correct(
                        data_source, column_name, document_field, specific_age_filter,
                        edad_meses_field, edad_años_field, geo_filter, corte_fecha, 
                        año, mes, date_format
                    )
                    
                    temporal_data[año_str]["months"][mes_nombre] = {
                        "month": mes,
                        "month_name": mes_nombre,
                        "numerador": mes_data["numerador"],
                        "denominador": mes_data["denominador"], 
                        "cobertura_porcentaje": mes_data["cobertura_porcentaje"],
                        "sin_datos": mes_data["sin_datos"]
                    }
                    
                    temporal_data[año_str]["total_numerador"] += mes_data["numerador"]
                    temporal_data[año_str]["total_denominador"] += mes_data["denominador"]
            
            # PASO 3: CALCULAR COBERTURA ANUAL
            for año_str, año_data in temporal_data.items():
                if año_data["total_denominador"] > 0:
                    año_data["cobertura_anual_porcentaje"] = round(
                        (año_data["total_numerador"] / año_data["total_denominador"]) * 100, 2
                    )
                else:
                    año_data["cobertura_anual_porcentaje"] = 0.0
            
            return temporal_data
            
        except Exception as e:
            print(f"      Error: {e}")
            import traceback
            traceback.print_exc()
            return {}
        
    def _calculate_temporal_numerator_denominator_correct(
        self, data_source: str, column_name: str, document_field: str,
        specific_age_filter: str, edad_meses_field: str, edad_años_field: str,
        geo_filter: str, corte_fecha: str, año: int, mes: int, date_format: str
    ) -> Dict[str, Any]:
        """
        LÓGICA CORREGIDA CON FECHA DINÁMICA: Calcular denominador con registros vacíos incluidos
        """
        try:
            escaped_column = duckdb_service.escape_identifier(column_name)
            
            # DENOMINADOR TEMPORAL CON FECHA DINÁMICA
            denominator_sql = f"""
            SELECT COUNT(DISTINCT {document_field}) as denominador
            FROM {data_source}
            WHERE 
                ({specific_age_filter})
                AND "Fecha Nacimiento" IS NOT NULL 
                AND TRY_CAST(strptime("Fecha Nacimiento", '%d/%m/%Y') AS DATE) IS NOT NULL
                AND strptime("Fecha Nacimiento", '%d/%m/%Y') <= DATE '{corte_fecha}'
                AND {document_field} IS NOT NULL
                AND {geo_filter}
            """
            
            denominator_result = duckdb_service.conn.execute(denominator_sql).fetchone()
            denominador = int(denominator_result[0]) if denominator_result and denominator_result[0] else 0
            
            # NUMERADOR TEMPORAL CON FECHA DINÁMICA
            numerator_sql = f"""
            SELECT COUNT(DISTINCT {document_field}) as numerador
            FROM {data_source}
            WHERE 
                ({specific_age_filter})
                AND "Fecha Nacimiento" IS NOT NULL 
                AND TRY_CAST(strptime("Fecha Nacimiento", '%d/%m/%Y') AS DATE) IS NOT NULL
                AND strptime("Fecha Nacimiento", '%d/%m/%Y') <= DATE '{corte_fecha}'
                AND {document_field} IS NOT NULL
                AND {geo_filter}
                -- CON consulta en el período específico
                AND {escaped_column} IS NOT NULL 
                AND TRIM(CAST({escaped_column} AS VARCHAR)) != ''
                AND TRIM(CAST({escaped_column} AS VARCHAR)) NOT IN ('NULL', 'null', 'None', 'none', 'NaN', 'nan', 'N/A', 'n/a', '-')
                AND LENGTH(TRIM(CAST({escaped_column} AS VARCHAR))) >= 8
                AND TRY_CAST(strptime(TRIM({escaped_column}), '{date_format}') AS DATE) IS NOT NULL
                AND date_part('year', TRY_CAST(strptime(TRIM({escaped_column}), '{date_format}') AS DATE)) = {año}
                AND date_part('month', TRY_CAST(strptime(TRIM({escaped_column}), '{date_format}') AS DATE)) = {mes}
            """
            
            numerator_result = duckdb_service.conn.execute(numerator_sql).fetchone()
            numerador = int(numerator_result[0]) if numerator_result and numerator_result[0] else 0
            
            # VALIDACIÓN Y MÉTRICAS
            if numerador > denominador:
                print(f"         Numerador > Denominador en {año}/{mes}, ajustando...")
                numerador = denominador
                
            cobertura_porcentaje = (numerador / denominador) * 100 if denominador > 0 else 0.0
            sin_datos = denominador - numerador
            
            print(f"         {año}/{mes:02d}: N={numerador}, D={denominador}, Sin datos={sin_datos}, Cob={cobertura_porcentaje:.1f}%")
            
            return {
                "numerador": numerador,
                "denominador": denominador,
                "cobertura_porcentaje": round(cobertura_porcentaje, 2),
                "sin_datos": sin_datos
            }
            
        except Exception as e:
            print(f"         Error calculando {año}/{mes}: {e}")
            return {"numerador": 0, "denominador": 0, "cobertura_porcentaje": 0.0, "sin_datos": 0}
    
    def _detect_date_format(self, data_source: str, column_name: str) -> str:
        """DETECTA AUTOMÁTICAMENTE EL FORMATO DE FECHA DE UNA COLUMNA"""
        try:
            escaped_column = duckdb_service.escape_identifier(column_name)
            
            sample_query = f"""
            SELECT DISTINCT {escaped_column}
            FROM {data_source}
            WHERE {escaped_column} IS NOT NULL 
            AND TRIM({escaped_column}) != ''
            AND TRIM({escaped_column}) NOT IN ('NULL', 'null', 'None', 'none', 'NaN', 'nan', 'N/A', 'n/a', '-')
            LIMIT 10
            """
            
            samples = duckdb_service.conn.execute(sample_query).fetchall()
            
            if not samples:
                return '%d/%m/%Y'
            
            for sample in samples:
                fecha_str = str(sample[0]).strip()
                
                # Patrón YYYY-MM-DD (ISO)
                if len(fecha_str) == 10 and fecha_str[4] == '-' and fecha_str[7] == '-':
                    print(f"   Formato detectado: ISO (YYYY-MM-DD) - Ejemplo: {fecha_str}")
                    return '%Y-%m-%d'
                
                # Patrón DD/MM/YYYY
                elif len(fecha_str) >= 8 and '/' in fecha_str:
                    parts = fecha_str.split('/')
                    if len(parts) == 3 and len(parts[2]) == 4:
                        print(f"   Formato detectado: DD/MM/YYYY - Ejemplo: {fecha_str}")
                        return '%d/%m/%Y'
            
            return '%d/%m/%Y'
            
        except Exception as e:
            print(f"   Error detectando formato de fecha: {e}")
            return '%d/%m/%Y'
