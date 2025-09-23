from typing import Any, Dict, List, Optional

from services.duckdb_service.duckdb_service import duckdb_service
from services.keyword_age_report import ColumnKeywordReportService
from services.technical_note_services.report_service_aux.analysis_breakdown_temporal import AnalysisBreakdownTemporal
from services.technical_note_services.report_service_aux.analysis_numerador_denominador import AnalysisNumeratorDenominator
from services.technical_note_services.report_service_aux.corrected_months import CorrectedMonths
from services.technical_note_services.report_service_aux.corrected_years import CorrectedYear
from services.technical_note_services.report_service_aux.identity_document import IdentityDocument
from services.technical_note_services.report_service_aux.report_empty import ReportEmpty
from services.technical_note_services.report_service_aux.semaforization import Semaforization
from services.technical_note_services.report_service_aux.statistics import Statistics
from utils.keywords_NT import KeywordRule
from .analysis_temporal import AnalysisTemporal
from .analysis_vaccination import AnalysisVaccination


class GenerateReport:
    def generate_keyword_age_report(
        self,
        age_extractor,
        data_source: str,
        filename: str,
        keywords: Optional[List[str]] = None,
        min_count: int = 0,
        include_temporal: bool = True,
        geographic_filters: Optional[Dict[str, Optional[str]]] = None,
        corte_fecha: str = "2025-07-31",
    ) -> Dict[str, Any]:
        """
        🆕 Genera reporte CON NUMERADOR Y DENOMINADOR POR RANGO DE EDAD ESPECÍFICO + SEMAFORIZACIÓN
        """
        try:            
            geographic_filters = geographic_filters or {}
            departamento = geographic_filters.get('departamento')
            municipio = geographic_filters.get('municipio') 
            ips = geographic_filters.get('ips')
            
            # PASO 1: OBTENER COLUMNAS Y BUSCAR COINCIDENCIAS
            columns = self._get_table_columns(data_source)
            rules = self._setup_keyword_rules(keywords)
            service = ColumnKeywordReportService(keywords=rules)
            matches = service.match_columns(columns)
            
            if not matches:
                return ReportEmpty().build_empty_report(filename, keywords, geographic_filters)
            
            # PASO 2: GENERAR REPORTES CON NUMERADOR/DENOMINADOR POR RANGO ESPECÍFICO
            items_with_numerator_denominator = AnalysisNumeratorDenominator().execute_numerator_denominator_analysis(
                data_source, matches, departamento, municipio, ips, min_count, corte_fecha, age_extractor
            )
            
            # PASO 3: ANÁLISIS TEMPORAL (OPCIONAL)
            temporal_breakdown_data = {}
            combined_temporal_data = {}

            if include_temporal and items_with_numerator_denominator:
                
                # NUEVO: Desglose temporal con N/D por mes/año
                temporal_breakdown_data = AnalysisBreakdownTemporal().execute_temporal_breakdown_analysis(
                    data_source, matches, departamento, municipio, ips, corte_fecha, age_extractor
                )

                # EXISTENTE: Análisis temporal tradicional
                temporal_data = AnalysisTemporal().execute_temporal_analysis(
                    service, data_source, matches, departamento, municipio, ips
                )
                
                vaccination_states_data = AnalysisVaccination().execute_vaccination_states_analysis(
                    data_source, matches, departamento, municipio, ips
                )
                
                combined_temporal_data.update(temporal_data)
                combined_temporal_data.update(vaccination_states_data)
            
            # PASO 4: CALCULAR TOTALES Y ESTADÍSTICAS GLOBALES
            totals_by_keyword = AnalysisNumeratorDenominator().calculate_totals_with_numerator_denominator(items_with_numerator_denominator)
            global_statistics = Statistics().calculate_global_statistics(items_with_numerator_denominator)
            
            # Limpiar combined_temporal_data y reemplazar con temporal_breakdown_data
            combined_temporal_data.clear()

            # Convertir temporal_breakdown_data al formato que espera el frontend
            for key, breakdown_data in temporal_breakdown_data.items():
                column = breakdown_data.get('column')
                keyword = breakdown_data.get('keyword')
                age_range = breakdown_data.get('age_range')
                temporal_breakdown = breakdown_data.get('temporal_breakdown', {})
                
                # Crear la estructura que espera el frontend tradicional
                combined_temporal_data[key] = {
                    "column": column,
                    "keyword": keyword,
                    "age_range": age_range,
                    "years": {}
                }
                
                for year_str, year_data in temporal_breakdown.items():
                    combined_temporal_data[key]["years"][year_str] = {
                        "year": int(year_str),
                        "total": year_data.get("total_numerador", 0),
                        "months": {}
                    }
                    
                    for month_name, month_data in year_data.get("months", {}).items():
                        combined_temporal_data[key]["years"][year_str]["months"][month_name] = {
                            "month": month_data.get("month"),
                            "month_name": month_name,
                            "count": month_data.get("numerador", 0),
                            "numerador": month_data.get("numerador", 0),
                            "denominador": month_data.get("denominador", 0),
                            "cobertura_porcentaje": month_data.get("cobertura_porcentaje", 0.0)
                        }

            # ✅ DEFINIR VARIABLES NECESARIAS
            try:
                document_field = IdentityDocument().get_document_field(data_source)
                edad_meses_field = CorrectedMonths().get_age_months_field_corrected(data_source, corte_fecha)
                edad_años_field = CorrectedYear().get_age_years_field_corrected(data_source, corte_fecha)
            except Exception as e:
                document_field = '"Nro Identificación"'
                edad_meses_field = f"date_diff('month', strptime(\"Fecha Nacimiento\", '%d/%m/%Y'), DATE '{corte_fecha}')"
                edad_años_field = 'TRY_CAST("Edad" AS INTEGER)'

            # ✅ CONSTRUIR FILTROS GEOGRÁFICOS
            geo_conditions = []
            if departamento and departamento != 'Todos': 
                geo_conditions.append(f'"Departamento" = \'{departamento}\'')
            if municipio and municipio != 'Todos': 
                geo_conditions.append(f'"Municipio" = \'{municipio}\'')  
            if ips and ips != 'Todos': 
                geo_conditions.append(f'"Nombre IPS" = \'{ips}\'')
            geo_filter = " AND ".join(geo_conditions) if geo_conditions else "1=1"

            # 🔧 CORRECCIÓN DEFINITIVA: LÓGICA DE EXCEL + SEMAFORIZACIÓN
            
            for key, data in combined_temporal_data.items():
                if 'years' in data:
                    column_name = data.get('column', '')
                    age_range_obj = age_extractor.extract_age_range(column_name)
                    
                    if age_range_obj:                        
                        for year_str, year_data in data['years'].items():
                            total_numerador_anual = 0
                            
                            # CORREGIR DENOMINADORES MENSUALES + SEMAFORIZACIÓN
                            for month_name, month_data in year_data.get('months', {}).items():
                                mes_num = month_data.get('month')
                                if mes_num:
                                    
                                    # USAR LÓGICA DE EXCEL: registros del mes + vacíos
                                    denominador_mensual = self._calculate_denominator_excel_logic(
                                        data_source, age_range_obj, document_field, geo_filter,
                                        int(year_str), mes_num, edad_meses_field, corte_fecha, column_name
                                    )
                                    
                                    # Mantener numerador original
                                    numerador_mensual = month_data.get('numerador', 0)
                                    
                                    # ACTUALIZAR DATOS DEL MES
                                    month_data['denominador'] = denominador_mensual
                                    month_data['den'] = denominador_mensual
                                    month_data['num'] = numerador_mensual
                                    
                                    # Recalcular porcentaje
                                    if denominador_mensual > 0:
                                        porcentaje = round((numerador_mensual / denominador_mensual) * 100, 2)
                                        month_data['pct'] = porcentaje
                                        month_data['cobertura_porcentaje'] = porcentaje
                                    else:
                                        porcentaje = 0.0
                                        month_data['pct'] = 0.0
                                        month_data['cobertura_porcentaje'] = 0.0
                                    
                                    # 🚦 AGREGAR SEMAFORIZACIÓN MENSUAL
                                    semaforizacion = Semaforization().calculate_semaforizacion(numerador_mensual, porcentaje)
                                    month_data['semaforizacion'] = semaforizacion['estado']
                                    month_data['color'] = semaforizacion['color']
                                    month_data['color_name'] = semaforizacion['color_name']
                                    month_data['descripcion'] = semaforizacion['descripcion']
                                                                        
                                    # Sumar numerador para total anual
                                    total_numerador_anual += numerador_mensual
                            
                     
                            denominador_anual_correcto = self._calculate_denominador_anual_correcto(
                                data_source, age_range_obj, document_field, geo_filter,
                                int(year_str), edad_meses_field, corte_fecha, column_name
                            )
                            
                            # CALCULAR PORCENTAJE ANUAL
                            if denominador_anual_correcto > 0:
                                porcentaje_anual = round((total_numerador_anual / denominador_anual_correcto) * 100, 2)
                            else:
                                porcentaje_anual = 0.0
                            
                            # 🚦 AGREGAR SEMAFORIZACIÓN ANUAL
                            semaforizacion_anual = Semaforization().calculate_semaforizacion(total_numerador_anual, porcentaje_anual)
                            
                            # ACTUALIZAR TOTALES ANUALES CON DENOMINADOR CORRECTO + SEMAFORIZACIÓN
                            year_data['total'] = total_numerador_anual
                            year_data['total_num'] = total_numerador_anual        
                            year_data['total_den'] = denominador_anual_correcto
                            year_data['pct'] = porcentaje_anual
                            year_data['semaforizacion'] = semaforizacion_anual['estado']
                            year_data['color'] = semaforizacion_anual['color']
                            year_data['color_name'] = semaforizacion_anual['color_name']
                            year_data['descripcion'] = semaforizacion_anual['descripcion']
                            
       
            for item in items_with_numerator_denominator:
                numerador = item.get('numerador', 0)
                denominador = item.get('denominador', 0)
                
                if denominador > 0:
                    porcentaje = round((numerador / denominador) * 100, 2)
                    item['cobertura_porcentaje'] = porcentaje
                else:
                    porcentaje = 0.0
                    item['cobertura_porcentaje'] = 0.0
                
                # Agregar semaforización
                semaforizacion = Semaforization().calculate_semaforizacion(numerador, porcentaje)
                item['semaforizacion'] = semaforizacion['estado']
                item['color'] = semaforizacion['color']
                item['color_name'] = semaforizacion['color_name']
                item['descripcion'] = semaforizacion['descripcion']

            return AnalysisNumeratorDenominator().build_success_report_with_numerator_denominator(
                filename, keywords, geographic_filters, 
                items_with_numerator_denominator, totals_by_keyword, 
                combined_temporal_data, data_source, global_statistics, corte_fecha,
                temporal_breakdown_data
            )
            
        except Exception as e:
            print(f"Error generando reporte numerador/denominador: {e}")
            import traceback
            traceback.print_exc()
            raise Exception(f"Error en generación de reporte: {e}")
        
    def _get_table_columns(self, data_source: str) -> List[str]:
        """Obtiene columnas de la tabla"""
        try:
            describe_sql = f"DESCRIBE SELECT * FROM {data_source}"
            columns_result = duckdb_service.conn.execute(describe_sql).fetchall()
            columns = [row[0] for row in columns_result]
            return columns
        except Exception as e:
            print(f"Error obteniendo columnas: {e}")
            raise Exception("Error analizando estructura de datos")
    
    def _setup_keyword_rules(self, keywords: Optional[List[str]]) -> Optional[List[KeywordRule]]:
        """Configura reglas de palabras clave"""
        if not keywords:
            return None
        return [KeywordRule(name=k, synonyms=(k.lower(),)) for k in keywords]
    
    
    
    def _parse_date_flexible(self, date_field: str) -> str:
        """
        🔧 FUNCIÓN AUXILIAR: Parser de fechas flexible que maneja múltiples formatos
        
        Maneja:
        - DD/MM/YYYY (formato original)  
        - YYYY-MM-DD (formato ISO)
        - DD-MM-YYYY 
        - Otros formatos comunes
        """
        return f"""
        CASE
            -- Formato DD/MM/YYYY
            WHEN {date_field} ~ '^[0-9]{{1,2}}/[0-9]{{1,2}}/[0-9]{{4}}$' 
                THEN TRY_CAST(strptime({date_field}, '%d/%m/%Y') AS DATE)
            -- Formato YYYY-MM-DD (ISO)
            WHEN {date_field} ~ '^[0-9]{{4}}-[0-9]{{1,2}}-[0-9]{{1,2}}$'
                THEN TRY_CAST(strptime({date_field}, '%Y-%m-%d') AS DATE)
            -- Formato DD-MM-YYYY
            WHEN {date_field} ~ '^[0-9]{{1,2}}-[0-9]{{1,2}}-[0-9]{{4}}$'
                THEN TRY_CAST(strptime({date_field}, '%d-%m-%Y') AS DATE)
            -- Formato MM/DD/YYYY (estadounidense)
            WHEN {date_field} ~ '^[0-9]{{1,2}}/[0-9]{{1,2}}/[0-9]{{4}}$' AND 
                 CAST(split_part({date_field}, '/', 1) AS INTEGER) > 12
                THEN TRY_CAST(strptime({date_field}, '%m/%d/%Y') AS DATE)
            ELSE NULL
        END
        """
    
    def _calculate_denominator_excel_logic(
        self, data_source: str, age_range_obj, document_field: str, geo_filter: str,
        año: int, mes: int, edad_meses_field: str, corte_fecha: str, column_name: str
    ) -> int:
        """
        MÉTODO MENSUAL: Lógica exacta como Excel CON MANEJO FLEXIBLE DE FECHAS
        
        DENOMINADOR MENSUAL = Registros del mes + Registros vacíos
        Filtrados por edad según age_range_obj
        """
        try:
            # Obtener parámetros del age_range_obj
            min_age = getattr(age_range_obj, 'min_age', 1)
            max_age = getattr(age_range_obj, 'max_age', min_age)
            unit = getattr(age_range_obj, 'unit', 'months')
            
            # Construir filtro de edad
            if unit.lower() == 'months':
                edad_filter = f"{edad_meses_field} >= {min_age} AND {edad_meses_field} <= {max_age}"
            else:
                # Convertir años a meses
                min_months = min_age * 12
                max_months = max_age * 12 + 11
                edad_filter = f"{edad_meses_field} >= {min_months} AND {edad_meses_field} <= {max_months}"
            
            # Nombre de columna seguro para SQL
            column_safe = f'"{column_name}"' if not column_name.startswith('"') else column_name
            
            # PARSER FLEXIBLE DE FECHAS
            date_parser = self._parse_date_flexible(column_safe)
            
            # SQL ESTILO EXCEL CON PARSER FLEXIBLE: Registros del mes + Registros vacíos
            denominador_sql = f"""
            SELECT COUNT(DISTINCT {document_field}) as denominador_mensual
            FROM {data_source}
            WHERE 
                "Fecha Nacimiento" IS NOT NULL 
                AND TRY_CAST(strptime("Fecha Nacimiento", '%d/%m/%Y') AS DATE) IS NOT NULL
                AND strptime("Fecha Nacimiento", '%d/%m/%Y') <= DATE '{corte_fecha}'
                AND {document_field} IS NOT NULL
                AND {document_field} != ''
                AND {geo_filter}
                AND ({edad_filter})
                AND (
                    -- CASO 1: Registros con fecha en el mes específico (PARSER FLEXIBLE)
                    (
                        {column_safe} IS NOT NULL 
                        AND {column_safe} != '' 
                        AND {column_safe} != 'No'
                        AND ({date_parser}) IS NOT NULL
                        AND date_part('year', {date_parser}) = {año}
                        AND date_part('month', {date_parser}) = {mes}
                    )
                    OR
                    -- CASO 2: Registros vacíos (población elegible sin consulta)
                    (
                        {column_safe} IS NULL 
                        OR {column_safe} = '' 
                        OR {column_safe} = 'No'
                    )
                )
            """
            
            result = duckdb_service.conn.execute(denominador_sql).fetchone()
            denominador_mensual = int(result[0]) if result and result[0] else 0
            
            # Fallback si es 0
            if denominador_mensual == 0:
                denominador_mensual = self._calculate_fallback_denominator(
                    data_source, age_range_obj, document_field, geo_filter, 
                    edad_meses_field, corte_fecha
                )
            
            return denominador_mensual
            
        except Exception as e:
            print(f"         Error calculando denominador mensual: {e}")
            import traceback
            traceback.print_exc()
            return 0

    def _calculate_denominador_anual_correcto(
        self, data_source: str, age_range_obj, document_field: str, geo_filter: str,
        año: int, edad_meses_field: str, corte_fecha: str, column_name: str
    ) -> int:
        """
        MÉTODO ANUAL CORRECTO: Denominador anual = Registros de TODO el año + Registros vacíos (UNA SOLA VEZ)
        
        LÓGICA CORRECTA:
        - Registros con fecha en cualquier mes del año
        - + Registros vacíos (sin duplicar)
        - Filtrado por edad específica
        """
        try:
            # Obtener parámetros del age_range_obj
            min_age = getattr(age_range_obj, 'min_age', 1)
            max_age = getattr(age_range_obj, 'max_age', min_age)
            unit = getattr(age_range_obj, 'unit', 'months')
            
            # Construir filtro de edad
            if unit.lower() == 'months':
                edad_filter = f"{edad_meses_field} >= {min_age} AND {edad_meses_field} <= {max_age}"
            else:
                # Convertir años a meses
                min_months = min_age * 12
                max_months = max_age * 12 + 11
                edad_filter = f"{edad_meses_field} >= {min_months} AND {edad_meses_field} <= {max_months}"
            
            # Nombre de columna seguro para SQL
            column_safe = f'"{column_name}"' if not column_name.startswith('"') else column_name
            
            # PARSER FLEXIBLE DE FECHAS
            date_parser = self._parse_date_flexible(column_safe)
            
            # SQL CORRECTO PARA DENOMINADOR ANUAL
            denominador_anual_sql = f"""
            SELECT COUNT(DISTINCT {document_field}) as denominador_anual
            FROM {data_source}
            WHERE 
                "Fecha Nacimiento" IS NOT NULL 
                AND TRY_CAST(strptime("Fecha Nacimiento", '%d/%m/%Y') AS DATE) IS NOT NULL
                AND strptime("Fecha Nacimiento", '%d/%m/%Y') <= DATE '{corte_fecha}'
                AND {document_field} IS NOT NULL
                AND {document_field} != ''
                AND {geo_filter}
                AND ({edad_filter})
                AND (
                    -- CASO 1: Registros con fecha en CUALQUIER MES del año específico
                    (
                        {column_safe} IS NOT NULL 
                        AND {column_safe} != '' 
                        AND {column_safe} != 'No'
                        AND ({date_parser}) IS NOT NULL
                        AND date_part('year', {date_parser}) = {año}
                    )
                    OR
                    -- CASO 2: Registros vacíos (población elegible sin consulta - UNA SOLA VEZ)
                    (
                        {column_safe} IS NULL 
                        OR {column_safe} = '' 
                        OR {column_safe} = 'No'
                    )
                )
            """
            
            result = duckdb_service.conn.execute(denominador_anual_sql).fetchone()
            denominador_anual = int(result[0]) if result and result[0] else 0
            
            # Fallback si es 0
            if denominador_anual == 0:
                denominador_anual = self._calculate_fallback_denominator(
                    data_source, age_range_obj, document_field, geo_filter, 
                    edad_meses_field, corte_fecha
                )
            
            return denominador_anual
            
        except Exception as e:
            print(f"         Error calculando denominador anual: {e}")
            import traceback
            traceback.print_exc()
            return 0

    def _calculate_fallback_denominator(
        self, data_source: str, age_range_obj, document_field: str, geo_filter: str,
        edad_meses_field: str, corte_fecha: str
    ) -> int:
        """🔄 Fallback: Calcular denominador usando población total de esa edad"""
        try:
            min_age = getattr(age_range_obj, 'min_age', 1)
            max_age = getattr(age_range_obj, 'max_age', min_age)
            unit = getattr(age_range_obj, 'unit', 'months')
            
            # Usar el filtro de edad general
            if unit.lower() == 'months':
                edad_filter = f"{edad_meses_field} >= {min_age} AND {edad_meses_field} <= {max_age}"
            else:
                # Convertir años a meses
                min_months = min_age * 12
                max_months = max_age * 12 + 11
                edad_filter = f"{edad_meses_field} >= {min_months} AND {edad_meses_field} <= {max_months}"
            
            fallback_sql = f"""
            SELECT COUNT(DISTINCT {document_field}) 
            FROM {data_source}
            WHERE 
                "Fecha Nacimiento" IS NOT NULL 
                AND TRY_CAST(strptime("Fecha Nacimiento", '%d/%m/%Y') AS DATE) IS NOT NULL
                AND strptime("Fecha Nacimiento", '%d/%m/%Y') <= DATE '{corte_fecha}'
                AND {document_field} IS NOT NULL
                AND {document_field} != ''
                AND {geo_filter}
                AND ({edad_filter})
            """
            
            result = duckdb_service.conn.execute(fallback_sql).fetchone()
            total_poblacion = int(result[0]) if result and result[0] else 0
            
            # Estimar denominador mensual (dividir por 12)
            denominador_estimado = max(1, total_poblacion // 12) if total_poblacion > 0 else 1            
            return denominador_estimado
            
        except Exception as e:
            print(f"         Error en fallback: {e}")
            return 1
