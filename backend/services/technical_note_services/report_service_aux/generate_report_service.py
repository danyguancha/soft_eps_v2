from datetime import datetime
from typing import Dict, Any, List, Optional
from services.duckdb_service.duckdb_service import duckdb_service
from services.keyword_age_report import ColumnKeywordReportService, KeywordRule
from services.technical_note_services.report_service_aux.analysis_breakdown_temporal import AnalysisBreakdownTemporal
from services.technical_note_services.report_service_aux.analysis_numerador_denominador import AnalysisNumeratorDenominator
from services.technical_note_services.report_service_aux.corrected_months import CorrectedMonths
from services.technical_note_services.report_service_aux.corrected_years import CorrectedYear
from services.technical_note_services.report_service_aux.identity_document import IdentityDocument
from services.technical_note_services.report_service_aux.report_empty import ReportEmpty
from services.technical_note_services.report_service_aux.report_exporter import ReportExporter
from services.technical_note_services.report_service_aux.semaforization import Semaforization
from services.technical_note_services.report_service_aux.statistics import Statistics
from utils.keywords_NT import KeywordRule
from .analysis_temporal import AnalysisTemporal
from .analysis_vaccination import AnalysisVaccination


def log(msg):
    with open('generate_report.txt', 'a', encoding='utf-8') as f:
        f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {msg}\n")


class GenerateReport:
    def __init__(self):
        self.exporter = ReportExporter()
    
    def _build_age_filter(self, age_range_obj, corte_fecha: str) -> str:
        """Construye filtro de edad unificado para meses o años"""
        min_age = getattr(age_range_obj, 'min_age', 1)
        max_age = getattr(age_range_obj, 'max_age', min_age)
        unit = getattr(age_range_obj, 'unit', 'months')
        
        base_calc = f"""(
            (date_part('year', DATE '{corte_fecha}') - date_part('year', strptime("Fecha Nacimiento", '%d/%m/%Y'))) * 12
            + (date_part('month', DATE '{corte_fecha}') - date_part('month', strptime("Fecha Nacimiento", '%d/%m/%Y')))
            + CASE 
                WHEN date_part('day', strptime("Fecha Nacimiento", '%d/%m/%Y')) <= date_part('day', DATE '{corte_fecha}')
                THEN 0 ELSE -1
            END
        )"""
        
        if unit.lower() == 'months':
            return f"{base_calc} BETWEEN {min_age} AND {max_age}"
        else:
            min_months = min_age * 12
            max_months = (max_age + 1) * 12 - 1
            return f"{base_calc} BETWEEN {min_months} AND {max_months}"
    
    def _build_geo_filter(self, departamento: str, municipio: str, ips: str) -> str:
        """Construye filtro geográfico SQL"""
        conditions = []
        filters = [
            (departamento, '"Departamento"'),
            (municipio, '"Municipio"'),
            (ips, '"Nombre IPS"')
        ]
        
        for value, field in filters:
            if value and value != 'Todos':
                conditions.append(f"{field} = '{value}'")
        
        return " AND ".join(conditions) if conditions else "1=1"
    
    def _parse_date_flexible(self, date_field: str) -> str:
        """Parseo flexible de fechas en múltiples formatos"""
        return f"""
        CASE
            WHEN {date_field} ~ '^[0-9]{{1,2}}/[0-9]{{1,2}}/[0-9]{{4}}$' 
                THEN TRY_CAST(strptime({date_field}, '%d/%m/%Y') AS DATE)
            WHEN {date_field} ~ '^[0-9]{{4}}-[0-9]{{1,2}}-[0-9]{{1,2}}$'
                THEN TRY_CAST(strptime({date_field}, '%Y-%m-%d') AS DATE)
            WHEN {date_field} ~ '^[0-9]{{1,2}}-[0-9]{{1,2}}-[0-9]{{4}}$'
                THEN TRY_CAST(strptime({date_field}, '%d-%m-%Y') AS DATE)
            WHEN {date_field} ~ '^[0-9]{{1,2}}/[0-9]{{1,2}}/[0-9]{{4}}$' AND 
                 CAST(split_part({date_field}, '/', 1) AS INTEGER) > 12
                THEN TRY_CAST(strptime({date_field}, '%m/%d/%Y') AS DATE)
            ELSE NULL
        END
        """
    
    def _calculate_denominator_unified(
        self, data_source: str, age_range_obj, document_field: str, geo_filter: str,
        corte_fecha: str, column_name: str, anio: int, mes: int = None
    ) -> int:
        """Calcula denominador unificado para mensual o anual según parámetros"""
        try:
            period = f"{anio}/{mes:02d}" if mes else f"anio {anio}"
            log(f"\n   Calculando DENOMINADOR {period}")
            log(f"      Rango edad: {age_range_obj.min_age}-{age_range_obj.max_age} {age_range_obj.unit}")
            log(f"      Fecha corte: {corte_fecha}")
            
            edad_filter = self._build_age_filter(age_range_obj, corte_fecha)
            column_safe = f'"{column_name}"' if not column_name.startswith('"') else column_name
            date_parser = self._parse_date_flexible(column_safe)
            
            # Condición temporal: si mes existe, filtrar por mes; si no, filtrar por año
            temporal_condition = f"""
                date_part('year', {date_parser}) = {anio}
                {f"AND date_part('month', {date_parser}) = {mes}" if mes else ""}
            """
            
            log(f"      Filtro: Consultas en {period} O vacías")
            
            sql = f"""
            SELECT COUNT({document_field}) as denominador
            FROM {data_source}
            WHERE 
                ({edad_filter})
                AND "Fecha Nacimiento" IS NOT NULL 
                AND TRIM("Fecha Nacimiento") != ''
                AND TRY_CAST(strptime("Fecha Nacimiento", '%d/%m/%Y') AS DATE) IS NOT NULL
                AND strptime("Fecha Nacimiento", '%d/%m/%Y') <= DATE '{corte_fecha}'
                AND {document_field} IS NOT NULL
                AND TRIM({document_field}) != ''
                AND {geo_filter}
                AND (
                    (
                        {column_safe} IS NOT NULL 
                        AND TRIM(CAST({column_safe} AS VARCHAR)) != ''
                        AND TRIM(CAST({column_safe} AS VARCHAR)) NOT IN ('NULL', 'null', 'None', 'none', 'NaN', 'nan', 'N/A', 'n/a', '-', 'No')
                        AND ({date_parser}) IS NOT NULL
                        AND {temporal_condition}
                    )
                    OR
                    (
                        {column_safe} IS NULL 
                        OR TRIM(CAST({column_safe} AS VARCHAR)) = ''
                        OR TRIM(CAST({column_safe} AS VARCHAR)) IN ('NULL', 'null', 'None', 'none', 'NaN', 'nan', 'N/A', 'n/a', '-', 'No')
                    )
                )
            """
            
            log(f"      SQL (primeros 300 chars): {sql[:300]}...")
            
            result = duckdb_service.conn.execute(sql).fetchone()
            denominador = int(result[0]) if result and result[0] else 0
            
            log(f"      DENOMINADOR {period}: {denominador:,}")
            
            if denominador > 0:
                self._log_debug_breakdown(data_source, edad_filter, document_field, geo_filter, 
                                         column_safe, date_parser, temporal_condition, mes, anio)
            
            if denominador == 0:
                log("Denominador = 0, usando fallback")
                denominador = self._calculate_fallback_denominator(
                    data_source, age_range_obj, document_field, geo_filter, corte_fecha
                )
            
            return denominador
            
        except Exception as e:
            log(f"         Error calculando denominador: {e}")
            import traceback
            traceback.print_exc()
            return 0
    
    def _log_debug_breakdown(self, data_source: str, edad_filter: str, document_field: str,
                            geo_filter: str, column_safe: str, date_parser: str, 
                            temporal_condition: str, mes: int = None, anio: int = None):
        """Log debug del desglose con/sin consulta"""
        debug_sql = f"""
        SELECT 
            COUNT(CASE 
                WHEN {column_safe} IS NOT NULL 
                    AND TRIM(CAST({column_safe} AS VARCHAR)) != ''
                    AND TRIM(CAST({column_safe} AS VARCHAR)) NOT IN ('NULL', 'null', 'None', 'none', 'NaN', 'nan', 'N/A', 'n/a', '-', 'No')
                    AND ({date_parser}) IS NOT NULL
                    AND {temporal_condition}
                THEN 1 END) as con_consulta,
            COUNT(CASE 
                WHEN {column_safe} IS NULL 
                    OR TRIM(CAST({column_safe} AS VARCHAR)) = ''
                    OR TRIM(CAST({column_safe} AS VARCHAR)) IN ('NULL', 'null', 'None', 'none', 'NaN', 'nan', 'N/A', 'n/a', '-', 'No')
                THEN 1 END) as sin_consulta
        FROM {data_source}
        WHERE 
            ({edad_filter})
            AND "Fecha Nacimiento" IS NOT NULL 
            AND {document_field} IS NOT NULL
            AND {geo_filter}
            AND (
                (
                    {column_safe} IS NOT NULL 
                    AND TRIM(CAST({column_safe} AS VARCHAR)) != ''
                    AND TRIM(CAST({column_safe} AS VARCHAR)) NOT IN ('NULL', 'null', 'None', 'none', 'NaN', 'nan', 'N/A', 'n/a', '-', 'No')
                    AND ({date_parser}) IS NOT NULL
                    AND {temporal_condition}
                )
                OR
                (
                    {column_safe} IS NULL 
                    OR TRIM(CAST({column_safe} AS VARCHAR)) = ''
                    OR TRIM(CAST({column_safe} AS VARCHAR)) IN ('NULL', 'null', 'None', 'none', 'NaN', 'nan', 'N/A', 'n/a', '-', 'No')
                )
            )
        """
        
        try:
            debug_result = duckdb_service.conn.execute(debug_sql).fetchone()
            con_consulta = debug_result[0] if debug_result else 0
            sin_consulta = debug_result[1] if debug_result else 0
            period = f"{mes}/{anio}" if mes else str(anio)
            log(f"Con consulta en {period}: {con_consulta:,}")
            log(f"Sin consulta: {sin_consulta:,}")
            log(f"TOTAL: {con_consulta + sin_consulta:,}")
        except Exception as debug_error:
            log(f"         Error en debug: {debug_error}")
    
    def _calculate_fallback_denominator(
        self, data_source: str, age_range_obj, document_field: str, geo_filter: str, corte_fecha: str
    ) -> int:
        """Calcula denominador fallback basado en población total en rango"""
        try:
            min_age = getattr(age_range_obj, 'min_age', 1)
            max_age = getattr(age_range_obj, 'max_age', min_age)
            unit = getattr(age_range_obj, 'unit', 'months')
            
            edad_meses_field = f"date_diff('month', strptime(\"Fecha Nacimiento\", '%d/%m/%Y'), DATE '{corte_fecha}')"
            
            if unit.lower() == 'months':
                edad_filter = f"{edad_meses_field} >= {min_age} AND {edad_meses_field} <= {max_age}"
            else:
                min_months = min_age * 12
                max_months = max_age * 12 + 11
                edad_filter = f"{edad_meses_field} >= {min_months} AND {edad_meses_field} <= {max_months}"
            
            sql = f"""
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
            
            result = duckdb_service.conn.execute(sql).fetchone()
            total_poblacion = int(result[0]) if result and result[0] else 0
            
            log(f"         Fallback: {total_poblacion:,} personas en el rango")
            return total_poblacion if total_poblacion > 0 else 1
            
        except Exception as e:
            log(f"         Error en fallback: {e}")
            return 1
    
    def _process_semaforizacion_data(self, data: dict, numerador: int, denominador: int) -> int:
        """Procesa semaforización para cualquier tipo de dato (mes/año/item)"""
        porcentaje = round((numerador / denominador) * 100, 2) if denominador > 0 else 0.0
        semaforizacion = Semaforization().calculate_semaforizacion(numerador, porcentaje)
        
        data['denominador'] = denominador
        data['den'] = denominador
        data['num'] = numerador
        data['pct'] = porcentaje
        data['cobertura_porcentaje'] = porcentaje
        data['semaforizacion'] = semaforizacion['estado']
        data['color'] = semaforizacion['color']
        data['color_name'] = semaforizacion['color_name']
        data['descripcion'] = semaforizacion['descripcion']
        
        return numerador
    
    def _merge_temporal_breakdown_into_combined(self, temporal_breakdown_data: dict, 
                                                combined_temporal_data: dict):
        """Merge temporal breakdown data into combined temporal data"""
        combined_temporal_data.clear()
        
        for key, breakdown_data in temporal_breakdown_data.items():
            combined_temporal_data[key] = {
                "column": breakdown_data.get('column'),
                "keyword": breakdown_data.get('keyword'),
                "age_range": breakdown_data.get('age_range'),
                "years": {}
            }
            
            for year_str, year_data in breakdown_data.get('temporal_breakdown', {}).items():
                combined_temporal_data[key]["years"][year_str] = {
                    "year": int(year_str),
                    "total": year_data.get("total_numerador", 0),
                    "months": {
                        month_name: {
                            "month": month_data.get("month"),
                            "month_name": month_name,
                            "count": month_data.get("numerador", 0),
                            "numerador": month_data.get("numerador", 0),
                            "denominador": month_data.get("denominador", 0),
                            "cobertura_porcentaje": month_data.get("cobertura_porcentaje", 0.0)
                        }
                        for month_name, month_data in year_data.get("months", {}).items()
                    }
                }
    
    def _calculate_temporal_denominators_for_data(self, combined_temporal_data: dict, data_source: str,
                                                age_extractor, document_field: str, geo_filter: str,
                                                corte_fecha: str):
        """Calcula denominadores y semaforización para datos temporales"""
        for data in combined_temporal_data.values():
            if 'years' not in data:
                continue
            
            age_range_obj = age_extractor.extract_age_range(data.get('column', ''))
            if not age_range_obj:
                continue
            
            for year_str, year_data in data['years'].items():
                total_numerador_anual = 0
                
                # Procesar cada mes
                for month_data in year_data.get('months', {}).values():
                    mes_num = month_data.get('month')
                    if not mes_num:
                        continue
                    
                    denominador_mensual = self._calculate_denominator_unified(
                        data_source, age_range_obj, document_field, geo_filter,
                        corte_fecha, data.get('column', ''), int(year_str), mes_num
                    )
                    
                    total_numerador_anual += self._process_semaforizacion_data(
                        month_data, month_data.get('numerador', 0), denominador_mensual
                    )
                
                # Calcular denominador anual
                denominador_anual = self._calculate_denominator_unified(
                    data_source, age_range_obj, document_field, geo_filter,
                    corte_fecha, data.get('column', ''), int(year_str)
                )
                
                self._process_semaforizacion_data(year_data, total_numerador_anual, denominador_anual)
                year_data['total'] = total_numerador_anual
                year_data['total_num'] = total_numerador_anual
                year_data['total_den'] = denominador_anual
    
    def generate_keyword_age_report(
        self,
        age_extractor,
        data_source: str,
        filename: str,
        keywords: Optional[List[str]] = None,
        min_count: int = 0,
        include_temporal: bool = True,
        geographic_filters: Optional[Dict[str, Optional[str]]] = None,
        corte_fecha: str = None,
    ) -> Dict[str, Any]:
        """Genera reporte de keywords y edad con análisis completo"""
        try:
            # Validar fecha de corte
            if not corte_fecha:
                raise ValueError("El parámetro 'corte_fecha' es obligatorio y debe venir desde el frontend")
            
            log(f"\n{'='*60}")
            log("GENERANDO REPORTE CON FECHA DINÁMICA")
            log(f"{'='*60}")
            log(f"Fecha de corte RECIBIDA: {corte_fecha}")
            
            # Extraer filtros geográficos
            geographic_filters = geographic_filters or {}
            departamento = geographic_filters.get('departamento')
            municipio = geographic_filters.get('municipio')
            ips = geographic_filters.get('ips')
            
            # Ejecutar matching
            columns = self._get_table_columns(data_source)
            rules = [KeywordRule(name=k, synonyms=(k.lower(),)) for k in keywords] if keywords else None
            service = ColumnKeywordReportService(keywords=rules)
            matches = service.match_columns(columns)
            
            if not matches:
                return ReportEmpty().build_empty_report(filename, keywords, geographic_filters)
            
            # Análisis numerador/denominador
            items_with_numerator_denominator = AnalysisNumeratorDenominator().execute_numerator_denominator_analysis(
                data_source, matches, departamento, municipio, ips, min_count, corte_fecha, age_extractor
            )
            
            # Análisis temporales
            temporal_breakdown_data = {}
            combined_temporal_data = {}
            
            if include_temporal and items_with_numerator_denominator:
                temporal_breakdown_data = AnalysisBreakdownTemporal().execute_temporal_breakdown_analysis(
                    data_source, matches, departamento, municipio, ips, corte_fecha, age_extractor
                )
                
                temporal_data = AnalysisTemporal().execute_temporal_analysis(
                    service, data_source, matches, departamento, municipio, ips
                )
                
                vaccination_states_data = AnalysisVaccination().execute_vaccination_states_analysis(
                    data_source, matches, departamento, municipio, ips
                )
                
                combined_temporal_data.update(temporal_data)
                combined_temporal_data.update(vaccination_states_data)
            
            # Calcular totales y estadísticas
            totals_by_keyword = AnalysisNumeratorDenominator().calculate_totals_with_numerator_denominator(
                items_with_numerator_denominator
            )
            global_statistics = Statistics().calculate_global_statistics(items_with_numerator_denominator)
            
            # Merge temporal breakdown
            self._merge_temporal_breakdown_into_combined(temporal_breakdown_data, combined_temporal_data)
            
            # Setup campos de documento y edad
            try:
                document_field = IdentityDocument().get_document_field(data_source)
            except Exception as e:
                log(f"⚠️ Usando campo documento por defecto: {e}")
                document_field = '"Nro Identificación"'
            
            # Construir filtro geográfico
            geo_filter = self._build_geo_filter(departamento, municipio, ips)
            
            # Calcular denominadores temporales y semaforización
            self._calculate_temporal_denominators_for_data(
                combined_temporal_data, data_source, age_extractor, document_field,
                geo_filter, corte_fecha
            )
            
            # Aplicar semaforización a items
            for item in items_with_numerator_denominator:
                self._process_semaforizacion_data(
                    item, item.get('numerador', 0), item.get('denominador', 0)
                )
            
            log(f"✅ Reporte generado exitosamente con fecha: {corte_fecha}")
            
            # Construir reporte final
            return AnalysisNumeratorDenominator().build_success_report_with_numerator_denominator(
                filename, keywords, geographic_filters, items_with_numerator_denominator,
                totals_by_keyword, combined_temporal_data, data_source, global_statistics,
                corte_fecha, temporal_breakdown_data
            )
            
        except Exception as e:
            log(f"❌ Error generando reporte: {e}")
            import traceback
            traceback.print_exc()
            raise ValueError(f"Error en generación de reporte: {e}")
    
    def _get_table_columns(self, data_source: str) -> List[str]:
        """Obtiene columnas de la tabla"""
        try:
            describe_sql = f"DESCRIBE SELECT * FROM {data_source}"
            columns_result = duckdb_service.conn.execute(describe_sql).fetchall()
            return [row[0] for row in columns_result]
        except Exception as e:
            log(f"Error obteniendo columnas: {e}")
            raise ValueError("Error analizando estructura de datos")
    
    def export_report_csv(self, report_data: Dict[str, Any], output_path: str, include_temporal: bool = True) -> str:
        """Exporta reporte a CSV"""
        try:
            return self.exporter.export_to_csv(report_data, output_path, include_temporal)
        except Exception as e:
            log(f"Error exportando CSV: {e}")
            raise ValueError(f"Error en exportación CSV: {e}")
    
    def export_report_pdf(self, report_data: Dict[str, Any], output_path: str, include_temporal: bool = True) -> str:
        """Exporta reporte a PDF"""
        try:
            return self.exporter.export_to_pdf(report_data, output_path, include_temporal)
        except Exception as e:
            log(f"Error exportando PDF: {e}")
            raise ValueError(f"Error en exportación PDF: {e}")
    
    def export_report_all_formats(self, report_data: Dict[str, Any], base_filename: str, 
                                 export_csv: bool = True, export_pdf: bool = True, 
                                 include_temporal: bool = True) -> Dict[str, str]:
        """Exporta reporte en todos los formatos"""
        try:
            return self.exporter.export_report(report_data, base_filename, export_csv, export_pdf, include_temporal)
        except Exception as e:
            log(f"Error exportando reporte: {e}")
            raise ValueError(f"Error en exportación: {e}")
    
    def generate_and_export_report(
        self,
        age_extractor, data_source: str, filename: str,
        keywords: Optional[List[str]] = None, min_count: int = 0,
        include_temporal: bool = True, geographic_filters: Optional[Dict[str, Optional[str]]] = None,
        corte_fecha: str = None,
        export_csv: bool = True,
        export_pdf: bool = True, base_export_path: str = "exports/reporte"
    ) -> Dict[str, Any]:
        """Genera y exporta reporte completo"""
        try:
            if not corte_fecha:
                raise ValueError("El parámetro 'corte_fecha' es obligatorio")
            
            log(f"Generando reporte con fecha: {corte_fecha}")
            report_data = self.generate_keyword_age_report(
                age_extractor, data_source, filename, keywords, min_count,
                include_temporal, geographic_filters, corte_fecha
            )
            
            exported_files = {}
            if export_csv or export_pdf:
                log("Exportando archivos...")
                exported_files = self.export_report_all_formats(
                    report_data, base_export_path, export_csv, export_pdf, include_temporal
                )
            
            log(f"Proceso completado con fecha: {corte_fecha}")
            
            return {
                "report": report_data,
                "exported_files": exported_files,
                "success": True,
                "message": "Reporte generado y exportado exitosamente",
                "corte_fecha_usado": corte_fecha
            }
            
        except Exception as e:
            log(f"Error en proceso completo: {e}")
            import traceback
            traceback.print_exc()
            raise ValueError(f"Error en generación y exportación: {e}")
