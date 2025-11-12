# services/technical_note_services/report_service.py - MODIFICADO PARA FECHA DINÁMICA
from typing import Dict, Any, List, Optional
from services.duckdb_service.duckdb_service import duckdb_service
from services.keyword_age_report import ColumnKeywordReportService, KeywordRule
from controllers.technical_note_controller.age_range_extractor import AgeRangeExtractor
from services.technical_note_services.report_service_aux.generate_report_service import GenerateReport

class ReportService:
    """Servicio especializado para generación de reportes CON NUMERADOR/DENOMINADOR"""
    
    def __init__(self):
        self.column_service = ColumnKeywordReportService()
        self.age_extractor = AgeRangeExtractor()

    def generate_keyword_age_report(
        self,
        data_source: str,
        filename: str,
        keywords: Optional[List[str]] = None,
        min_count: int = 0,
        include_temporal: bool = True,
        geographic_filters: Optional[Dict[str, Optional[str]]] = None,
        corte_fecha: str = None  # SIN VALOR POR DEFECTO - OBLIGATORIO
    ) -> Dict[str, Any]:
        """MODIFICADO: Pasar fecha dinámica al generador de reportes"""
        
        # VALIDAR QUE VENGA LA FECHA
        if not corte_fecha:
            raise ValueError("El parámetro 'corte_fecha' es obligatorio y debe venir desde el frontend")
        
        print(f"ReportService usando fecha dinámica: {corte_fecha}")
        
        return GenerateReport().generate_keyword_age_report(
            self.age_extractor,
            data_source,
            filename,
            keywords,
            min_count,
            include_temporal,
            geographic_filters,
            corte_fecha  # FECHA DINÁMICA
        )
    
    def _debug_age_range_coverage(
        self, data_source: str, age_range_obj, edad_meses_field: str, 
        edad_anios_field: str, geo_filter: str, corte_fecha: str, document_field: str
    ):
        """Debug con campo documento correcto"""
        try:
            if age_range_obj.unit == 'months' and age_range_obj.min_age != age_range_obj.max_age:
                debug_sql = f"""
                SELECT 
                    {edad_meses_field} as edad_meses,
                    COUNT(DISTINCT {document_field}) as poblacion
                FROM {data_source}
                WHERE 
                    {edad_meses_field} BETWEEN {age_range_obj.min_age} AND {age_range_obj.max_age}
                    AND "Fecha Nacimiento" IS NOT NULL 
                    AND TRY_CAST(strptime("Fecha Nacimiento", '%d/%m/%Y') AS DATE) IS NOT NULL
                    AND strptime("Fecha Nacimiento", '%d/%m/%Y') <= DATE '{corte_fecha}'
                    AND {document_field} IS NOT NULL
                    AND {geo_filter}
                GROUP BY {edad_meses_field}
                ORDER BY edad_meses
                """
                
                debug_result = duckdb_service.conn.execute(debug_sql).fetchall()
                print("DESGLOSE POR MES:")
                total_verification = 0
                for row in debug_result:
                    edad_mes = row[0]
                    poblacion = row[1]
                    total_verification += poblacion
                    print(f"         {edad_mes} meses: {poblacion:,} personas")
                print(f"      TOTAL VERIFICACIÓN: {total_verification:,}")
                
        except Exception as e:
            print(f"      Error en debug: {e}")
