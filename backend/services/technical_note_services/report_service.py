# services/technical_note_services/report_service.py
from typing import Dict, Any, List, Optional
from services.duckdb_service.duckdb_service import duckdb_service
from services.keyword_age_report import ColumnKeywordReportService, KeywordRule

class ReportService:
    """Servicio especializado para generación de reportes"""
    
    def __init__(self):
        self.column_service = ColumnKeywordReportService()
    
    def generate_keyword_age_report(
        self,
        data_source: str,
        filename: str,
        keywords: Optional[List[str]] = None,
        min_count: int = 0,
        include_temporal: bool = True,
        geographic_filters: Optional[Dict[str, Optional[str]]] = None
    ) -> Dict[str, Any]:
        """Genera reporte de palabras clave y edades"""
        try:
            print(f"📊 Generando reporte para: {filename}")
            
            geographic_filters = geographic_filters or {}
            departamento = geographic_filters.get('departamento')
            municipio = geographic_filters.get('municipio') 
            ips = geographic_filters.get('ips')
            
            print(f"🗺️ Filtros: Dept={departamento}, Mun={municipio}, IPS={ips}")
            
            # Obtener columnas
            columns = self._get_table_columns(data_source)
            
            # Configurar reglas de keywords
            rules = self._setup_keyword_rules(keywords)
            service = ColumnKeywordReportService(keywords=rules)
            matches = service.match_columns(columns)
            
            if not matches:
                return self._build_empty_report(filename, keywords, geographic_filters)
            
            # Ejecutar consulta principal
            items = self._execute_main_query(
                service, data_source, matches, 
                departamento, municipio, ips, min_count
            )
            
            # Análisis temporal si se requiere
            temporal_data = {}
            if include_temporal and items:
                temporal_data = self._execute_temporal_analysis(
                    service, data_source, matches,
                    departamento, municipio, ips
                )
            
            # Calcular totales
            totals_by_keyword = self._calculate_totals(items)
            
            return self._build_success_report(
                filename, keywords, geographic_filters, 
                items, totals_by_keyword, temporal_data, data_source
            )
            
        except Exception as e:
            print(f"❌ Error generando reporte: {e}")
            raise Exception(f"Error en generación de reporte: {e}")
    
    def _get_table_columns(self, data_source: str) -> List[str]:
        """Obtiene columnas de la tabla"""
        try:
            describe_sql = f"DESCRIBE SELECT * FROM {data_source}"
            columns_result = duckdb_service.conn.execute(describe_sql).fetchall()
            columns = [row[0] for row in columns_result]
            print(f"📋 Columnas obtenidas: {len(columns)} columnas")
            return columns
        except Exception as e:
            print(f"❌ Error obteniendo columnas: {e}")
            raise Exception("Error analizando estructura de datos")
    
    def _setup_keyword_rules(self, keywords: Optional[List[str]]) -> Optional[List[KeywordRule]]:
        """Configura reglas de palabras clave"""
        if not keywords:
            return None
        return [KeywordRule(name=k, synonyms=(k.lower(),)) for k in keywords]
    
    def _execute_main_query(
        self, service, data_source: str, matches, 
        departamento, municipio, ips, min_count: int
    ) -> List[Dict[str, Any]]:
        """Ejecuta la consulta principal del reporte"""
        basic_sql = service.build_report_sql_with_filters(
            data_source, matches, duckdb_service.escape_identifier,
            departamento, municipio, ips
        )
        
        try:
            basic_result = duckdb_service.conn.execute(basic_sql).fetchall()
            print(f"✅ SQL básico ejecutado: {len(basic_result)} filas")
            
            items = []
            for row in basic_result:
                count = int(row[3]) if row[3] is not None else 0
                if count >= min_count:
                    items.append({
                        "column": str(row[0]) if row[0] is not None else "",
                        "keyword": str(row[1]) if row[1] is not None else "",
                        "age_range": str(row[2]) if row[2] is not None else "",
                        "count": count
                    })
            
            return items
            
        except Exception as e:
            print(f"❌ Error en consulta principal: {e}")
            raise Exception(f"Error ejecutando consulta: {e}")
    
    def _execute_temporal_analysis(
        self, service, data_source: str, matches,
        departamento, municipio, ips
    ) -> Dict[str, Any]:
        """Ejecuta análisis temporal"""
        temporal_data = {}
        
        try:
            temporal_sql = service.build_temporal_report_sql_with_filters(
                data_source, matches, duckdb_service.escape_identifier,
                departamento, municipio, ips
            )
            
            temporal_result = duckdb_service.conn.execute(temporal_sql).fetchall()
            print(f"✅ SQL temporal ejecutado: {len(temporal_result)} filas")
            
            temporal_data = self._process_temporal_results(temporal_result)
            print(f"📊 Análisis temporal completado: {len(temporal_data)} columnas")
            
        except Exception as e:
            print(f"⚠️ Error temporal (continuando): {e}")
        
        return temporal_data
    
    def _process_temporal_results(self, temporal_result) -> Dict[str, Any]:
        """Procesa resultados del análisis temporal"""
        temporal_data = {}
        
        month_names = {
            1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
            5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto", 
            9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
        }
        
        for row in temporal_result:
            column_name = str(row[0]) if row[0] is not None else ""
            keyword = str(row[1]) if row[1] is not None else ""
            age_range = str(row[2]) if row[2] is not None else ""
            year = int(row[3]) if row[3] is not None and row[3] > 0 else None
            month = int(row[4]) if row[4] is not None and row[4] > 0 else None
            count = int(row[5]) if row[5] is not None else 0
            
            if self._is_valid_temporal_data(year, month):
                column_key = f"{column_name}|{keyword}|{age_range}"
                
                if column_key not in temporal_data:
                    temporal_data[column_key] = {
                        "column": column_name,
                        "keyword": keyword, 
                        "age_range": age_range,
                        "years": {}
                    }
                
                year_str = str(year)
                if year_str not in temporal_data[column_key]["years"]:
                    temporal_data[column_key]["years"][year_str] = {
                        "year": year,
                        "total": 0,
                        "months": {}
                    }
                
                month_name = month_names.get(month, f"Mes {month}")
                temporal_data[column_key]["years"][year_str]["months"][month_name] = {
                    "month": month,
                    "month_name": month_name,
                    "count": count
                }
                temporal_data[column_key]["years"][year_str]["total"] += count
        
        return temporal_data
    
    def _is_valid_temporal_data(self, year, month) -> bool:
        """Valida datos temporales"""
        return (year and year > 1900 and year < 2100 and 
                month and 1 <= month <= 12)
    
    def _calculate_totals(self, items: List[Dict[str, Any]]) -> Dict[str, int]:
        """Calcula totales por palabra clave"""
        totals_by_keyword = {}
        for item in items:
            keyword = item["keyword"]
            totals_by_keyword[keyword] = totals_by_keyword.get(keyword, 0) + item["count"]
        return totals_by_keyword
    
    def _build_empty_report(
        self, filename: str, keywords: Optional[List[str]], 
        geographic_filters: Dict[str, Optional[str]]
    ) -> Dict[str, Any]:
        """Construye reporte vacío cuando no hay matches"""
        return {
            "success": True,
            "filename": filename,
            "rules": {"keywords": keywords or []},
            "geographic_filters": {**geographic_filters, "filter_type": "duckdb_service_methods"},
            "items": [],
            "totals_by_keyword": {},
            "temporal_data": {},
            "message": "No se encontraron columnas con las palabras clave especificadas"
        }
    
    def _build_success_report(
        self, filename: str, keywords: Optional[List[str]], 
        geographic_filters: Dict[str, Optional[str]], 
        items: List[Dict[str, Any]], 
        totals_by_keyword: Dict[str, int], 
        temporal_data: Dict[str, Any], 
        data_source: str
    ) -> Dict[str, Any]:
        """Construye reporte exitoso"""
        return {
            "success": True,
            "filename": filename,
            "rules": {"keywords": keywords or []},
            "geographic_filters": {**geographic_filters, "filter_type": "duckdb_service_methods"},
            "items": items,
            "totals_by_keyword": totals_by_keyword,
            "temporal_data": temporal_data,
            "ultra_fast": True,
            "engine": "DuckDB_Service_Existing_Methods",
            "data_source_used": data_source
        }
