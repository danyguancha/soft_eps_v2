from typing import Any, Dict
from services.duckdb_service.duckdb_service import duckdb_service

class AnalysisTemporal:
    def execute_temporal_analysis(self, service, data_source: str, matches, departamento, municipio, ips) -> Dict[str, Any]:
        """Ejecuta análisis temporal"""
        temporal_data = {}
        try:
            temporal_sql = service.build_temporal_report_sql_with_filters(
                data_source, matches, duckdb_service.escape_identifier,
                departamento, municipio, ips
            )
            temporal_result = duckdb_service.conn.execute(temporal_sql).fetchall()
            temporal_data = self._process_temporal_results(temporal_result)
            print(f"Análisis temporal: {len(temporal_data)} columnas")
        except Exception as e:
            print(f"Error temporal (continuando): {e}")
        return temporal_data
    
    def _process_temporal_results(self, temporal_result) -> Dict[str, Any]:
        """Procesa resultados temporales"""
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
        return (year and year > 1900 and year < 2100 and month and 1 <= month <= 12)