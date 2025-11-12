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
    
    def _get_month_names(self) -> dict:
        """Retorna diccionario de nombres de meses"""
        return {
            1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
            5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto", 
            9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
        }


    def _extract_row_data(self, row: tuple) -> tuple:
        """Extrae y valida datos de una fila de resultado temporal"""
        column_name = str(row[0]) if row[0] is not None else ""
        keyword = str(row[1]) if row[1] is not None else ""
        age_range = str(row[2]) if row[2] is not None else ""
        year = int(row[3]) if row[3] is not None and row[3] > 0 else None
        month = int(row[4]) if row[4] is not None and row[4] > 0 else None
        count = int(row[5]) if row[5] is not None else 0
        
        return (column_name, keyword, age_range, year, month, count)


    def _initialize_column_structure(self, column_name: str, keyword: str, age_range: str) -> dict:
        """Inicializa estructura de datos para una columna"""
        return {
            "column": column_name,
            "keyword": keyword, 
            "age_range": age_range,
            "years": {}
        }


    def _initialize_year_structure(self, year: int) -> dict:
        """Inicializa estructura de datos para un año"""
        return {
            "year": year,
            "total": 0,
            "months": {}
        }


    def _add_month_data(self, year_data: dict, month: int, month_name: str, count: int):
        """Agrega datos de un mes a la estructura del año"""
        year_data["months"][month_name] = {
            "month": month,
            "month_name": month_name,
            "count": count
        }
        year_data["total"] += count


    def _process_temporal_row(self, row: tuple, temporal_data: dict, month_names: dict):
        """Procesa una fila individual de resultado temporal"""
        # Extraer datos de la fila
        column_name, keyword, age_range, year, month, count = self._extract_row_data(row)
        
        # Validar datos temporales
        if not self._is_valid_temporal_data(year, month):
            return
        
        # Crear clave de columna
        column_key = f"{column_name}|{keyword}|{age_range}"
        
        # Inicializar estructura de columna si no existe
        if column_key not in temporal_data:
            temporal_data[column_key] = self._initialize_column_structure(column_name, keyword, age_range)
        
        # Inicializar estructura de año si no existe
        year_str = str(year)
        if year_str not in temporal_data[column_key]["years"]:
            temporal_data[column_key]["years"][year_str] = self._initialize_year_structure(year)
        
        # Agregar datos del mes
        month_name = month_names.get(month, f"Mes {month}")
        self._add_month_data(
            temporal_data[column_key]["years"][year_str],
            month, month_name, count
        )


    def _process_temporal_results(self, temporal_result) -> Dict[str, Any]:
        """Procesa resultados temporales"""
        temporal_data = {}
        month_names = self._get_month_names()
        
        for row in temporal_result:
            self._process_temporal_row(row, temporal_data, month_names)
        
        return temporal_data

    
    def _is_valid_temporal_data(self, year, month) -> bool:
        """Valida datos temporales"""
        return (year and year > 1900 and year < 2100 and month and 1 <= month <= 12)