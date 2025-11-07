from typing import Any, Dict
from services.duckdb_service.duckdb_service import duckdb_service

class AnalysisVaccination:
    def execute_vaccination_states_analysis(self, data_source: str, matches, departamento, municipio, ips) -> Dict[str, Any]:
        """Ejecuta análisis de estados de vacunación"""
        states_data = {}
        try:
            vaccination_columns = []
            for match in matches:
                column_name = match['column']
                if any(keyword in column_name.lower() for keyword in ['vacunación', 'vacunacion']):
                    if self._column_has_states(data_source, column_name):
                        vaccination_columns.append(match)
            
            if vaccination_columns:
                states_sql = self._build_vaccination_states_sql_simple(
                    data_source, vaccination_columns, 
                    duckdb_service.escape_identifier,
                    departamento, municipio, ips
                )
                states_result = duckdb_service.conn.execute(states_sql).fetchall()
                states_data = self._process_vaccination_states_results(states_result)
                print(f"Estados de vacunación: {len(states_data)} entradas")
                
        except Exception as e:
            print(f"Error análisis estados: {e}")
        
        return states_data
    
    def _process_vaccination_states_results(self, states_result) -> Dict[str, Any]:
        """Procesa resultados de estados"""
        states_data = {}
        for row in states_result:
            column_name = str(row[0]) if row[0] is not None else ""
            keyword = str(row[1]) if row[1] is not None else ""
            age_range = str(row[2]) if row[2] is not None else ""
            estado = str(row[3]) if row[3] is not None else ""
            count = int(row[4]) if row[4] is not None else 0
            
            if estado in ['Completo', 'Incompleto'] and count > 0:
                column_key = f"{column_name}|{keyword}|{age_range}"
                if column_key not in states_data:
                    states_data[column_key] = {
                        "column": column_name,
                        "keyword": keyword,
                        "age_range": age_range,
                        "type": "states",
                        "states": {}
                    }
                states_data[column_key]["states"][estado] = {"state": estado, "count": count}
        
        return states_data
    
    def _build_vaccination_states_sql_simple(self, data_source: str, vaccination_columns, escape_func, departamento, municipio, ips) -> str:
        """Construye SQL para estados de vacunación"""
        queries = []
        for match in vaccination_columns:
            column_name = match['column']
            keyword = match['keyword']
            age_range = match['age_range']
            escaped_column = escape_func(column_name)
            
            where_conditions = []
            if departamento: where_conditions.append(f"{escape_func('departamento')} = '{departamento}'")
            if municipio: where_conditions.append(f"{escape_func('municipio')} = '{municipio}'")
            if ips: where_conditions.append(f"{escape_func('ips')} = '{ips}'")
            base_where = " AND ".join(where_conditions) if where_conditions else "1=1"
            
            for estado in ['Completo', 'Incompleto']:
                state_condition = "completo" if estado == "Completo" else "incompleto"
                query = f"""
                SELECT '{column_name}', '{keyword}', '{age_range}', '{estado}', COUNT(*)
                FROM {data_source}
                WHERE {base_where} AND {escaped_column} IS NOT NULL
                AND (TRIM(LOWER({escaped_column})) = '{state_condition}' 
                     OR TRIM(LOWER({escaped_column})) = '{state_condition.replace("completo", "complete").replace("incompleto", "incomplete")}')
                """
                queries.append(query)
        
        return " UNION ALL ".join(queries) + " ORDER BY 1, 4" if queries else "SELECT 'no_data', 'no_data', 'no_data', 'no_data', 0 WHERE 1=0"
    
    def _column_has_states(self, data_source: str, column_name: str) -> bool:
        """Verifica si columna tiene estados"""
        try:
            escaped_column = duckdb_service.escape_identifier(column_name)
            check_sql = f"""
            SELECT DISTINCT {escaped_column} FROM {data_source} 
            WHERE {escaped_column} IS NOT NULL 
            AND TRIM(LOWER({escaped_column})) IN ('completo', 'incompleto', 'complete', 'incomplete')
            LIMIT 1
            """
            result = duckdb_service.conn.execute(check_sql).fetchall()
            return len(result) > 0
        except:
            return False