import re
import time
from typing import List, Dict, Any, Optional

class SQLUtils:
    """Utilidades para construcción y manejo de SQL"""
    
    def sanitize_table_name(self, table_name: str) -> str:
        """Convierte nombres de tabla a formato seguro para DuckDB"""
        
        # Reemplazar guiones con guiones bajos y otros caracteres problemáticos
        sanitized = table_name.replace('-', '_')
        sanitized = re.sub(r'\W', '_', sanitized) 
        
        # Asegurar que no comience con número
        if sanitized and sanitized[0].isdigit():
            sanitized = "t_" + sanitized
        
        # Asegurar que no esté vacío
        if not sanitized:
            sanitized = "table_" + str(int(time.time()))
            
        print(f"🔧 Nombre de tabla sanitizado: '{table_name}' → '{sanitized}'")
        return sanitized

    def escape_identifier(self, identifier: str) -> str:
        """Escape robusto para identificadores SQL"""
        if not identifier:
            return '""'
        
        # Convertir a string y limpiar
        identifier = str(identifier).strip()
        
        if not identifier:
            return '""'
        
        # CASOS ESPECIALES
        if identifier.lower() in ("*", "all"):
            return identifier  # No escapar wildcards
        
        # ESCAPE SEGURO: Siempre usar comillas dobles
        # Esto previene todos los problemas con caracteres especiales
        escaped = identifier.replace('"', '""')  # Escapar comillas internas
        return f'"{escaped}"'

    def escape_sql_value(self, value: Any) -> str:
        """Escapa valores para uso seguro en SQL"""
        if value is None:
            return 'NULL'
        
        # Convertir a string y escapar comillas simples
        str_value = str(value).replace("'", "''")
        return f"'{str_value}'"

    def _build_equals_condition(self, escaped_column: str, value: Any, **kwargs) -> str:
        """Construye condición de igualdad"""
        return f"{escaped_column} = {self.escape_sql_value(value)}"


    def _build_not_equals_condition(self, escaped_column: str, value: Any, **kwargs) -> str:
        """Construye condición de desigualdad"""
        return f"{escaped_column} != {self.escape_sql_value(value)}"


    def _build_contains_condition(self, escaped_column: str, value: Any, **kwargs) -> str:
        """Construye condición LIKE para búsqueda de texto"""
        escaped_value = str(value).replace("'", "''")
        return f"LOWER(CAST({escaped_column} AS VARCHAR)) LIKE LOWER('%{escaped_value}%')"


    def _build_not_contains_condition(self, escaped_column: str, value: Any, **kwargs) -> str:
        """Construye condición NOT LIKE para búsqueda de texto"""
        escaped_value = str(value).replace("'", "''")
        return f"LOWER(CAST({escaped_column} AS VARCHAR)) NOT LIKE LOWER('%{escaped_value}%')"


    def _build_starts_with_condition(self, escaped_column: str, value: Any, **kwargs) -> str:
        """Construye condición para texto que comienza con"""
        escaped_value = str(value).replace("'", "''")
        return f"LOWER(CAST({escaped_column} AS VARCHAR)) LIKE LOWER('{escaped_value}%')"


    def _build_ends_with_condition(self, escaped_column: str, value: Any, **kwargs) -> str:
        """Construye condición para texto que termina con"""
        escaped_value = str(value).replace("'", "''")
        return f"LOWER(CAST({escaped_column} AS VARCHAR)) LIKE LOWER('%{escaped_value}')"


    def _build_in_condition(self, escaped_column: str, values: list, **kwargs) -> str:
        """Construye condición IN"""
        if not values:
            return ""
        values_str = ", ".join(self.escape_sql_value(v) for v in values)
        return f"{escaped_column} IN ({values_str})"


    def _build_not_in_condition(self, escaped_column: str, values: list, **kwargs) -> str:
        """Construye condición NOT IN"""
        if not values:
            return ""
        values_str = ", ".join(self.escape_sql_value(v) for v in values)
        return f"{escaped_column} NOT IN ({values_str})"


    def _build_is_null_condition(self, escaped_column: str, **kwargs) -> str:
        """Construye condición IS NULL"""
        return f"({escaped_column} IS NULL OR {escaped_column} = '')"


    def _build_is_not_null_condition(self, escaped_column: str, **kwargs) -> str:
        """Construye condición IS NOT NULL"""
        return f"({escaped_column} IS NOT NULL AND {escaped_column} != '')"


    def _build_greater_than_condition(self, escaped_column: str, value: Any, **kwargs) -> str:
        """Construye condición mayor que"""
        numeric_value = float(value) if self._is_numeric(value) else 0
        return f"CAST({escaped_column} AS DOUBLE) > {numeric_value}"


    def _build_greater_equal_condition(self, escaped_column: str, value: Any, **kwargs) -> str:
        """Construye condición mayor o igual"""
        numeric_value = float(value) if self._is_numeric(value) else 0
        return f"CAST({escaped_column} AS DOUBLE) >= {numeric_value}"


    def _build_less_than_condition(self, escaped_column: str, value: Any, **kwargs) -> str:
        """Construye condición menor que"""
        numeric_value = float(value) if self._is_numeric(value) else 0
        return f"CAST({escaped_column} AS DOUBLE) < {numeric_value}"


    def _build_less_equal_condition(self, escaped_column: str, value: Any, **kwargs) -> str:
        """Construye condición menor o igual"""
        numeric_value = float(value) if self._is_numeric(value) else 0
        return f"CAST({escaped_column} AS DOUBLE) <= {numeric_value}"


    def _build_between_condition(self, escaped_column: str, values: list, **kwargs) -> str:
        """Construye condición BETWEEN"""
        if not isinstance(values, list) or len(values) != 2:
            return ""
        
        val1, val2 = values
        if not self._is_numeric(val1) or not self._is_numeric(val2):
            return ""
        
        return f"CAST({escaped_column} AS DOUBLE) BETWEEN {float(val1)} AND {float(val2)}"


    def _build_regex_condition(self, escaped_column: str, value: Any, **kwargs) -> str:
        """Construye condición de expresión regular"""
        if not value:
            return ""
        escaped_value = str(value).replace("'", "''")
        return f"regexp_matches({escaped_column}, '{escaped_value}')"


    def _get_condition_builders(self) -> dict:
        """Retorna diccionario de funciones builder por operador"""
        return {
            'equals': self._build_equals_condition,
            'not_equals': self._build_not_equals_condition,
            'contains': self._build_contains_condition,
            'not_contains': self._build_not_contains_condition,
            'starts_with': self._build_starts_with_condition,
            'ends_with': self._build_ends_with_condition,
            'in': self._build_in_condition,
            'not_in': self._build_not_in_condition,
            'is_null': self._build_is_null_condition,
            'is_not_null': self._build_is_not_null_condition,
            'greater_than': self._build_greater_than_condition,
            'greater_equal': self._build_greater_equal_condition,
            'less_than': self._build_less_than_condition,
            'less_equal': self._build_less_equal_condition,
            'between': self._build_between_condition,
            'regex': self._build_regex_condition
        }


    def build_filter_conditions(self, filters: List[Dict[str, Any]]) -> List[str]:
        """Construye condiciones WHERE desde filtros usando Strategy Pattern"""
        conditions = []
        condition_builders = self._get_condition_builders()
        
        for filter_item in filters:
            column = filter_item.get('column')
            operator = filter_item.get('operator')
            value = filter_item.get('value')
            values = filter_item.get('values', [])
            
            if not column or not operator:
                continue
            
            # Obtener builder para el operador
            builder = condition_builders.get(operator)
            if not builder:
                continue
            
            # Construir condición usando el builder correspondiente
            escaped_column = self.escape_identifier(column)
            
            try:
                condition = builder(
                    escaped_column=escaped_column,
                    value=value,
                    values=values
                )
                
                if condition:
                    conditions.append(condition)
            except Exception as e:
                print(f"⚠️ Error construyendo condición para operador '{operator}': {e}")
                continue
        
        return conditions


    def _is_numeric(self, value: Any) -> bool:
        """Verifica si un valor es numérico"""
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False

    def build_search_conditions(self, search_term: str, text_columns: List[str]) -> Optional[str]:
        """Construye condiciones de búsqueda global"""
        if not search_term or not search_term.strip() or not text_columns:
            return None
        
        search_escaped = search_term.strip().replace("'", "''")
        search_conditions = []
        
        for col in text_columns:
            escaped_col = self.escape_identifier(col)
            condition = f"LOWER(CAST({escaped_col} AS VARCHAR)) LIKE LOWER('%{search_escaped}%')"
            search_conditions.append(condition)
        
        if search_conditions:
            return f"({' OR '.join(search_conditions)})"
        
        return None

    def build_order_clause(self, sort_by: Optional[str], sort_order: str = "ASC") -> str:
        """Construye cláusula ORDER BY"""
        if not sort_by:
            return ""
        
        escaped_column = self.escape_identifier(sort_by)
        order = sort_order.upper()
        
        if order not in ["ASC", "DESC"]:
            order = "ASC"
        
        return f"ORDER BY {escaped_column} {order}"

    def build_pagination_clause(self, page: int, page_size: int) -> str:
        """Construye cláusula de paginación"""
        if page < 1:
            page = 1
        if page_size < 1:
            page_size = 1000
        
        offset = (page - 1) * page_size
        return f"LIMIT {page_size} OFFSET {offset}"

    def build_select_clause(self, columns: Optional[List[str]]) -> str:
        """Construye cláusula SELECT"""
        if not columns:
            return "*"
        
        escaped_columns = [self.escape_identifier(col) for col in columns if col]
        
        if not escaped_columns:
            return "*"
        
        return ", ".join(escaped_columns)

    def validate_sql_identifier(self, identifier: str) -> bool:
        """Valida que un identificador SQL sea seguro"""
        if not identifier or not isinstance(identifier, str):
            return False
        
        # Verificar longitud razonable
        if len(identifier) > 200:
            return False
        
        # Verificar que no contenga caracteres peligrosos
        dangerous_chars = [';', '--', '/*', '*/', 'xp_', 'sp_', 'DROP', 'DELETE', 'UPDATE', 'INSERT']
        identifier_upper = identifier.upper()
        
        for dangerous in dangerous_chars:
            if dangerous in identifier_upper:
                return False
        
        return True

    def sanitize_column_name(self, column_name: str) -> str:
        """Sanitiza un nombre de columna"""
        if not column_name:
            return "unnamed_column"
        
        # Remover caracteres problemáticos
        sanitized = re.sub(r'[^\w\s-.]', '_', str(column_name))
        
        # Reemplazar espacios con guiones bajos
        sanitized = re.sub(r'\s+', '_', sanitized)
        
        # Remover guiones bajos múltiples
        sanitized = re.sub(r'_+', '_', sanitized)
        
        # Remover guiones bajos al inicio y final
        sanitized = sanitized.strip('_')
        
        if not sanitized:
            sanitized = "unnamed_column"
        
        return sanitized

    def build_join_condition(self, left_column: str, right_column: str, join_type: str = "INNER") -> str:
        """Construye condición de JOIN"""
        left_escaped = self.escape_identifier(left_column)
        right_escaped = self.escape_identifier(right_column)
        
        join_type = join_type.upper()
        if join_type not in ["INNER", "LEFT", "RIGHT", "FULL"]:
            join_type = "INNER"
        
        return f"{join_type} JOIN ON {left_escaped} = {right_escaped}"

    def _estimate_execution_time(self, complexity_score: int) -> str:
        """Estima el tiempo de ejecución basado en el score de complejidad"""
        if complexity_score <= 2:
            return "fast"
        elif complexity_score <= 4:
            return "moderate"
        else:
            return "slow"


    def estimate_query_complexity(self, sql: str) -> Dict[str, Any]:
        """Estima la complejidad de una consulta SQL"""
        sql_upper = sql.upper()
        
        complexity_score = 0
        features = {
            "has_joins": "JOIN" in sql_upper,
            "has_subqueries": "SELECT" in sql_upper[sql_upper.find("SELECT") + 6:],
            "has_aggregates": any(agg in sql_upper for agg in ["COUNT", "SUM", "AVG", "MAX", "MIN"]),
            "has_window_functions": "OVER" in sql_upper,
            "has_group_by": "GROUP BY" in sql_upper,
            "has_order_by": "ORDER BY" in sql_upper,
            "has_having": "HAVING" in sql_upper
        }
        
        # Calcular score
        for feature, present in features.items():
            if present:
                complexity_score += 1
        
        # Determinar nivel de complejidad
        if complexity_score <= 1:
            complexity_level = "simple"
        elif complexity_score <= 3:
            complexity_level = "moderate"
        else:
            complexity_level = "complex"
        
        return {
            "complexity_score": complexity_score,
            "complexity_level": complexity_level,
            "features": features,
            "estimated_execution_time": self._estimate_execution_time(complexity_score)  #  Extraído
        }


    def format_sql_for_logging(self, sql: str, max_length: int = 500) -> str:
        """Formatea SQL para logging (trunca si es muy largo)"""
        if not sql:
            return "EMPTY SQL"
        
        # Limpiar espacios extra
        formatted = re.sub(r'\s+', ' ', sql.strip())
        
        # Truncar si es muy largo
        if len(formatted) > max_length:
            formatted = formatted[:max_length] + "... [TRUNCATED]"
        
        return formatted
