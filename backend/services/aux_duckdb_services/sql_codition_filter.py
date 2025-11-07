

from typing import Any, Dict, Optional


class SqlConditionFilter:
    """Clase para manejar filtros SQL en DuckDB"""
    def build_filter_condition(self, filter_item: Dict[str, Any]) -> Optional[str]:
        """
        Construye una condición WHERE SQL basada en un objeto de filtro
        
        filter_item formato esperado:
        {
            'column': 'Regional', 
            'operator': 'in', 
            'values': ['REGIONAL BOGOTA']
        }
        """
        try:
            column = filter_item.get('column')
            operator = filter_item.get('operator', '=').lower()
            values = filter_item.get('values', filter_item.get('value'))
            
            if not column or values is None:
                print(f"⚠️ Filtro incompleto: {filter_item}")
                return None
            
            # Asegurar que values sea una lista
            if not isinstance(values, list):
                values = [values]
            
            # Escapar nombre de columna
            escaped_column = self._escape_identifier(column)
            
            # ✅ FUNCIÓN AUXILIAR PARA ESCAPAR VALORES
            def escape_value(val):
                return str(val).replace("'", "''")
            
            # ✅ CONSTRUCCIÓN DE CONDICIONES SQL (COMILLAS CORREGIDAS)
            if operator in ['=', 'eq', 'equals']:
                if len(values) == 0:
                    return None
                escaped_val = escape_value(values[0])
                return f"{escaped_column} = '{escaped_val}'"
                
            elif operator in ['!=', 'ne', 'not_equals']:
                if len(values) == 0:
                    return None
                escaped_val = escape_value(values[0])
                return f"{escaped_column} != '{escaped_val}'"
                
            elif operator in ['in', 'IN']:
                if len(values) == 0:
                    return None
                # Escapar cada valor y construir lista
                escaped_values = [f"'{escape_value(v)}'" for v in values if v is not None]
                if not escaped_values:
                    return None
                values_str = ', '.join(escaped_values)
                return f"{escaped_column} IN ({values_str})"
                
            elif operator in ['not_in', 'not in', 'NOT IN']:
                if len(values) == 0:
                    return None
                escaped_values = [f"'{escape_value(v)}'" for v in values if v is not None]
                if not escaped_values:
                    return None
                values_str = ', '.join(escaped_values)
                return f"{escaped_column} NOT IN ({values_str})"
                
            elif operator in ['contains', 'like']:
                if len(values) == 0:
                    return None
                search_term = escape_value(values[0])
                return f"{escaped_column} LIKE '%{search_term}%'"
                
            elif operator in ['starts_with', 'startswith']:
                if len(values) == 0:
                    return None
                search_term = escape_value(values[0])
                return f"{escaped_column} LIKE '{search_term}%'"
                
            elif operator in ['ends_with', 'endswith']:
                if len(values) == 0:
                    return None
                search_term = escape_value(values[0])
                return f"{escaped_column} LIKE '%{search_term}'"
                
            elif operator in ['>', 'gt', 'greater_than']:
                if len(values) == 0:
                    return None
                escaped_val = escape_value(values[0])
                return f"{escaped_column} > '{escaped_val}'"
                
            elif operator in ['>=', 'gte', 'greater_than_or_equal']:
                if len(values) == 0:
                    return None
                escaped_val = escape_value(values[0])
                return f"{escaped_column} >= '{escaped_val}'"
                
            elif operator in ['<', 'lt', 'less_than']:
                if len(values) == 0:
                    return None
                escaped_val = escape_value(values[0])
                return f"{escaped_column} < '{escaped_val}'"
                
            elif operator in ['<=', 'lte', 'less_than_or_equal']:
                if len(values) == 0:
                    return None
                escaped_val = escape_value(values[0])
                return f"{escaped_column} <= '{escaped_val}'"
                
            elif operator in ['between']:
                if len(values) < 2:
                    return None
                val1 = escape_value(values[0])
                val2 = escape_value(values[1])
                return f"{escaped_column} BETWEEN '{val1}' AND '{val2}'"
                
            elif operator in ['is_null', 'null']:
                return f"{escaped_column} IS NULL"
                
            elif operator in ['is_not_null', 'not_null']:
                return f"{escaped_column} IS NOT NULL"
                
            else:
                print(f"⚠️ Operador no soportado: {operator}")
                return None
                
        except Exception as e:
            print(f"❌ Error construyendo filtro: {e}")
            print(f"   Filtro problemático: {filter_item}")
            return None
    
    def _escape_identifier(self, identifier: str) -> str:
        """Escapa un identificador SQL (nombre de columna)"""
        escaped = identifier.replace('"', '""')
        return f'"{escaped}"'