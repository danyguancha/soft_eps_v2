
from typing import Any, Dict, Optional


class ConditionSearch:
    
    def build_search_condition(self, conn, search_term: str, table_info: Dict[str, Any]) -> Optional[str]:
        """
        Construye condición de búsqueda que aplica a todas las columnas de texto
        """
        try:
            if not search_term or not search_term.strip():
                return None
            
            # Limpiar término de búsqueda
            clean_search = search_term.strip().replace("'", "''")
            
            # Obtener columnas disponibles
            columns = []
            if table_info.get("type") == "lazy":
                # Para lazy load, obtener columnas del parquet
                parquet_path = table_info.get("parquet_path")
                if parquet_path:
                    try:
                        describe_sql = f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}') LIMIT 0"
                        describe_result = conn.execute(describe_sql).fetchall()
                        columns = [row[0] for row in describe_result]
                    except:
                        columns = []
            else:
                # Para tablas normales
                table_name = table_info.get("table_name")
                if table_name:
                    try:
                        describe_sql = f"DESCRIBE {table_name}"
                        describe_result = conn.execute(describe_sql).fetchall()
                        columns = [row[0] for row in describe_result]
                    except:
                        columns = []
            
            if not columns:
                print("No se pudieron obtener columnas para búsqueda")
                return None
            
            # CONSTRUIR BÚSQUEDA EN TODAS LAS COLUMNAS (STRING)
            search_conditions = []
            for column in columns:
                escaped_column = self._escape_identifier(column)
                # Convertir a string y buscar (DuckDB automáticamente maneja conversiones)
                condition = f"CAST({escaped_column} AS VARCHAR) LIKE '%{clean_search}%'"
                search_conditions.append(condition)
            
            if search_conditions:
                # Unir con OR - la búsqueda encuentra en cualquier columna
                return f"({' OR '.join(search_conditions)})"
            else:
                return None
                
        except Exception as e:
            print(f"Error construyendo búsqueda: {e}")
            return None