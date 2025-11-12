import time
from datetime import datetime
from typing import Dict, Any, List, Optional
from utils.sql_utils import SQLUtils

class QueryController:
    """Controlador para consultas y operaciones de base de datos"""
    
    def __init__(self, conn, loaded_tables: Dict):
        self.conn = conn
        self.loaded_tables = loaded_tables
        self.sql_utils = SQLUtils()

    def load_parquet_lazy(self, file_id: str, parquet_path: str, table_name: Optional[str], loaded_tables: Dict) -> str:
        """Carga lazy - solo registra el path (instantáneo)"""
        
        if not table_name:
            safe_file_id = self.sql_utils.sanitize_table_name("table_" + file_id)
            table_name = safe_file_id
        else:
            table_name = self.sql_utils.sanitize_table_name(table_name)
        
        # Solo registrar la información, no cargar datos
        loaded_tables[file_id] = {
            "table_name": table_name,
            "parquet_path": parquet_path,
            "loaded_at": datetime.now().isoformat(),
            "load_time": 0.001,
            "type": "lazy"
        }        
        return table_name

    def load_parquet_to_view(self, file_id: str, parquet_path: str, table_name: Optional[str], loaded_tables: Dict) -> str:
        """Crear vista en lugar de tabla (ultra-rápido)"""
        
        if not table_name:
            safe_file_id = self.sql_utils.sanitize_table_name("table_" + file_id)
            table_name = safe_file_id
        else:
            table_name = self.sql_utils.sanitize_table_name(table_name)
        start_time = time.time()
        
        try:
            # Crear vista en lugar de tabla (ultra-rápido)
            create_sql = f"""
            CREATE OR REPLACE VIEW {table_name} AS 
            SELECT * FROM read_parquet('{parquet_path}')
            """
            
            self.conn.execute(create_sql)
            
            # Registrar vista cargada
            loaded_tables[file_id] = {
                "table_name": table_name,
                "parquet_path": parquet_path,
                "loaded_at": datetime.now().isoformat(),
                "load_time": time.time() - start_time,
                "type": "view"  # Indicar que es vista, no tabla
            }
            
            time.time() - start_time
            
            return table_name
            
        except Exception as e:
            raise ValueError(f"Error al crear vista: {str(e)}")

    def query_data_ultra_fast(
        self, 
        file_id: str, 
        filters: Optional[List[Dict[str, Any]]] = None,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "ASC",
        page: int = 1,
        page_size: int = 1000,
        selected_columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Consulta directa al Parquet o vista según el tipo"""
        
        if file_id not in self.loaded_tables:
            raise ValueError("Archivo no cargado en DuckDB")
        
        table_info = self.loaded_tables[file_id]
        table_type = table_info.get("type", "table")
        
        # Si es lazy, usar consulta directa al Parquet
        if table_type == "lazy":
            return self._query_parquet_direct(
                file_id, filters, search, sort_by, sort_order, page, page_size, selected_columns
            )
        else:
            # Si es vista o tabla, usar consulta normal
            return self._query_table_or_view(
                file_id, filters, search, sort_by, sort_order, page, page_size, selected_columns
            )

    def _build_select_clause(self, selected_columns: Optional[List[str]]) -> str:
        """Construye la cláusula SELECT"""
        if selected_columns:
            return ", ".join(self.sql_utils.escape_identifier(col) for col in selected_columns)
        return "*"


    def _build_search_condition(self, search: str, parquet_path: str) -> Optional[str]:
        """Construye la condición de búsqueda global"""
        if not search or not search.strip():
            return None
        
        # Obtener columnas de texto
        columns_sql = f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')"
        all_columns = self.conn.execute(columns_sql).fetchall()
        text_columns = [col[0] for col in all_columns if col[1] in ['VARCHAR', 'TEXT']]
        
        if not text_columns:
            return None
        
        # Construir condiciones de búsqueda
        search_escaped = search.strip().replace("'", "''")
        search_conditions = [
            f"LOWER(CAST({self.sql_utils.escape_identifier(col)} AS VARCHAR)) LIKE LOWER('%{search_escaped}%')"
            for col in text_columns
        ]
        
        return f"({' OR '.join(search_conditions)})" if search_conditions else None


    def _build_where_clause(self, search_condition: Optional[str], filters: Optional[List[Dict]]) -> str:
        """Construye la cláusula WHERE completa"""
        where_conditions = []
        
        if search_condition:
            where_conditions.append(search_condition)
        
        if filters:
            where_conditions.extend(self.sql_utils.build_filter_conditions(filters))
        
        return f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""


    def _build_order_clause(self, sort_by: Optional[str], sort_order: str) -> str:
        """Construye la cláusula ORDER BY"""
        if not sort_by:
            return ""
        
        escaped_sort_column = self.sql_utils.escape_identifier(sort_by)
        return f"ORDER BY {escaped_sort_column} {sort_order.upper()}"


    def _build_limit_clause(self, page: int, page_size: int) -> str:
        """Construye la cláusula LIMIT/OFFSET"""
        offset = (page - 1) * page_size
        return f"LIMIT {page_size} OFFSET {offset}"


    def _query_parquet_direct(
        self,
        file_id: str,
        filters: Optional[List[Dict[str, Any]]] = None,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "ASC",
        page: int = 1,
        page_size: int = 1000,
        selected_columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Consulta directa al Parquet sin tabla intermedia"""
        
        # Guard clause: validar archivo
        if file_id not in self.loaded_tables:
            raise ValueError("Archivo no registrado")
        
        parquet_path = self.loaded_tables[file_id]["parquet_path"]
        start_time = time.time()
        
        # Construir componentes del query
        select_clause = self._build_select_clause(selected_columns)
        search_condition = self._build_search_condition(search, parquet_path)
        where_clause = self._build_where_clause(search_condition, filters)
        order_clause = self._build_order_clause(sort_by, sort_order)
        limit_clause = self._build_limit_clause(page, page_size)
        
        # Construir queries completos
        data_sql = f"""
        SELECT {select_clause}
        FROM read_parquet('{parquet_path}')
        {where_clause}
        {order_clause}
        {limit_clause}
        """
        
        count_sql = f"""
        SELECT COUNT(*) as total
        FROM read_parquet('{parquet_path}')
        {where_clause}
        """
        
        # Ejecutar consultas
        data_result = self.conn.execute(data_sql).fetchdf()
        total_result = self.conn.execute(count_sql).fetchone()[0]
        
        query_time = time.time() - start_time
        data_records = data_result.to_dict(orient='records')
        total_pages = (total_result + page_size - 1) // page_size if total_result > 0 else 1
        
        return {
            "data": data_records,
            "pagination": {
                "current_page": page,
                "page_size": page_size,
                "total_rows": total_result,
                "total_pages": total_pages,
                "rows_in_page": len(data_records),
                "has_next": page < total_pages,
                "has_prev": page > 1
            },
            "query_time": query_time,
            "ultra_fast": True,
            "engine": "DuckDB Direct Parquet",
            "method": "direct_parquet_query"
        }


    def _execute_query_with_pagination(
        self,
        source: str,  # Puede ser table_name o read_parquet(path)
        select_clause: str,
        where_clause: str,
        order_clause: str,
        limit_clause: str,
        engine: str,
        start_time: float,
        page: int,
        page_size: int
    ) -> Dict[str, Any]:
        """Método base para ejecutar queries con paginación"""
        
        data_sql = f"""
        SELECT {select_clause}
        FROM {source}
        {where_clause}
        {order_clause}
        {limit_clause}
        """
        
        count_sql = f"""
        SELECT COUNT(*) as total
        FROM {source}
        {where_clause}
        """
        
        # Ejecutar consultas
        data_result = self.conn.execute(data_sql).fetchdf()
        total_result = self.conn.execute(count_sql).fetchone()[0]
        
        query_time = time.time() - start_time
        data_records = data_result.to_dict(orient='records')
        total_pages = (total_result + page_size - 1) // page_size if total_result > 0 else 1
        
        return {
            "data": data_records,
            "pagination": {
                "current_page": page,
                "page_size": page_size,
                "total_rows": total_result,
                "total_pages": total_pages,
                "rows_in_page": len(data_records),
                "has_next": page < total_pages,
                "has_prev": page > 1
            },
            "query_time": query_time,
            "ultra_fast": True,
            "engine": engine
        }


    def _query_table_or_view(
        self,
        file_id: str,
        filters: Optional[List[Dict[str, Any]]] = None,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "ASC",
        page: int = 1,
        page_size: int = 1000,
        selected_columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Consulta normal a tabla o vista DuckDB"""
        
        table_name = self.loaded_tables[file_id]["table_name"]
        start_time = time.time()
        
        # Construir componentes del query
        select_clause = self._build_select_clause(selected_columns)
        search_condition = self._build_search_condition_from_table(search, table_name)
        where_clause = self._build_where_clause(search_condition, filters)
        order_clause = self._build_order_clause(sort_by, sort_order)
        limit_clause = self._build_limit_clause(page, page_size)
        
        # Ejecutar con método base
        return self._execute_query_with_pagination(
            source=table_name,
            select_clause=select_clause,
            where_clause=where_clause,
            order_clause=order_clause,
            limit_clause=limit_clause,
            engine="DuckDB Table/View",
            start_time=start_time,
            page=page,
            page_size=page_size
        )


    def get_unique_values_ultra_fast(self, file_id: str, column_name: str, limit: int = 1000) -> List[str]:
        """Valores únicos con consulta directa si es lazy"""
        
        if file_id not in self.loaded_tables:
            raise ValueError("Archivo no cargado en DuckDB")
        
        table_info = self.loaded_tables[file_id]
        table_type = table_info.get("type", "table")
        start_time = time.time()
        
        escaped_column = self.sql_utils.escape_identifier(column_name)
        
        # Determinar si usar tabla/vista o Parquet directo
        if table_type == "lazy":
            parquet_path = table_info["parquet_path"]
            unique_sql = f"""
            SELECT DISTINCT {escaped_column} as value
            FROM read_parquet('{parquet_path}')
            WHERE {escaped_column} IS NOT NULL 
            AND CAST({escaped_column} AS VARCHAR) != ''
            ORDER BY value
            LIMIT {limit}
            """
        else:
            table_name = table_info["table_name"]
            unique_sql = f"""
            SELECT DISTINCT {escaped_column} as value
            FROM {table_name}
            WHERE {escaped_column} IS NOT NULL 
            AND CAST({escaped_column} AS VARCHAR) != ''
            ORDER BY value
            LIMIT {limit}
            """
        
        try:
            result = self.conn.execute(unique_sql).fetchall()
            unique_values = [str(row[0]) for row in result if row[0] is not None]
            
            query_time = time.time() - start_time
            print(f"⚡ {len(unique_values)} valores únicos en {query_time:.3f}s")
            
            return unique_values
            
        except Exception:
            return []

    def get_file_stats(self, file_id: str) -> Dict[str, Any]:
        """Obtiene estadísticas ultra-rápidas del archivo"""
        
        if file_id not in self.loaded_tables:
            return {"loaded": False}
        
        table_info = self.loaded_tables[file_id]
        table_type = table_info.get("type", "table")
        
        # Si es lazy, consultar directamente al Parquet
        if table_type == "lazy":
            parquet_path = table_info["parquet_path"]
            stats_sql = f"SELECT COUNT(*) as total_rows FROM read_parquet('{parquet_path}')"
        else:
            table_name = table_info["table_name"]
            stats_sql = f"SELECT COUNT(*) as total_rows FROM {table_name}"
        
        result = self.conn.execute(stats_sql).fetchone()
        
        return {
            "loaded": True,
            "table_name": table_info["table_name"],
            "table_type": table_type,
            "total_rows": result[0],
            "parquet_path": table_info["parquet_path"],
            "loaded_at": table_info["loaded_at"],
            "load_time": table_info["load_time"],
            "ultra_fast": True
        }
