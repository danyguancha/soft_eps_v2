
import os
from typing import Any, Dict, List, Optional

from services.aux_duckdb_services.condition_search import ConditionSearch
from .sql_codition_filter import SqlConditionFilter

class QueryPagination:
    """Clase para manejar la paginación de consultas en DuckDB"""

    def query_data_ultra_fast(
        self,
        conn,
        file_id: str,
        filters: Optional[List[Dict[str, Any]]] = None,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "ASC",
        page: int = 1,
        page_size: int = 1000,
        selected_columns: Optional[List[str]] = None,
        loaded_tables: Dict[str, Any] = {},
    ) -> Dict[str, Any]:
        """Query ultra-rápida CON PAGINACIÓN COMPLETA """
        
        try:
            # VERIFICAR Y REGENERAR PARQUET SI ES NECESARIO
            if file_id not in loaded_tables:
                print(f"Archivo {file_id} no cargado, cargando bajo demanda...")
                if not self._load_file_on_demand_with_regeneration(file_id, loaded_tables):
                    raise Exception(f"No se pudo cargar archivo {file_id}")
            else:
                # Verificar que el Parquet exista físicamente
                if not self.ensure_parquet_exists_or_regenerate(file_id, loaded_tables):
                    raise Exception(f"No se pudo asegurar la existencia del Parquet para {file_id}")
            
            # Obtener referencia a la tabla
            table_info = loaded_tables[file_id]
            
            if table_info.get("type") == "lazy":
                table_ref = f"read_parquet('{table_info['parquet_path']}')"
            else:
                table_ref = table_info["table_name"]
            
            # CONSTRUIR COLUMNAS PARA SELECT
            if selected_columns:
                columns_clause = ", ".join([self._escape_identifier(col) for col in selected_columns])
            else:
                columns_clause = "*"
            
            # CONSTRUIR CONDICIONES WHERE (COMPARTIDAS ENTRE COUNT Y SELECT)
            where_conditions = []
            
            # Aplicar filtros
            if filters:
                for filter_item in filters:
                    condition = SqlConditionFilter().build_filter_condition(filter_item)
                    if condition:
                        where_conditions.append(condition)
            
            # Aplicar búsqueda
            if search:
                search_condition = ConditionSearch().build_search_condition(conn, search, table_info)
                if search_condition:
                    where_conditions.append(search_condition)
            
            # Construir cláusula WHERE
            where_clause = ""
            if where_conditions:
                where_clause = f" WHERE {' AND '.join(where_conditions)}"
            
            # PASO 1: OBTENER TOTAL DE REGISTROS (CON FILTROS APLICADOS)
            count_query = f"SELECT COUNT(*) FROM {table_ref}{where_clause}"
            
            count_result = conn.execute(count_query).fetchone()
            total_records = count_result[0] if count_result else 0
            
            print(f"Total de registros (con filtros): {total_records:,}")
            
            # PASO 2: CALCULAR INFORMACIÓN DE PAGINACIÓN
            import math
            total_pages = math.ceil(total_records / page_size) if total_records > 0 else 1
            has_next = page < total_pages
            has_prev = page > 1
            
            pagination_info = {
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "total_rows": total_records,  # TOTAL REAL, NO SOLO DE LA PÁGINA
                "has_next": has_next,
                "has_prev": has_prev,
                "next_page": page + 1 if has_next else None,
                "prev_page": page - 1 if has_prev else None
            }
            
            # PASO 3: CONSTRUIR QUERY PAGINADA
            base_query = f"SELECT {columns_clause} FROM {table_ref}{where_clause}"
            
            # Aplicar ordenamiento
            if sort_by:
                escaped_sort = self._escape_identifier(sort_by)
                base_query += f" ORDER BY {escaped_sort} {sort_order}"
            
            # Aplicar paginación
            offset = (page - 1) * page_size
            paginated_query = f"{base_query} LIMIT {page_size} OFFSET {offset}"
            
            print(f"Ejecutando query paginada: página {page} de {total_pages}")
            
            # PASO 4: EJECUTAR QUERY PAGINADA
            result = conn.execute(paginated_query).fetchall()
            
            # PASO 5: OBTENER COLUMNAS
            if result:
                # Usar la misma query sin LIMIT para obtener columnas
                columns_query = f"{base_query} LIMIT 0"
                columns_result = conn.execute(columns_query).description
                columns = [col[0] for col in columns_result]
            else:
                # Si no hay datos, obtener columnas de la tabla directamente
                describe_query = f"DESCRIBE {table_ref}"
                describe_result = conn.execute(describe_query).fetchall()
                columns = [row[0] for row in describe_result]
            
            # PASO 6: CONVERTIR RESULTADO A DICCIONARIOS
            data = []
            for row in result:
                row_dict = {}
                for i, col_name in enumerate(columns):
                    if i < len(row):  # Protección contra índices fuera de rango
                        row_dict[col_name] = row[i]
                    else:
                        row_dict[col_name] = None
                data.append(row_dict)
            
            print(f"Query completada: {len(data)} registros de página {page}")
            
            # PASO 7: RETORNO COMPLETO CON PAGINATION
            return {
                "success": True,
                "data": data,
                "columns": columns,
                
                # INFORMACIÓN DE PAGINACIÓN COMPLETA (evita KeyError)
                "pagination": pagination_info,
                
                # Campos adicionales para compatibilidad
                "total_rows": total_records,  # Total real con filtros
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "has_next": has_next,
                "has_prev": has_prev,
                
                # Metadata
                "method": "ultra_fast_with_complete_pagination",
                "filters_applied": len(filters) if filters else 0,
                "search_applied": bool(search),
                "sort_applied": bool(sort_by)
            }
            
        except Exception as e:
            error_msg = str(e)
            
            # DETECTAR ERROR DE ARCHIVO FALTANTE ESPECÍFICAMENTE
            if "No files found that match the pattern" in error_msg:
                print(f"Detectado error de Parquet faltante, intentando regeneración...")
                
                try:
                    # Forzar regeneración
                    if self.ensure_parquet_exists_or_regenerate(file_id, loaded_tables):
                        print(f"Parquet regenerado, reintentando consulta...")
                        # Reintentar consulta una vez (evitar recursión infinita)
                        return self.query_data_ultra_fast(
                            file_id, filters, search, sort_by, sort_order, page, page_size, selected_columns
                        )
                    else:
                        raise Exception(f"No se pudo regenerar Parquet para {file_id}")
                except Exception as regen_error:
                    raise Exception(f"Error regenerando Parquet: {regen_error}")
            
            print(f"Error en query_data_ultra_fast: {e}")
            
            # RETORNO DE ERROR CON PAGINATION VACÍA (evita KeyError)
            return {
                "success": False,
                "error": error_msg,
                "data": [],
                "columns": [],
                
                # PAGINATION VACÍA PERO PRESENTE (evita KeyError)
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_pages": 0,
                    "total_rows": 0,
                    "has_next": False,
                    "has_prev": False,
                    "next_page": None,
                    "prev_page": None
                },
                
                # Campos adicionales
                "total_rows": 0,
                "page": page,
                "page_size": page_size,
                "total_pages": 0,
                "has_next": False,
                "has_prev": False,
                "method": "error_with_pagination"
            }

    def _load_file_on_demand_with_regeneration(self, table_key: str, loaded_tables: Dict[str, Any]) -> bool:
        """Carga archivo con regeneración automática si es necesario"""
        try:
            # SI ESTÁ EN CACHE, VERIFICAR EXISTENCIA FÍSICA
            if table_key in loaded_tables:
                if self.ensure_parquet_exists_or_regenerate(table_key, loaded_tables):
                    return True
                # Si no existe físicamente, continuar con regeneración
            
            print(f"Carga bajo demanda no disponible para {table_key}")
            print(f"Usa read_technical_file_data_paginated() para forzar conversión")
            return False
            
        except Exception as e:
            print(f"Error en carga bajo demanda: {e}")
            return False

    def ensure_parquet_exists_or_regenerate(self, table_key: str, loaded_tables: Dict[str, Any]) -> bool:
        """Verifica que el Parquet existe físicamente, si no lo regenera"""
        try:
            if table_key not in loaded_tables:
                print(f"Tabla {table_key} no está en cache")
                return False
            
            table_info = loaded_tables[table_key]
            parquet_path = table_info.get('parquet_path')
            
            if not parquet_path:
                print(f"No hay ruta de Parquet para {table_key}")
                return False
            
            # VERIFICACIÓN FÍSICA DEL ARCHIVO
            if not os.path.exists(parquet_path):
                print(f"Archivo Parquet no existe físicamente: {parquet_path}")
                print(f"Intentando regenerar desde fuente original...")
                
                # Remover de cache para forzar regeneración
                del loaded_tables[table_key]
                return False
            
            # VERIFICAR QUE EL ARCHIVO NO ESTÉ VACÍO O CORRUPTO
            try:
                file_size = os.path.getsize(parquet_path)
                if file_size == 0:
                    print(f"Archivo Parquet está vacío: {parquet_path}")
                    os.remove(parquet_path)
                    del loaded_tables[table_key]
                    return False
                
                print(f"Parquet verificado: {parquet_path} ({file_size:,} bytes)")
                return True
                
            except Exception as file_error:
                print(f"Error verificando archivo Parquet: {file_error}")
                return False
            
        except Exception as e:
            print(f"Error en ensure_parquet_exists_or_regenerate: {e}")
            return False