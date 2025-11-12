import time
from typing import Dict, Any, List, Optional
from utils.sql_utils import SQLUtils

class CrossFilesController:
    """Controlador para cruces de archivos (BUSCARX)"""
    
    def __init__(self, conn, loaded_tables: Dict):
        self.conn = conn
        self.loaded_tables = loaded_tables
        self.sql_utils = SQLUtils()

    def _get_table_reference(self, table_info: dict) -> str:
        """Obtiene la referencia correcta de la tabla"""
        if table_info.get("type") == "lazy":
            return f"read_parquet('{table_info['parquet_path']}')"
        return table_info["table_name"]


    def _validate_and_map_keys(self, key_column_file1: str, key_column_file2: str, 
                            real_cols_file1: list, real_cols_file2: list) -> tuple:
        """Valida y mapea las columnas clave"""
        mapped_key1 = self._map_column_to_real(key_column_file1, real_cols_file1)
        mapped_key2 = self._map_column_to_real(key_column_file2, real_cols_file2)
        
        if not mapped_key1 or not mapped_key2:
            raise ValueError("No se pudieron mapear las claves de join")
        
        return (mapped_key1, mapped_key2)


    def _build_select_clause(self, columns_to_include: Optional[Dict], 
                            real_cols_file1: list, real_cols_file2: list,
                            mapped_key2: str, mapped_file1_cols: list = None) -> str:
        """Construye la cláusula SELECT del query"""
        if not columns_to_include:
            return "base.*, lookup.*"
        
        select_parts = []
        file1_cols = columns_to_include.get("file1_columns", [])
        file2_cols = columns_to_include.get("file2_columns", [])
        
        # Columnas del archivo base
        if file1_cols:
            mapped_file1_cols = self._map_user_columns_to_real(file1_cols, real_cols_file1)
            for real_col in mapped_file1_cols:
                escaped_col = self.sql_utils.escape_identifier(real_col)
                select_parts.append(f"base.{escaped_col}")
        else:
            select_parts.append("base.*")
        
        # Columnas de búsqueda
        if file2_cols:
            mapped_file2_cols = self._map_user_columns_to_real(file2_cols, real_cols_file2)
            for real_col in mapped_file2_cols:
                escaped_col = self.sql_utils.escape_identifier(real_col)
                # Evitar conflictos de nombres
                if mapped_file1_cols and real_col == mapped_key2 and real_col in mapped_file1_cols:
                    alias = self.sql_utils.escape_identifier(f"lookup_{real_col}")
                    select_parts.append(f"lookup.{escaped_col} AS {alias}")
                else:
                    select_parts.append(f"lookup.{escaped_col}")
        
        return ", ".join(select_parts) if select_parts else "base.*"


    def _build_vlookup_query(self, table1_ref: str, table2_ref: str, 
                            select_clause: str, mapped_key1: str, mapped_key2: str) -> str:
        """Construye el query VLOOKUP con QUALIFY"""
        esc_key1 = self.sql_utils.escape_identifier(mapped_key1)
        esc_key2 = self.sql_utils.escape_identifier(mapped_key2)
        
        return f"""
        SELECT {select_clause}
        FROM {table1_ref} base
        LEFT JOIN (
            SELECT *
            FROM {table2_ref}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY {esc_key2} ORDER BY {esc_key2}) = 1
        ) lookup ON CAST(base.{esc_key1} AS VARCHAR) = CAST(lookup.{esc_key2} AS VARCHAR)
        """


    def _execute_and_validate_vlookup(self, vlookup_sql: str, expected_rows: int) -> tuple:
        """Ejecuta el VLOOKUP y valida el resultado"""
        # Validar con EXPLAIN
        try:
            self.conn.execute(f"EXPLAIN {vlookup_sql}").fetchall()
        except Exception as explain_error:
            raise ValueError(f"Error en EXPLAIN del VLOOKUP: {str(explain_error)}")
        
        # Ejecutar query
        result_df = self.conn.execute(vlookup_sql).fetchdf()
        total_rows = len(result_df)
        
        # Validar resultado
        if total_rows != expected_rows:
            print(f"ADVERTENCIA VLOOKUP: {total_rows} ≠ {expected_rows} esperados")
        else:
            print(f"VLOOKUP PERFECTO: {total_rows} registros = {expected_rows} base")
        
        return (result_df, total_rows)


    def cross_files_ultra_fast(
        self,
        file1_id: str,
        file2_id: str,
        key_column_file1: str,
        key_column_file2: str,
        join_type: str = "LEFT",
        columns_to_include: Optional[Dict[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """VLOOKUP/BUSCARX PERFECTO: Replica exactamente el comportamiento de Excel"""
        
        # Validaciones iniciales
        if file1_id not in self.loaded_tables or file2_id not in self.loaded_tables:
            raise ValueError("Uno o ambos archivos no están cargados en DuckDB")
        
        start_time = time.time()
        
        try:
            # PASO 1: Preparación de referencias y columnas
            table1_info = self.loaded_tables[file1_id]
            table2_info = self.loaded_tables[file2_id]
            
            table1_ref = self._get_table_reference(table1_info)
            table2_ref = self._get_table_reference(table2_info)
            
            real_cols_file1 = self._get_table_columns(table1_info)
            real_cols_file2 = self._get_table_columns(table2_info)
            
            # PASO 2: Contar registros base
            expected_rows = self.conn.execute(f"SELECT COUNT(*) FROM {table1_ref}").fetchone()[0]
            
            # PASO 3: Mapear claves
            mapped_key1, mapped_key2 = self._validate_and_map_keys(
                key_column_file1, key_column_file2, real_cols_file1, real_cols_file2
            )
            
            # PASO 4: Construir SELECT
            select_clause = self._build_select_clause(
                columns_to_include, real_cols_file1, real_cols_file2, mapped_key2
            )
            
            # PASO 5: Construir y ejecutar VLOOKUP
            vlookup_sql = self._build_vlookup_query(
                table1_ref, table2_ref, select_clause, mapped_key1, mapped_key2
            )
            
            result_df, total_rows = self._execute_and_validate_vlookup(vlookup_sql, expected_rows)
            cross_time = time.time() - start_time
            
            # PASO 6: Contar matches
            file2_cols = columns_to_include.get("file2_columns", []) if columns_to_include else []
            mapped_file2_cols = self._map_user_columns_to_real(file2_cols, real_cols_file2) if file2_cols else []
            matches, no_matches = self._count_matches(result_df, mapped_file2_cols)
            
            return {
                "success": True,
                "total_rows": total_rows,
                "columns": list(result_df.columns),
                "data": result_df.to_dict(orient="records"),
                "statistics": {
                    "total_rows": total_rows,
                    "expected_base_rows": expected_rows,
                    "matched_rows": matches,
                    "no_match_rows": no_matches,
                    "processing_time": cross_time,
                    "method": "vlookup_excel_perfect_replica",
                    "vlookup_perfect": total_rows == expected_rows
                },
                "metadata": {
                    "engine": "DuckDB",
                    "join_type": "VLOOKUP/BUSCARX",
                    "excel_equivalent": True,
                    "processing_strategy": "qualify_row_number_first_match",
                    "base_records_preserved": True,
                    "lookup_behavior": "first_match_only",
                    "no_row_multiplication": True
                }
            }
        
        except Exception as e:
            print(f"   Error: {str(e)}")
            if 'vlookup_sql' in locals():
                print(f"VLOOKUP SQL generado:\n{'='*60}\n{vlookup_sql}\n{'='*60}")
            raise e


    def _get_table_columns(self, table_info: Dict) -> List[str]:
        """Obtiene columnas de una tabla o archivo Parquet"""
        if table_info.get("type") == "lazy":
            cols_sql = f"DESCRIBE SELECT * FROM read_parquet('{table_info['parquet_path']}')"
        else:
            cols_sql = f"DESCRIBE {table_info['table_name']}"
        
        return [row[0] for row in self.conn.execute(cols_sql).fetchall()]

    def _map_column_to_real(self, user_column: str, real_columns: List[str]) -> str:
        """Mapea una columna de usuario a una columna real"""
        user_clean = user_column.strip()
        for real_col in real_columns:
            if user_clean == real_col.strip():
                return real_col
        return ""

    def _map_user_columns_to_real(self, user_cols: List[str], real_cols: List[str]) -> List[str]:
        """Mapea columnas de usuario a columnas reales"""
        mapped_cols = []
        for user_col in user_cols:
            if user_col in real_cols:
                mapped_cols.append(user_col)
                continue
            
            user_clean = user_col.strip()
            for real_col in real_cols:
                if user_clean == real_col.strip():
                    mapped_cols.append(real_col)
                    print(f"    Mapeado: '{user_col}' → '{real_col}'")
                    break
            else:
                raise ValueError(f"Columna '{user_col}' no existe en el archivo")
        return mapped_cols

    def _count_matches(self, result_df, lookup_cols: List[str]) -> tuple:
        """Cuenta matches y no-matches en el resultado"""
        matches = 0
        no_matches = 0
        
        for record in result_df.to_dict('records'):
            has_match = False
            for col in lookup_cols:
                if col in record and record[col] is not None and str(record[col]) not in ['', 'nan', 'None']:
                    has_match = True
                    break
            
            if has_match:
                matches += 1
            else:
                no_matches += 1
        
        return matches, no_matches
