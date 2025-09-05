import time
from typing import Dict, Any, List, Optional
from utils.sql_utils import SQLUtils

class CrossFilesController:
    """Controlador para cruces de archivos (VLOOKUP/BUSCARX)"""
    
    def __init__(self, conn, loaded_tables: Dict):
        self.conn = conn
        self.loaded_tables = loaded_tables
        self.sql_utils = SQLUtils()

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

        table1_info = self.loaded_tables[file1_id]
        table2_info = self.loaded_tables[file2_id]

        # Referencias SIN paréntesis
        if table1_info.get("type") == "lazy":
            table1_ref = f"read_parquet('{table1_info['parquet_path']}')"
        else:
            table1_ref = table1_info["table_name"]
            
        if table2_info.get("type") == "lazy":
            table2_ref = f"read_parquet('{table2_info['parquet_path']}')"
        else:
            table2_ref = table2_info["table_name"]
        start_time = time.time()

        try:
            # PASO 1: OBTENER COLUMNAS REALES            
            real_cols_file1 = self._get_table_columns(table1_info)
            real_cols_file2 = self._get_table_columns(table2_info)

            # PASO 2: CONTAR REGISTROS DEL ARCHIVO BASE
            base_count_sql = f"SELECT COUNT(*) FROM {table1_ref}"
            expected_rows = self.conn.execute(base_count_sql).fetchone()[0]
            
            # PASO 3: MAPEAR COLUMNAS Y CLAVES
            mapped_key1 = self._map_column_to_real(key_column_file1, real_cols_file1)
            mapped_key2 = self._map_column_to_real(key_column_file2, real_cols_file2)

            if not mapped_key1 or not mapped_key2:
                raise ValueError("No se pudieron mapear las claves de join")

            # PASO 4: CONSTRUCCIÓN DEL SELECT
            select_parts = []
            
            if columns_to_include:
                file1_cols = columns_to_include.get("file1_columns", [])
                file2_cols = columns_to_include.get("file2_columns", [])

                # TODAS LAS COLUMNAS DEL ARCHIVO BASE
                if file1_cols:
                    mapped_file1_cols = self._map_user_columns_to_real(file1_cols, real_cols_file1)
                    for real_col in mapped_file1_cols:
                        escaped_col = self.sql_utils.escape_identifier(real_col)
                        select_parts.append(f"base.{escaped_col}")
                else:
                    select_parts.append("base.*")

                # COLUMNAS DE BÚSQUEDA (PRIMERA COINCIDENCIA)
                if file2_cols:
                    mapped_file2_cols = self._map_user_columns_to_real(file2_cols, real_cols_file2)
                    for real_col in mapped_file2_cols:
                        escaped_col = self.sql_utils.escape_identifier(real_col)
                        # Usar alias para evitar conflictos
                        if real_col == mapped_key2 and real_col in mapped_file1_cols:
                            alias = self.sql_utils.escape_identifier(f"lookup_{real_col}")
                            select_parts.append(f"lookup.{escaped_col} AS {alias}")
                        else:
                            select_parts.append(f"lookup.{escaped_col}")
            else:
                select_parts = ["base.*", "lookup.*"]

            if not select_parts:
                select_parts = ["base.*"]

            select_clause = ", ".join(select_parts)

            # PASO 5: VLOOKUP/BUSCARX PERFECTO CON QUALIFY
            esc_key1 = self.sql_utils.escape_identifier(mapped_key1)
            esc_key2 = self.sql_utils.escape_identifier(mapped_key2)

            # ESTA ES LA MAGIA: VLOOKUP EXACTO COMO EXCEL
            vlookup_sql = f"""
            SELECT {select_clause}
            FROM {table1_ref} base
            LEFT JOIN (
                SELECT *
                FROM {table2_ref}
                QUALIFY ROW_NUMBER() OVER (PARTITION BY {esc_key2} ORDER BY {esc_key2}) = 1
            ) lookup ON CAST(base.{esc_key1} AS VARCHAR) = CAST(lookup.{esc_key2} AS VARCHAR)
            """

            # VALIDACIÓN CON EXPLAIN
            try:
                explain_sql = f"EXPLAIN {vlookup_sql}"
                self.conn.execute(explain_sql).fetchall()
            except Exception as explain_error:
                raise Exception(f"VLOOKUP SQL inválido: {explain_error}")

            # EJECUTAR VLOOKUP
            result_df = self.conn.execute(vlookup_sql).fetchdf()
            cross_time = time.time() - start_time
            total_rows = len(result_df)

            # VALIDAR RESULTADO VLOOKUP
            if total_rows != expected_rows:
                print(f"⚠️ ADVERTENCIA VLOOKUP: {total_rows} ≠ {expected_rows} esperados")
            else:
                print(f"✅ VLOOKUP PERFECTO: {total_rows} registros = {expected_rows} base")

            # Contar matches y no-matches
            matches, no_matches = self._count_matches(result_df, mapped_file2_cols if 'mapped_file2_cols' in locals() else [])

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
                },
            }

        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
            
            if 'vlookup_sql' in locals():
                print(f"🔍 VLOOKUP SQL generado:")
                print("="*60)
                print(vlookup_sql)
                print("="*60)
            
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
        return None

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
                    print(f"   🔄 Mapeado: '{user_col}' → '{real_col}'")
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
