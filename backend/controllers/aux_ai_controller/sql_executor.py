# controllers/aux_ai_controller/sql_executor.py
import asyncio
from services.duckdb_service.duckdb_service import duckdb_service
import os


class SQLExecutor:
    """Ejecuta consultas SQL para análisis de datos con logging mejorado"""
    
    async def calculate_statistics(self, file_id: str, columns: list, parquet_path: str = None) -> dict:
        """Calcula estadísticas con logging detallado"""
        try:
            loop = asyncio.get_event_loop()
            table_path = parquet_path if parquet_path else file_id
            result = await loop.run_in_executor(None, self._execute_stats_sync, table_path, columns)
            return result
        except Exception as e:
            print(f"Error calculando estadísticas: {e}")
            import traceback
            print(traceback.format_exc())
            return {}


    def _execute_stats_sync(self, table_path: str, columns: list) -> dict:
        """Ejecuta cálculo de estadísticas sincrónico"""
        if not os.path.exists(table_path):
            print(f"Archivo no encontrado: {table_path}")
            return {}
        
        column_types = self._get_column_types(table_path)
        if not column_types:
            return {}
        
        numeric_cols, categorical_cols = self._classify_columns(columns, column_types)
        print(f"{len(numeric_cols)} numéricas, {len(categorical_cols)} categóricas")
        
        results = {}
        results.update(self._process_numeric_columns(numeric_cols, table_path))
        results.update(self._process_categorical_columns(categorical_cols, table_path))
        
        print("Estadísticas calculadas exitosamente")
        return results


    def _get_column_types(self, table_path: str) -> dict:
        """Obtiene tipos de todas las columnas"""
        try:
            type_query = f"DESCRIBE SELECT * FROM '{table_path}' LIMIT 1"
            column_types_df = duckdb_service.conn.execute(type_query).fetchdf()
            return {row['column_name']: str(row['column_type']).upper() 
                    for _, row in column_types_df.iterrows()}
        except Exception as e:
            print(f"Error obteniendo tipos: {e}")
            return {}


    def _classify_columns(self, columns: list, column_types: dict) -> tuple:
        """Clasifica columnas en numéricas y categóricas"""
        numeric_cols = []
        categorical_cols = []
        numeric_types = ['INT', 'DOUBLE', 'FLOAT', 'DECIMAL', 'NUMERIC', 'BIGINT']
        
        for col in columns:
            col_type = column_types.get(col, "")
            if any(t in col_type for t in numeric_types):
                numeric_cols.append(col)
            else:
                categorical_cols.append(col)
        
        return numeric_cols, categorical_cols


    def _calculate_numeric_stats(self, col: str, table_path: str) -> dict:
        """Calcula estadísticas para una columna numérica"""
        col_escaped = f'"{col}"'
        stats_query = f"""
            SELECT 
                COUNT({col_escaped}) as count,
                AVG({col_escaped}::DOUBLE) as avg,
                MIN({col_escaped}::DOUBLE) as min,
                MAX({col_escaped}::DOUBLE) as max,
                MEDIAN({col_escaped}::DOUBLE) as median,
                STDDEV({col_escaped}::DOUBLE) as stddev
            FROM '{table_path}'
            WHERE {col_escaped} IS NOT NULL
        """
        stats = duckdb_service.conn.execute(stats_query).fetchone()
        
        return {
            'count': int(stats[0]) if stats[0] else 0,
            'promedio': round(float(stats[1]), 2) if stats[1] else 0,
            'minimo': round(float(stats[2]), 2) if stats[2] else 0,
            'maximo': round(float(stats[3]), 2) if stats[3] else 0,
            'mediana': round(float(stats[4]), 2) if stats[4] else 0,
            'desviacion_std': round(float(stats[5]), 2) if stats[5] else 0
        }


    def _calculate_categorical_stats(self, col: str, table_path: str) -> dict:
        """Calcula estadísticas para una columna categórica"""
        col_escaped = f'"{col}"'
        
        freq_query = f"""
            SELECT 
                {col_escaped} as value,
                COUNT(*) as frequency,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM '{table_path}'
            WHERE {col_escaped} IS NOT NULL
            GROUP BY {col_escaped}
            ORDER BY frequency DESC
            LIMIT 5
        """
        freq_df = duckdb_service.conn.execute(freq_query).fetchdf()
        
        unique_query = f"""
            SELECT COUNT(DISTINCT {col_escaped}) as unique_count
            FROM '{table_path}'
            WHERE {col_escaped} IS NOT NULL
        """
        unique_count = duckdb_service.conn.execute(unique_query).fetchone()[0]
        
        return {
            'valores_unicos': int(unique_count),
            'top_5': freq_df.to_dict('records')
        }


    def _process_numeric_columns(self, numeric_cols: list, table_path: str) -> dict:
        """Procesa todas las columnas numéricas"""
        if not numeric_cols:
            return {}
        
        results = {'numeric': {}}
        for col in numeric_cols[:5]:
            try:
                results['numeric'][col] = self._calculate_numeric_stats(col, table_path)
                print(f"{col}: avg={results['numeric'][col]['promedio']:.2f}")
            except Exception as e:
                print(f"Error en {col}: {e}")
                continue
        
        return results


    def _process_categorical_columns(self, categorical_cols: list, table_path: str) -> dict:
        """Procesa todas las columnas categóricas"""
        if not categorical_cols:
            return {}
        
        results = {'categorical': {}}
        for col in categorical_cols[:8]:
            try:
                results['categorical'][col] = self._calculate_categorical_stats(col, table_path)
                print(f"{col}: {results['categorical'][col]['valores_unicos']} valores únicos")
            except Exception as e:
                print(f"Error en {col}: {e}")
                continue
        
        return results




# Instancia global
sql_executor = SQLExecutor()
