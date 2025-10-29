# controllers/aux_ai_controller/sql_executor.py
import asyncio
from typing import Dict, Any, List
from services.duckdb_service.duckdb_service import duckdb_service
import os


class SQLExecutor:
    """Ejecuta consultas SQL para análisis de datos con logging mejorado"""
    
    async def calculate_statistics(self, file_id: str, columns: List[str], parquet_path: str = None) -> Dict[str, Any]:
        """Calcula estadísticas con logging detallado"""
        try:
            print(f"\n📊 Iniciando cálculo de estadísticas...")
            print(f"   File ID: {file_id}")
            print(f"   Parquet: {parquet_path}")
            print(f"   Columnas: {len(columns)}")
            
            loop = asyncio.get_event_loop()
            
            def execute_stats():
                results = {}
                
                table_path = parquet_path if parquet_path else file_id
                
                if not os.path.exists(table_path):
                    print(f"   ❌ Archivo no encontrado: {table_path}")
                    return {}
                
                # Obtener tipos de columnas
                try:
                    type_query = f"DESCRIBE SELECT * FROM '{table_path}' LIMIT 1"
                    column_types = duckdb_service.conn.execute(type_query).fetchdf()
                except Exception as e:
                    print(f"   ❌ Error obteniendo tipos: {e}")
                    return {}
                
                # Clasificar columnas
                numeric_cols = []
                categorical_cols = []
                
                for col in columns:
                    col_type = column_types[column_types['column_name'] == col]['column_type'].values
                    if len(col_type) > 0:
                        type_str = str(col_type[0]).upper()
                        if any(t in type_str for t in ['INT', 'DOUBLE', 'FLOAT', 'DECIMAL', 'NUMERIC', 'BIGINT']):
                            numeric_cols.append(col)
                        else:
                            categorical_cols.append(col)
                
                print(f"   📈 {len(numeric_cols)} numéricas, {len(categorical_cols)} categóricas")
                
                # Estadísticas numéricas
                if numeric_cols:
                    results['numeric'] = {}
                    for col in numeric_cols[:5]:
                        try:
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
                            
                            results['numeric'][col] = {
                                'count': int(stats[0]) if stats[0] else 0,
                                'promedio': round(float(stats[1]), 2) if stats[1] else 0,
                                'minimo': round(float(stats[2]), 2) if stats[2] else 0,
                                'maximo': round(float(stats[3]), 2) if stats[3] else 0,
                                'mediana': round(float(stats[4]), 2) if stats[4] else 0,
                                'desviacion_std': round(float(stats[5]), 2) if stats[5] else 0
                            }
                            print(f"   ✅ {col}: avg={stats[1]:.2f}")
                        except Exception as e:
                            print(f"   ⚠️ Error en {col}: {e}")
                            continue
                
                # Estadísticas categóricas
                if categorical_cols:
                    results['categorical'] = {}
                    for col in categorical_cols[:8]:
                        try:
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
                            
                            results['categorical'][col] = {
                                'valores_unicos': int(unique_count),
                                'top_5': freq_df.to_dict('records')
                            }
                            print(f"   ✅ {col}: {unique_count} valores únicos")
                        except Exception as e:
                            print(f"   ⚠️ Error en {col}: {e}")
                            continue
                
                print(f"   ✅ Estadísticas calculadas exitosamente")
                return results
            
            result = await loop.run_in_executor(None, execute_stats)
            return result
            
        except Exception as e:
            print(f"   ❌ Error calculando estadísticas: {e}")
            import traceback
            print(traceback.format_exc())
            return {}


# Instancia global
sql_executor = SQLExecutor()
