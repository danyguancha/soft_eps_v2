

from typing import Any, Dict
from services.duckdb_service.duckdb_service import duckdb_service


from services.technical_note_services.data_source_service import DataSourceService
class AgeController:
    def get_age_ranges(
        self, 
        filename: str,
        corte_fecha: str = "2025-07-31",
        path_technical_note = ''
    ) -> Dict[str, Any]:
        """Obtiene rangos de edades únicas en años y meses - CON FORMATO DD/MM/YYYY"""
        try:
            print(f"📅 Obteniendo rangos de edades para: {filename}")
            print(f"🗓️ Fecha de corte: {corte_fecha}")
            
            file_key = f"technical_{filename.replace('.', '_').replace(' ', '_').replace('-', '_')}"
            
            # ASEGURAR FUENTE DE DATOS
            data_source = DataSourceService(path_technical_note).ensure_data_source_available(filename, file_key)
            
            # Obtener edades en años con CAST seguro (mantener igual)
            years_sql = f"""
            SELECT DISTINCT CAST(edad AS INTEGER) as age_years
            FROM {data_source}
            WHERE edad IS NOT NULL 
            AND TRIM(edad) != ''
            AND TRY_CAST(edad AS INTEGER) IS NOT NULL
            AND TRY_CAST(edad AS INTEGER) >= 0
            ORDER BY age_years ASC
            """
            
            years_result = duckdb_service.conn.execute(years_sql).fetchall()
            unique_years = [int(row[0]) for row in years_result if row[0] is not None]
            
            print(f"📊 Edades en años encontradas: {len(unique_years)} valores únicos")
            
            # CORREGIR: Calcular edades en meses usando strptime para formato DD/MM/YYYY
            months_sql = f"""
            SELECT DISTINCT date_diff('month', 
                strptime("Fecha Nacimiento", '%d/%m/%Y'), 
                DATE '{corte_fecha}'
            ) as edad_meses
            FROM {data_source}
            WHERE "Fecha Nacimiento" IS NOT NULL
            AND TRIM("Fecha Nacimiento") != ''
            AND TRY_CAST(strptime("Fecha Nacimiento", '%d/%m/%Y') AS DATE) IS NOT NULL
            AND strptime("Fecha Nacimiento", '%d/%m/%Y') <= DATE '{corte_fecha}'
            AND date_diff('month', strptime("Fecha Nacimiento", '%d/%m/%Y'), DATE '{corte_fecha}') >= 0
            ORDER BY edad_meses ASC
            """
            
            months_result = duckdb_service.conn.execute(months_sql).fetchall()
            unique_months = [int(row[0]) for row in months_result if row[0] is not None]
            
            print(f"📊 Edades en meses encontradas: {len(unique_months)} valores únicos")
            
            # CORREGIR: Estadísticas con formato DD/MM/YYYY correcto
            stats_sql = f"""
            SELECT 
                COUNT(*) as total_registros,
                COUNT(CASE WHEN "Fecha Nacimiento" IS NOT NULL AND TRIM("Fecha Nacimiento") != '' 
                        AND TRY_CAST(strptime("Fecha Nacimiento", '%d/%m/%Y') AS DATE) IS NOT NULL THEN 1 END) as registros_con_fecha_nacimiento,
                COUNT(CASE WHEN edad IS NOT NULL AND TRIM(edad) != '' 
                        AND TRY_CAST(edad AS INTEGER) IS NOT NULL THEN 1 END) as registros_con_edad,
                MIN(TRY_CAST(edad AS INTEGER)) as edad_min_años,
                MAX(TRY_CAST(edad AS INTEGER)) as edad_max_años,
                MIN(CASE WHEN TRY_CAST(strptime("Fecha Nacimiento", '%d/%m/%Y') AS DATE) IS NOT NULL 
                        THEN date_diff('month', strptime("Fecha Nacimiento", '%d/%m/%Y'), DATE '{corte_fecha}') 
                        ELSE NULL END) as edad_min_meses,
                MAX(CASE WHEN TRY_CAST(strptime("Fecha Nacimiento", '%d/%m/%Y') AS DATE) IS NOT NULL 
                        THEN date_diff('month', strptime("Fecha Nacimiento", '%d/%m/%Y'), DATE '{corte_fecha}') 
                        ELSE NULL END) as edad_max_meses
            FROM {data_source}
            """
            
            stats_result = duckdb_service.conn.execute(stats_sql).fetchone()
            
            statistics = {
                "total_registros": int(stats_result[0]) if stats_result[0] else 0,
                "registros_con_fecha_nacimiento": int(stats_result[1]) if stats_result[1] else 0,
                "registros_con_edad": int(stats_result[2]) if stats_result[2] else 0,
                "rango_años": {
                    "min": int(stats_result[3]) if stats_result[3] is not None else 0,
                    "max": int(stats_result[4]) if stats_result[4] is not None else 0
                },
                "rango_meses": {
                    "min": int(stats_result[5]) if stats_result[5] is not None else 0,
                    "max": int(stats_result[6]) if stats_result[6] is not None else 0
                }
            }
            
            print(f"Rangos obtenidos - Años: {statistics['rango_años']}, Meses: {statistics['rango_meses']}")
            
            return {
                "success": True,
                "filename": filename,
                "corte_fecha": corte_fecha,
                "age_ranges": {
                    "years": unique_years,
                    "months": unique_months
                },
                "statistics": statistics,
                "engine": "DuckDB_Service_Ultra_Fast"
            }
            
        except Exception as e:
            print(f"❌ Error obteniendo rangos de edades: {e}")
            import traceback
            traceback.print_exc()
            return {
                "success": False,
                "error": str(e),
                "filename": filename,
                "age_ranges": {
                    "years": [],
                    "months": []
                }
            }