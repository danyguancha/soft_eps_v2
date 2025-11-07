

from typing import Any, Dict, List
from services.duckdb_service.duckdb_service import duckdb_service


class ReportActivity:
    def generate_activity_reports(
        self,
        data_source: str,
        activity_columns: List[str],
        age_filter: str,
        geo_filter: str,
        corte_fecha: str
    ) -> List[Dict[str, Any]]:
        """Genera reportes individuales por cada actividad"""
        activity_reports = []
        
        # EXTRAER RUTA LIMPIA PARA LAS QUERIES
        if data_source.startswith("read_parquet('") and data_source.endswith("')"):
            clean_path = data_source[14:-2]
            table_reference = f"'{clean_path}'"
        elif data_source.startswith("'") and data_source.endswith("'"):
            table_reference = data_source
        else:
            table_reference = f"'{data_source}'"
        
        for column in activity_columns:
            try:
                # Query para esta actividad específica
                activity_sql = f"""
                SELECT 
                    "Departamento" as departamento,
                    "Municipio" as municipio,
                    "Nombre IPS" as nombre_ips,
                    "Nro Identificación" as nro_identificacion,
                    "Primer Apellido" as primer_apellido,
                    "Segundo Apellido" as segundo_apellido,
                    "Primer Nombre" as primer_nombre,
                    "Segundo Nombre" as segundo_nombre,
                    "Fecha Nacimiento" as fecha_nacimiento,
                    TRY_CAST(edad AS INTEGER) as edad_anos,
                    date_diff('month', strptime("Fecha Nacimiento", '%d/%m/%Y'), DATE '{corte_fecha}') as edad_meses,
                    {column} as actividad_valor
                FROM {table_reference}
                WHERE 
                    -- Filtro de inasistencia para esta actividad
                    ({column} IS NULL OR TRIM({column}) = '')
                    
                    -- Filtros de edad
                    AND ({age_filter})
                    
                    -- Fecha de nacimiento válida
                    AND "Fecha Nacimiento" IS NOT NULL 
                    AND TRIM("Fecha Nacimiento") != ''
                    AND TRY_CAST(strptime("Fecha Nacimiento", '%d/%m/%Y') AS DATE) IS NOT NULL
                    
                    -- Filtros geográficos
                    AND {geo_filter}
                    
                ORDER BY "Departamento", "Municipio", "Nombre IPS", "Primer Apellido", "Primer Nombre"
                """
                
                activity_result = duckdb_service.conn.execute(activity_sql).fetchall()
                
                # Estadísticas para esta actividad
                stats_sql = f"""
                SELECT 
                    COUNT(*) as total_inasistentes,
                    COUNT(DISTINCT "Departamento") as departamentos_afectados,
                    COUNT(DISTINCT "Municipio") as municipios_afectados,
                    COUNT(DISTINCT "Nombre IPS") as ips_afectadas
                FROM {table_reference}
                WHERE 
                    ({column} IS NULL OR TRIM({column}) = '')
                    AND ({age_filter})
                    AND "Fecha Nacimiento" IS NOT NULL 
                    AND TRIM("Fecha Nacimiento") != ''
                    AND TRY_CAST(strptime("Fecha Nacimiento", '%d/%m/%Y') AS DATE) IS NOT NULL
                    AND {geo_filter}
                """
                
                stats_result = duckdb_service.conn.execute(stats_sql).fetchone()
                
                # Procesar resultados
                inasistentes_data = []
                for row in activity_result:
                    inasistentes_data.append({
                        "departamento": str(row[0]) if row[0] else "",
                        "municipio": str(row[1]) if row[1] else "",
                        "nombre_ips": str(row[2]) if row[2] else "",
                        "nro_identificacion": str(row[3]) if row[3] else "",
                        "primer_apellido": str(row[4]) if row[4] else "",
                        "segundo_apellido": str(row[5]) if row[5] else "",
                        "primer_nombre": str(row[6]) if row[6] else "",
                        "segundo_nombre": str(row[7]) if row[7] else "",
                        "fecha_nacimiento": str(row[8]) if row[8] else "",
                        "edad_anos": int(row[9]) if row[9] is not None else None,
                        "edad_meses": int(row[10]) if row[10] is not None else None,
                        "actividad_valor": str(row[11]) if row[11] else "VACÍO",
                        "columna_evaluada": column.replace('"', '')
                    })
                
                activity_reports.append({
                    "actividad": column.replace('"', ''),
                    "inasistentes": inasistentes_data,
                    "statistics": {
                        "total_inasistentes": int(stats_result[0]) if stats_result[0] else 0,
                        "departamentos_afectados": int(stats_result[1]) if stats_result[1] else 0,
                        "municipios_afectados": int(stats_result[2]) if stats_result[2] else 0,
                        "ips_afectadas": int(stats_result[3]) if stats_result[3] else 0
                    }
                })
                
                print(f"Actividad procesada: {column.replace('\"', '')} - {len(inasistentes_data)} inasistentes")
                
            except Exception as e:
                print(f"Error procesando actividad {column}: {e}")
                continue
        
        return activity_reports