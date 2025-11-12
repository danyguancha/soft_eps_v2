

from typing import Any, Dict, List
from services.duckdb_service.duckdb_service import duckdb_service


class ReportActivity:
    def _clean_table_reference(self, data_source: str) -> str:
        """Extrae referencia limpia de la tabla para queries"""
        if data_source.startswith("read_parquet('") and data_source.endswith("')"):
            clean_path = data_source[14:-2]
            return f"'{clean_path}'"
        elif data_source.startswith("'") and data_source.endswith("'"):
            return data_source
        else:
            return f"'{data_source}'"


    def _build_activity_query(self, table_reference: str, column: str, 
                            age_filter: str, geo_filter: str, corte_fecha: str) -> str:
        """Construye el query SQL para una actividad específica"""
        return f"""
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
            ({column} IS NULL OR TRIM({column}) = '')
            AND ({age_filter})
            AND "Fecha Nacimiento" IS NOT NULL 
            AND TRIM("Fecha Nacimiento") != ''
            AND TRY_CAST(strptime("Fecha Nacimiento", '%d/%m/%Y') AS DATE) IS NOT NULL
            AND {geo_filter}
        ORDER BY "Departamento", "Municipio", "Nombre IPS", "Primer Apellido", "Primer Nombre"
        """


    def _build_stats_query(self, table_reference: str, column: str, 
                        age_filter: str, geo_filter: str) -> str:
        """Construye el query SQL para estadísticas de una actividad"""
        return f"""
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


    def _process_activity_row(self, row: tuple, column: str) -> dict:
        """Procesa una fila individual de inasistente"""
        return {
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
        }


    def _process_stats_result(self, stats_result: tuple) -> dict:
        """Procesa resultado de estadísticas"""
        return {
            "total_inasistentes": int(stats_result[0]) if stats_result[0] else 0,
            "departamentos_afectados": int(stats_result[1]) if stats_result[1] else 0,
            "municipios_afectados": int(stats_result[2]) if stats_result[2] else 0,
            "ips_afectadas": int(stats_result[3]) if stats_result[3] else 0
        }


    def _process_single_activity(self, table_reference: str, column: str, 
                                age_filter: str, geo_filter: str, corte_fecha: str) -> dict:
        """Procesa una actividad individual completa"""
        # Query de datos
        activity_sql = self._build_activity_query(
            table_reference, column, age_filter, geo_filter, corte_fecha
        )
        activity_result = duckdb_service.conn.execute(activity_sql).fetchall()
        
        # Query de estadísticas
        stats_sql = self._build_stats_query(table_reference, column, age_filter, geo_filter)
        stats_result = duckdb_service.conn.execute(stats_sql).fetchone()
        
        # Procesar resultados
        inasistentes_data = [
            self._process_activity_row(row, column) 
            for row in activity_result
        ]
        
        statistics = self._process_stats_result(stats_result)
        
        print(f"✓ Actividad procesada: {column.replace('\"', '')} - {len(inasistentes_data)} inasistentes")
        
        return {
            "actividad": column.replace('"', ''),
            "inasistentes": inasistentes_data,
            "statistics": statistics
        }


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
        
        # Extraer referencia limpia para las queries
        table_reference = self._clean_table_reference(data_source)
        
        # Procesar cada actividad
        for column in activity_columns:
            try:
                report = self._process_single_activity(
                    table_reference, column, age_filter, geo_filter, corte_fecha
                )
                activity_reports.append(report)
                
            except Exception as e:
                print(f"❌ Error procesando actividad {column}: {e}")
                continue
        
        return activity_reports
