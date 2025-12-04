from typing import Dict, List
from services.duckdb_service.duckdb_service import duckdb_service


class ReportHelper:
    """Métodos auxiliares para reportes"""
    
    @staticmethod
    def has_geographic_filters(geographic_info: Dict) -> bool:
        """Verifica si hay filtros geográficos"""
        return any([
            geographic_info.get('depto'),
            geographic_info.get('muni'),
            geographic_info.get('ips_name')
        ])
    
    @staticmethod
    def build_where_clause(geographic_info: Dict) -> str:
        """Construye cláusula WHERE para filtros geográficos"""
        where_conditions = []
        
        if geographic_info.get('depto'):
            where_conditions.append(f'"Departamento" = \'{geographic_info["depto"]}\'')
        if geographic_info.get('muni'):
            where_conditions.append(f'"Municipio" = \'{geographic_info["muni"]}\'')
        if geographic_info.get('ips_name'):
            where_conditions.append(f'"Nombre IPS" = \'{geographic_info["ips_name"]}\'')
        
        return " AND ".join(where_conditions) if where_conditions else "1=1"
    
    @staticmethod
    def get_dataset_columns(data_source: str) -> List[str]:
        """Obtiene columnas del dataset"""
        describe_query = f"DESCRIBE SELECT * FROM {data_source}"
        columns_result = duckdb_service.conn.execute(describe_query).fetchall()
        return [row[0] for row in columns_result]
    
    @staticmethod
    def build_empty_report(
        filename: str,
        corte_fecha: str,
        keywords: List,
        geographic_info: Dict
    ) -> Dict:
        """Construye reporte vacío con estructura completa"""
        return {
            'success': True,
            'filename': filename,
            'corte_fecha': corte_fecha,
            'metodo': 'INASISTENTES_MES_A_MES_MODULAR',
            'filtros_aplicados': {
                'keywords': keywords or [],
                'departamento': geographic_info.get('depto'),
                'municipio': geographic_info.get('muni'),
                'ips': geographic_info.get('ips_name')
            },
            'inasistentes_por_actividad': [],
            'resumen_general': {  # SIEMPRE incluir esto
                'total_actividades_evaluadas': 0,
                'total_inasistentes_global': 0,
                'actividades_con_inasistentes': 0,
                'actividades_sin_inasistentes': 0
            },
            'engine': 'DuckDB_Modular_v3',
            'message': 'No se encontraron datos'
        }
