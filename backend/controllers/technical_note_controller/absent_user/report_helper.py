# controllers/technical_note_controller/absent_user/report_helper.py
from typing import Dict, List, Any, Optional
from services.duckdb_service.duckdb_service import duckdb_service



class ReportHelper:
    """Métodos auxiliares para reportes"""
    
    @staticmethod
    def has_geographic_filters(geographic_info: Dict) -> bool:
        """Verifica si hay filtros geográficos o régimen"""
        return any([
            geographic_info.get('depto'),
            geographic_info.get('muni'),
            geographic_info.get('ips_name'),
            geographic_info.get('regimen')  # ← Ya está
        ])
    
    @staticmethod
    def build_where_clause(geographic_info: Dict) -> str:
        """Construye cláusula WHERE para filtros geográficos y régimen"""
        
        # ← NUEVO: LOG COMPLETO DEL INPUT
        print(f"\n{'🔍'*40}")
        print(f"BUILD_WHERE_CLAUSE LLAMADO")
        print(f"{'🔍'*40}")
        print(f"geographic_info recibido: {geographic_info}")
        print(f"Tipo: {type(geographic_info)}")
        print(f"Keys: {list(geographic_info.keys())}")
        print(f"")
        print(f"Valores individuales:")
        print(f"   depto: {geographic_info.get('depto')}")
        print(f"   muni: {geographic_info.get('muni')}")
        print(f"   ips_name: {geographic_info.get('ips_name')}")
        print(f"   regimen: {geographic_info.get('regimen')}")  # ← CRÍTICO
        print(f"{'🔍'*40}\n")
        
        where_conditions = []
        
        if geographic_info.get('depto'):
            condition = f'"Departamento" = \'{geographic_info["depto"]}\''
            where_conditions.append(condition)
            print(f"   ✅ Agregada condición: {condition}")
        
        if geographic_info.get('muni'):
            condition = f'"Municipio" = \'{geographic_info["muni"]}\''
            where_conditions.append(condition)
            print(f"   ✅ Agregada condición: {condition}")
        
        if geographic_info.get('ips_name'):
            condition = f'"Nombre IPS" = \'{geographic_info["ips_name"]}\''
            where_conditions.append(condition)
            print(f"   ✅ Agregada condición: {condition}")

        # ← CRÍTICO: Filtro de régimen
        if geographic_info.get('regimen'):
            condition = f"\"Régimen\" = '{geographic_info['regimen']}'"
            where_conditions.append(condition)
            print(f"   ✅ Agregada condición RÉGIMEN: {condition}")
        else:
            print(f"   ⚠️⚠️⚠️ NO HAY RÉGIMEN EN geographic_info")
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        
        print(f"\n   📋 WHERE clause FINAL:")
        print(f"   {where_clause}")
        print(f"{'='*80}\n")
        
        return where_clause
    
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
                'ips': geographic_info.get('ips_name'),
                'regimen': geographic_info.get('regimen')
            },
            'inasistentes_por_actividad': [],
            'resumen_general': {
                'total_actividades_evaluadas': 0,
                'total_inasistentes_global': 0,
                'actividades_con_inasistentes': 0,
                'actividades_sin_inasistentes': 0
            },
            'engine': 'DuckDB_Modular_v3',
            'message': 'No se encontraron datos'
        }
