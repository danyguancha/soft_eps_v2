# services/technical_note_services/geographic_service.py
from typing import Dict, Any, Optional
from services.duckdb_service.duckdb_service import duckdb_service
from services.keyword_age_report import ColumnKeywordReportService

class GeographicService:
    """Servicio especializado para operaciones geográficas"""
    
    def __init__(self):
        self.column_service = ColumnKeywordReportService()
        self.geo_type_mapping = {
            'departamentos': 'departamento',
            'municipios': 'municipio',
            'ips': 'ips'
        }
    
    def get_geographic_values(
        self, 
        data_source: str,
        geo_type: str,
        departamento: Optional[str] = None,
        municipio: Optional[str] = None
    ) -> Dict[str, Any]:
        """Obtiene valores únicos de columnas geográficas"""
        try:
            print(f"Obteniendo {geo_type}")
            print(f"Filtros: departamento='{departamento}', municipio='{municipio}'")
            
            # Construir filtros padre
            parent_filter = self._build_parent_filters(geo_type, departamento, municipio)
            
            # Mapear tipo geográfico
            geo_type_for_service = self.geo_type_mapping.get(geo_type, geo_type)
            print(f"Mapeo: {geo_type} -> {geo_type_for_service}")
            
            # Generar y ejecutar consulta
            geo_sql = self.column_service.get_unique_geographic_values_sql(
                data_source, 
                geo_type_for_service, 
                duckdb_service.escape_identifier, 
                parent_filter
            )
            
            return self._execute_geographic_query(geo_sql, geo_type, parent_filter)
            
        except Exception as e:
            print(f"Error en get_geographic_values: {e}")
            raise Exception(f"Error obteniendo valores geográficos: {e}")
    
    def _build_parent_filters(
        self, 
        geo_type: str, 
        departamento: Optional[str], 
        municipio: Optional[str]
    ) -> Dict[str, str]:
        """Construye filtros padre para consultas geográficas"""
        parent_filter = {}
        
        if geo_type == 'municipios' and departamento and departamento.strip():
            parent_filter['departamento'] = departamento.strip()
            print(f"Filtro para municipios: departamento = '{departamento}'")
        
        elif geo_type == 'ips':
            if municipio and municipio.strip():
                parent_filter['municipio'] = municipio.strip()
                print(f"Filtro para IPS: municipio = '{municipio}'")
            if departamento and departamento.strip():
                parent_filter['departamento'] = departamento.strip()
                print(f"Filtro adicional para IPS: departamento = '{departamento}'")
        
        return parent_filter
    
    def _execute_geographic_query(
        self, 
        geo_sql: str, 
        geo_type: str, 
        parent_filter: Dict[str, str]
    ) -> Dict[str, Any]:
        """Ejecuta consulta geográfica y procesa resultados"""
        try:
            print(f"Ejecutando SQL: {geo_sql}")
            result = duckdb_service.conn.execute(geo_sql).fetchall()
            
            values = []
            for row in result:
                if row[0] is not None:
                    value = str(row[0]).strip()
                    if self._is_valid_value(value):
                        values.append(value)
            
            # Eliminar duplicados y ordenar
            values = sorted(list(set(values)))
            
            print(f"{geo_type} obtenidos: {len(values)} valores únicos")
            if values:
                print(f"Primeros 10 valores: {values[:10]}")
            
            return {
                "success": True,
                "geo_type": geo_type,
                "values": values,
                "total_values": len(values),
                "filters_applied": parent_filter,
                "engine": "DuckDB_Service_Methods"
            }
            
        except Exception as sql_error:
            print(f"Error SQL geográfico: {sql_error}")
            return {
                "success": False,
                "error": f"Error ejecutando consulta geográfica: {sql_error}",
                "geo_type": geo_type,
                "values": [],
                "sql_used": geo_sql
            }
    
    def _is_valid_value(self, value: str) -> bool:
        """Valida si un valor geográfico es válido"""
        invalid_values = {'NULL', 'null', 'None', 'none', 'NaN', 'nan', ''}
        return value and value not in invalid_values
