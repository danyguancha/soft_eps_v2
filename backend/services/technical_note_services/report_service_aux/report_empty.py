# services/technical_note_services/report_service_aux/report_empty.py
from typing import Any, Dict, List, Optional


class ReportEmpty:
    """Constructor de reportes vacíos"""
    
    def build_empty_report(
        self,
        filename: str,
        corte_fecha: str,
        keywords: Optional[List[str]],
        geographic_filters: Dict[str, Optional[str]]
    ) -> Dict[str, Any]:
        """
        Construye reporte vacío cuando no hay matches
        
        Args:
            filename: Nombre del archivo
            corte_fecha: Fecha de corte del reporte
            keywords: Palabras clave buscadas
            geographic_filters: Filtros geográficos aplicados
            
        Returns:
            Diccionario con estructura de reporte vacío
        """
        return {
            "success": True,
            "filename": filename,
            "corte_fecha": corte_fecha,
            "keywords": keywords or [],
            "geographic_filters": {
                "departamento": geographic_filters.get('departamento'),
                "municipio": geographic_filters.get('municipio'),
                "ips": geographic_filters.get('ips')
            },
            "items": [],
            "total_rows": 0,
            "meses_reportados": 0,
            "metodo": "reporte_vacio",
            "message": "No se encontraron columnas con las palabras clave especificadas"
        }
    
    def build_empty_report_with_filters(
        self,
        filename: str,
        corte_fecha: str,
        keywords: Optional[List[str]],
        geographic_filters: Dict[str, Optional[str]],
        reason: str = "Sin datos"
    ) -> Dict[str, Any]:
        """
        Construye reporte vacío con mensaje personalizado
        
        Args:
            filename: Nombre del archivo
            corte_fecha: Fecha de corte
            keywords: Palabras clave
            geographic_filters: Filtros geográficos
            reason: Razón del reporte vacío
            
        Returns:
            Diccionario con estructura de reporte vacío
        """
        report = self.build_empty_report(filename, corte_fecha, keywords, geographic_filters)
        report['message'] = reason
        return report
