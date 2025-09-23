

from typing import Any, Dict, List, Optional


class ReportEmpty:
    def build_empty_report(
        self, filename: str, keywords: Optional[List[str]], 
        geographic_filters: Dict[str, Optional[str]]
    ) -> Dict[str, Any]:
        """Construye reporte vacío cuando no hay matches"""
        return {
            "success": True,
            "filename": filename,
            "rules": {"keywords": keywords or []},
            "geographic_filters": {**geographic_filters, "filter_type": "numerador_denominador"},
            "items": [],
            "totals_by_keyword": {},
            "temporal_data": {},
            "global_statistics": {
                "total_denominador_global": 0,
                "total_numerador_global": 0,
                "cobertura_global_porcentaje": 0.0
            },
            "message": "No se encontraron columnas con las palabras clave especificadas",
            "metodo": "NUMERADOR_DENOMINADOR_VACIO"
        }