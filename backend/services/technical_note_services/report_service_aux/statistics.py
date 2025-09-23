

from typing import Any, Dict, List


class Statistics:
    def calculate_global_statistics(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calcula estadísticas globales del reporte"""
        total_denominador_global = sum(item.get("denominador", 0) for item in items)
        total_numerador_global = sum(item.get("numerador", 0) for item in items)
        total_sin_datos_global = sum(item.get("sin_datos", 0) for item in items)
        
        cobertura_global = (total_numerador_global / total_denominador_global * 100) if total_denominador_global > 0 else 0.0
        
        # Estadísticas de cobertura
        coverages = [item.get("cobertura_porcentaje", 0.0) for item in items]
        actividades_100_pct = len([c for c in coverages if c >= 100.0])
        actividades_menos_50_pct = len([c for c in coverages if c < 50.0])
        
        return {
            "total_actividades": len(items),
            "total_denominador_global": total_denominador_global,
            "total_numerador_global": total_numerador_global,
            "total_sin_datos_global": total_sin_datos_global,
            "cobertura_global_porcentaje": round(cobertura_global, 2),
            "actividades_100_pct_cobertura": actividades_100_pct,
            "actividades_menos_50_pct_cobertura": actividades_menos_50_pct,
            "mejor_cobertura": max(coverages) if coverages else 0.0,
            "peor_cobertura": min(coverages) if coverages else 0.0,
            "cobertura_promedio": round(sum(coverages) / len(coverages), 2) if coverages else 0.0
        }