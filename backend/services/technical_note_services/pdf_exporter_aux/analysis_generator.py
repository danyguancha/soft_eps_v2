# services/technical_note_services/report_service_aux/analysis_generator.py
from typing import Dict, Any, List


class AnalysisGenerator:
    """Generador de análisis automáticos para reportes"""

    @staticmethod
    def generate_global_analysis(items: List[Dict[str, Any]]) -> List[str]:
        """Genera análisis de estadísticas globales basado en items"""
        if not items:
            return []
        
        analysis = []
        
        # Calcular estadísticas
        total_actividades = len(items)
        coberturas = [item.get('anual', {}).get('cobertura', 0) for item in items]
        cobertura_promedio = sum(coberturas) / len(coberturas) if coberturas else 0
        
        # Actividades óptimas (>=95%)
        actividades_optimas = sum(1 for c in coberturas if c >= 95)
        
        # Actividades deficientes (<60%)
        actividades_deficientes = sum(1 for c in coberturas if c < 60)
        
        mejor_cobertura = max(coberturas) if coberturas else 0
        peor_cobertura = min(coberturas) if coberturas else 0
        
        # Evaluación general
        evaluation, recommendation = AnalysisGenerator._evaluate_coverage(cobertura_promedio)
        analysis.append(
            f"El análisis de {total_actividades} consultas/procedimientos revela una cobertura promedio de "
            f"{cobertura_promedio:.1f}%, lo que se clasifica como un desempeño {evaluation}. {recommendation}"
        )
        
        # Distribución
        if actividades_optimas > 0:
            pct_optimas = (actividades_optimas / total_actividades) * 100
            analysis.append(
                f"<b>Distribución del desempeño:</b> {actividades_optimas} consultas/procedimientos "
                f"({pct_optimas:.1f}%) alcanzaron cobertura óptima (≥95%). "
                f"{actividades_deficientes} presentan cobertura deficiente (<60%), requiriendo atención prioritaria."
            )
        
        # Dispersión
        dispersion = mejor_cobertura - peor_cobertura
        analysis.append(
            AnalysisGenerator._analyze_dispersion(dispersion, mejor_cobertura, peor_cobertura)
        )
        
        return analysis

    @staticmethod
    def _evaluate_coverage(cobertura: float) -> tuple:
        """Evalúa cobertura y retorna evaluación y recomendación"""
        if cobertura >= 95:
            return (
                "excelente",
                "Mantener las estrategias actuales y documentar buenas prácticas."
            )
        elif cobertura >= 80:
            return (
                "bueno",
                "Identificar oportunidades de mejora para alcanzar nivel óptimo."
            )
        elif cobertura >= 60:
            return (
                "regular",
                "Requiere intervención para mejorar indicadores deficientes."
            )
        else:
            return (
                "deficiente",
                "Situación crítica que requiere intervención urgente."
            )

    @staticmethod
    def _analyze_dispersion(dispersion: float, mejor: float, peor: float) -> str:
        """Analiza dispersión de resultados"""
        if dispersion > 50:
            return (
                f"<b>Dispersión de resultados:</b> Alta variabilidad con diferencia de {dispersion:.1f} puntos "
                f"porcentuales entre la mejor ({mejor:.1f}%) y peor ({peor:.1f}%) cobertura. "
                f"Esto indica oportunidades de estandarización."
            )
        else:
            return (
                f"<b>Consistencia en resultados:</b> Dispersión moderada de {dispersion:.1f} puntos "
                f"porcentuales indica desempeño relativamente homogéneo."
            )

    @staticmethod
    def generate_activities_analysis(items: List[Dict[str, Any]]) -> List[str]:
        """Genera análisis de consultas/procedimientos"""
        if not items:
            return []

        analysis = []
        
        # Ordenar por cobertura anual
        items_sorted = sorted(
            items,
            key=lambda x: x.get('anual', {}).get('cobertura', 0),
            reverse=True
        )

        # Top 3 mejores
        top3 = items_sorted[:3]
        if top3:
            top_names = [
                f"{item.get('consulta_procedimiento', '')[:40]} ({item.get('anual', {}).get('cobertura', 0):.1f}%)"
                for item in top3
            ]
            analysis.append(
                "<b>Consultas/Procedimientos destacados:</b> " + ", ".join(top_names) + "."
            )

        # Bottom 3 peores
        bottom3 = items_sorted[-3:]
        if bottom3:
            bottom_names = [
                f"{item.get('consulta_procedimiento', '')[:40]} ({item.get('anual', {}).get('cobertura', 0):.1f}%)"
                for item in bottom3
            ]
            analysis.append(
                "<b>Consultas/Procedimientos prioritarios:</b> " + ", ".join(bottom_names) +
                ". Se recomienda análisis de causas raíz."
            )

        # Actividades críticas
        critical = [
            item for item in items
            if item.get('anual', {}).get('semaforizacion') == 'Muy Deficiente'
        ]
        if critical:
            analysis.append(
                f'<b>Alerta crítica:</b> {len(critical)} consultas/procedimientos en estado "Muy Deficiente" '
                f"(<60% cobertura). Requieren atención inmediata."
            )

        return analysis

    @staticmethod
    def generate_temporal_analysis(item: Dict[str, Any]) -> List[str]:
        """Genera análisis temporal para un item específico"""
        analysis = []
        
        # Analizar tendencia trimestral
        trimestres = ['T1', 'T2', 'T3', 'T4']
        coberturas_trim = []
        
        for trim in trimestres:
            trim_data = item.get(trim, {})
            if trim_data.get('cobertura', 0) > 0:
                coberturas_trim.append((trim, trim_data.get('cobertura', 0)))
        
        if len(coberturas_trim) >= 2:
            mejor_trim = max(coberturas_trim, key=lambda x: x[1])
            peor_trim = min(coberturas_trim, key=lambda x: x[1])
            
            analysis.append(
                f"<b>Análisis trimestral:</b> Mejor desempeño en {mejor_trim[0]} ({mejor_trim[1]:.1f}%), "
                f"menor desempeño en {peor_trim[0]} ({peor_trim[1]:.1f}%)."
            )
        
        # Análisis anual
        anual_data = item.get('anual', {})
        cobertura_anual = anual_data.get('cobertura', 0)
        semaforo_anual = anual_data.get('semaforizacion', '')
        
        if cobertura_anual > 0:
            analysis.append(
                f"<b>Resultado anual:</b> Cobertura de {cobertura_anual:.1f}% - {semaforo_anual}."
            )
        
        return analysis
