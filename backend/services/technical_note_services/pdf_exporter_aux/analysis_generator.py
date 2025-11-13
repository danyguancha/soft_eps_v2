# services/technical_note_services/report_service_aux/analysis_generator.py
from typing import Dict, Any, List

from services.technical_note_services.pdf_exporter_aux.pdf_config import (
    SEMAFORO_MUY_DEFICIENTE,
    UMBRAL_ACEPTABLE,
    UMBRAL_DEFICIENTE,
    UMBRAL_OPTIMO,
)


class AnalysisGenerator:
    """Generador de análisis automáticos"""

    @staticmethod
    def generate_global_analysis(global_stats: Dict[str, Any]) -> List[str]:
        """Genera análisis de estadísticas globales"""
        cobertura_global = global_stats.get("cobertura_global_porcentaje", 0)
        total_actividades = global_stats.get("total_actividades", 0)
        actividades_optimas = global_stats.get("actividades_100_pct_cobertura", 0)
        actividades_deficientes = global_stats.get(
            "actividades_menos_50_pct_cobertura", 0
        )
        mejor_cobertura = global_stats.get("mejor_cobertura", 0)
        peor_cobertura = global_stats.get("peor_cobertura", 0)

        analysis = []

        # Evaluación general
        evaluation, recommendation = AnalysisGenerator._evaluate_coverage(
            cobertura_global
        )
        analysis.append(
            f"El análisis de {total_actividades} actividades evaluadas revela una cobertura global de {cobertura_global:.1f}%, "
            f"lo que se clasifica como un desempeño {evaluation}. {recommendation}"
        )

        # Distribución
        if actividades_optimas > 0 and total_actividades > 0:
            pct_optimas = actividades_optimas / total_actividades * 100
            analysis.append(
                f"<b>Distribución del desempeño:</b> {actividades_optimas} actividades ({pct_optimas:.1f}%) alcanzaron "
                f"cobertura óptima (≥90%), lo que representa una fortaleza significativa del sistema. Sin embargo, "
                f"{actividades_deficientes} actividades presentan cobertura deficiente (<60%), requiriendo atención prioritaria."
            )

        # Dispersión
        dispersion = mejor_cobertura - peor_cobertura
        analysis.append(
            AnalysisGenerator._analyze_dispersion(
                dispersion, mejor_cobertura, peor_cobertura
            )
        )

        return analysis

    @staticmethod
    def _evaluate_coverage(cobertura: float) -> tuple:
        """Evalúa cobertura y retorna evaluación y recomendación"""
        if cobertura >= UMBRAL_OPTIMO:
            return (
                "excelente",
                "Mantener las estrategias actuales y documentar buenas prácticas para replicarlas en otras áreas.",
            )
        elif cobertura >= UMBRAL_ACEPTABLE:
            return (
                "buena",
                "Identificar oportunidades de mejora en actividades específicas para alcanzar el nivel óptimo.",
            )
        elif cobertura >= UMBRAL_DEFICIENTE:
            return (
                "regular",
                "Requiere intervención inmediata para mejorar indicadores deficientes y evitar deterioro adicional.",
            )
        else:
            return (
                "deficiente",
                "Situación crítica que requiere intervención urgente y reestructuración de estrategias.",
            )

    @staticmethod
    def _analyze_dispersion(dispersion: float, mejor: float, peor: float) -> str:
        """Analiza dispersión de resultados"""
        if dispersion > 50:
            return (
                f"<b>Dispersión de resultados:</b> Se observa una alta variabilidad entre actividades, con una diferencia "
                f"de {dispersion:.1f} puntos porcentuales entre la mejor ({mejor:.1f}%) y peor ({peor:.1f}%) "
                f"cobertura. Esta dispersión indica oportunidades de estandarización y transferencia de conocimiento entre áreas."
            )
        else:
            return (
                f"<b>Consistencia en resultados:</b> La dispersión moderada de {dispersion:.1f} puntos porcentuales "
                f"entre actividades indica un desempeño relativamente homogéneo, facilitando intervenciones sistémicas."
            )

    @staticmethod
    def generate_activities_analysis(items: List[Dict[str, Any]]) -> List[str]:
        """Genera análisis de actividades"""
        if not items:
            return []

        analysis = []
        items_sorted = sorted(
            items, key=lambda x: x.get("cobertura_porcentaje", 0), reverse=True
        )

        # Top 3 mejores
        top3 = items_sorted[:3]
        if top3:
            top_names = [
                f"{item.get('keyword', '').upper()} ({item.get('cobertura_porcentaje', 0):.1f}%)"
                for item in top3
            ]
            analysis.append(
                "<b>Actividades destacadas:</b> Las tres actividades con mejor desempeño son: "
                + ", ".join(top_names)
                + "."
            )

        # Bottom 3 peores
        bottom3 = items_sorted[-3:]
        if bottom3:
            bottom_names = [
                f"{item.get('keyword', '').upper()} ({item.get('cobertura_porcentaje', 0):.1f}%)"
                for item in bottom3
            ]
            analysis.append(
                "<b>Actividades prioritarias:</b> Las tres actividades que requieren intervención urgente son: "
                + ", ".join(bottom_names)
                + ". Se recomienda análisis de causas raíz y plan de acción inmediato."
            )

        # Actividades críticas
        critical = [
            item
            for item in items
            if item.get("semaforizacion") == SEMAFORO_MUY_DEFICIENTE
        ]
        if critical:
            analysis.append(
                f'<b>Alerta crítica:</b> {len(critical)} actividades se encuentran en estado "{SEMAFORO_MUY_DEFICIENTE}" (<60% cobertura). '
                f"Estas requieren atención inmediata del equipo directivo y asignación de recursos específicos para su mejora."
            )

        return analysis

    @staticmethod
    def generate_temporal_analysis(years_dict: Dict[str, Any]) -> List[str]:
        """Genera análisis temporal"""
        if not years_dict:
            return []

        analysis = []
        years_list = sorted(years_dict.keys(), key=lambda y: int(y))

        # Tendencia anual
        if len(years_list) >= 2:
            year_older = years_dict[years_list[0]]
            year_recent = years_dict[years_list[-1]]

            pct_older = year_older.get("pct", 0)
            pct_recent = year_recent.get("pct", 0)
            trend_diff = pct_recent - pct_older

            analysis.append(
                AnalysisGenerator._analyze_trend(
                    trend_diff, years_list[0], years_list[-1]
                )
            )

        # Mejor y peor mes
        if years_list:
            last_year = years_dict[years_list[-1]]
            months_dict = last_year.get("months", {})

            if months_dict:
                months_pcts = [
                    (month, data.get("pct", 0)) for month, data in months_dict.items()
                ]
                months_pcts.sort(key=lambda x: x[1], reverse=True)

                best_month = months_pcts[0]
                worst_month = months_pcts[-1]

                analysis.append(
                    f"En {years_list[-1]}, el mejor desempeño se registró en {best_month[0]} ({best_month[1]:.1f}%) "
                    f"mientras que {worst_month[0]} presentó la menor cobertura ({worst_month[1]:.1f}%)."
                )

        return analysis

    @staticmethod
    def _analyze_trend(trend_diff: float, year_start: str, year_end: str) -> str:
        """Analiza tendencia temporal"""
        if trend_diff > 5:
            return f"<b>Tendencia positiva:</b> Se observa mejora de {trend_diff:.1f} puntos porcentuales entre {year_start} y {year_end}."
        elif trend_diff < -5:
            return f"<b>Alerta de deterioro:</b> Disminución de {abs(trend_diff):.1f} puntos porcentuales entre {year_start} y {year_end}. Requiere análisis de causas."
        else:
            return f"<b>Estabilidad:</b> Desempeño relativamente estable con variación de {abs(trend_diff):.1f} puntos porcentuales."
