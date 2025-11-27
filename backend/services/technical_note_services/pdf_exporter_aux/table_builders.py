# services/technical_note_services/report_service_aux/table_builders.py
from typing import List, Dict, Any
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.platypus import Table, TableStyle

from services.technical_note_services.pdf_exporter_aux.pdf_config import COLOR_MAP, SEMAFORO_NA



class TableBuilder:
    """Constructor de tablas para el PDF"""

    @staticmethod
    def build_interpretation_table(interpretation: Dict[str, str]) -> Table:
        """Construye tabla de interpretación de semáforos"""
        table_data = [["Estado", "Descripción"]]

        for estado, descripcion in interpretation.items():
            table_data.append([estado, descripcion])

        table = Table(table_data, colWidths=[1.3 * inch, 6.2 * inch])
        style = TableBuilder._get_interpretation_table_style(interpretation)
        table.setStyle(TableStyle(style))

        return table

    @staticmethod
    def _get_interpretation_table_style(interpretation: Dict[str, str]) -> List:
        """Genera estilo para tabla de interpretación"""
        style = [
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1890ff")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
            ("ALIGN", (0, 0), (0, -1), "CENTER"),
            ("ALIGN", (1, 0), (1, -1), "LEFT"),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 10),
            ("FONTSIZE", (0, 1), (-1, -1), 8),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 10),
            ("TOPPADDING", (0, 1), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 1), (-1, -1), 6),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]

        for i, (estado, _) in enumerate(interpretation.items(), start=1):
            color_hex = COLOR_MAP.get(estado, COLOR_MAP[SEMAFORO_NA])
            try:
                bg_color = colors.HexColor(color_hex)
                bg_color.alpha = 0.2
                style.append(("BACKGROUND", (0, i), (0, i), bg_color))
                style.append(("TEXTCOLOR", (0, i), (0, i), colors.HexColor(color_hex)))
                style.append(("FONTNAME", (0, i), (0, i), "Helvetica-Bold"))
            except Exception:
                pass

        return style

    @staticmethod
    def build_statistics_table(items: List[Dict[str, Any]]) -> Table:
        """Construye tabla de estadísticas globales"""
        # Calcular estadísticas
        total_items = len(items)
        
        total_numerador = sum(item.get('anual', {}).get('numerador', 0) for item in items)
        total_denominador = sum(item.get('anual', {}).get('denominador', 0) for item in items)
        cobertura_global = (total_numerador / total_denominador * 100) if total_denominador > 0 else 0
        
        coberturas = [item.get('anual', {}).get('cobertura', 0) for item in items]
        mejor_cobertura = max(coberturas) if coberturas else 0
        peor_cobertura = min(coberturas) if coberturas else 0
        
        optimas = sum(1 for c in coberturas if c >= 95)
        deficientes = sum(1 for c in coberturas if c < 60)
        
        stats_data = [
            ["Métrica", "Valor"],
            ["Total Consultas/Procedimientos", str(total_items)],
            ["Denominador Total", f"{total_denominador:,}"],
            ["Numerador Total", f"{total_numerador:,}"],
            ["Cobertura Global", f"{cobertura_global:.1f}%"],
            ["Mejor Cobertura", f"{mejor_cobertura:.1f}%"],
            ["Peor Cobertura", f"{peor_cobertura:.1f}%"],
            ["Consultas Óptimas (≥95%)", str(optimas)],
            ["Consultas Deficientes (<60%)", str(deficientes)],
        ]

        table = Table(stats_data, colWidths=[3.8 * inch, 2.2 * inch])
        table.setStyle(
            TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1890ff")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 10),
                ("FONTSIZE", (0, 1), (-1, -1), 9),
                ("BOTTOMPADDING", (0, 0), (-1, 0), 10),
                ("TOPPADDING", (0, 1), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 1), (-1, -1), 6),
                ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ])
        )

        return table

    @staticmethod
    def build_activities_table(items: List[Dict[str, Any]]) -> Table:
        """Construye tabla de consultas/procedimientos"""
        table_data = [["Consulta/Procedimiento", "Rango Edad", "Den", "Num", "% Cob", "Estado"]]

        for item in items:
            anual = item.get('anual', {})
            table_data.append([
                str(item.get("consulta_procedimiento", ""))[:35],
                str(item.get("rango_edad", ""))[:15],
                f"{anual.get('denominador', 0):,}",
                f"{anual.get('numerador', 0):,}",
                f"{anual.get('cobertura', 0):.1f}%",
                anual.get("semaforizacion", SEMAFORO_NA),
            ])

        table = Table(
            table_data,
            colWidths=[2.5 * inch, 1 * inch, 0.9 * inch, 0.9 * inch, 0.9 * inch, 1.3 * inch]
        )

        style = TableBuilder._get_activities_table_style(items)
        table.setStyle(TableStyle(style))

        return table

    @staticmethod
    def _get_activities_table_style(items: List[Dict[str, Any]]) -> List:
        """Genera estilo para tabla de actividades"""
        style = [
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1890ff")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 10),
            ("FONTSIZE", (0, 1), (-1, -1), 8),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 10),
            ("TOPPADDING", (0, 1), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 1), (-1, -1), 5),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]

        for i, item in enumerate(items, start=1):
            estado = item.get('anual', {}).get("semaforizacion", SEMAFORO_NA)
            color_hex = COLOR_MAP.get(estado, COLOR_MAP[SEMAFORO_NA])

            try:
                bg_color = colors.HexColor(color_hex)
                bg_color.alpha = 0.3
                style.append(("BACKGROUND", (5, i), (5, i), bg_color))
                style.append(("TEXTCOLOR", (5, i), (5, i), colors.HexColor(color_hex)))
                style.append(("FONTNAME", (5, i), (5, i), "Helvetica-Bold"))
            except Exception:
                pass

        return style

    @staticmethod
    def build_detailed_activity_table(item: Dict[str, Any]) -> Table:
        """Construye tabla detallada de una consulta/procedimiento"""
        table_data = [["Periodo", "Tipo", "Pob. Obj", "Den", "Num", "% Cob", "Estado"]]

        # Datos básicos
        pob_obj = item.get('poblacion_objeto', 0)
        
        # Trimestres
        for trim in ['T1', 'T2', 'T3', 'T4']:
            trim_data = item.get(trim, {})
            if trim_data.get('denominador', 0) > 0:
                table_data.append([
                    trim,
                    "Trimestre",
                    f"{trim_data.get('poblacion_objeto', 0):,}",
                    f"{trim_data.get('denominador', 0):,}",
                    f"{trim_data.get('numerador', 0):,}",
                    f"{trim_data.get('cobertura', 0):.1f}%",
                    trim_data.get('semaforizacion', SEMAFORO_NA),
                ])

        # Semestres
        for sem in ['S1', 'S2']:
            sem_data = item.get(sem, {})
            if sem_data.get('denominador', 0) > 0:
                table_data.append([
                    sem,
                    "Semestre",
                    f"{sem_data.get('poblacion_objeto', 0):,}",
                    f"{sem_data.get('denominador', 0):,}",
                    f"{sem_data.get('numerador', 0):,}",
                    f"{sem_data.get('cobertura', 0):.1f}%",
                    sem_data.get('semaforizacion', SEMAFORO_NA),
                ])

        # Anual
        anual = item.get('anual', {})
        if anual.get('denominador', 0) > 0:
            table_data.append([
                "Anual",
                "Año",
                f"{pob_obj:,}",
                f"{anual.get('denominador', 0):,}",
                f"{anual.get('numerador', 0):,}",
                f"{anual.get('cobertura', 0):.1f}%",
                anual.get('semaforizacion', SEMAFORO_NA),
            ])

        table = Table(
            table_data,
            colWidths=[0.9 * inch, 1 * inch, 1 * inch, 1 * inch, 1 * inch, 0.9 * inch, 1.3 * inch]
        )

        style = TableBuilder._get_detailed_table_style()
        table.setStyle(TableStyle(style))

        return table

    @staticmethod
    def _get_detailed_table_style() -> List:
        """Genera estilo para tabla detallada"""
        return [
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#52c41a")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 9),
            ("FONTSIZE", (0, 1), (-1, -1), 8),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("TOPPADDING", (0, 1), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 1), (-1, -1), 4),
        ]
