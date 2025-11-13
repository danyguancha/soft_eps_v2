# services/technical_note_services/report_service_aux/table_builders.py
from typing import List, Dict, Any
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.platypus import Table, TableStyle

from services.technical_note_services.pdf_exporter_aux.pdf_config import (
    COLOR_MAP,
    SEMAFORO_NA,
)


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
    def build_statistics_table(global_stats: Dict[str, Any]) -> Table:
        """Construye tabla de estadísticas globales"""
        stats_data = [
            ["Métrica", "Valor"],
            ["Total Actividades", str(global_stats.get("total_actividades", 0))],
            [
                "Denominador Global",
                f"{global_stats.get('total_denominador_global', 0):,}",
            ],
            ["Numerador Global", f"{global_stats.get('total_numerador_global', 0):,}"],
            [
                "Cobertura Global",
                f"{global_stats.get('cobertura_global_porcentaje', 0):.1f}%",
            ],
            ["Mejor Cobertura", f"{global_stats.get('mejor_cobertura', 0):.1f}%"],
            ["Peor Cobertura", f"{global_stats.get('peor_cobertura', 0):.1f}%"],
            [
                "Actividades Óptimas (≥90%)",
                str(global_stats.get("actividades_100_pct_cobertura", 0)),
            ],
            [
                "Actividades Deficientes (<60%)",
                str(global_stats.get("actividades_menos_50_pct_cobertura", 0)),
            ],
        ]

        table = Table(stats_data, colWidths=[3.8 * inch, 2.2 * inch])
        table.setStyle(
            TableStyle(
                [
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
                ]
            )
        )

        return table

    @staticmethod
    def build_activities_table(items: List[Dict[str, Any]]) -> Table:
        """Construye tabla de actividades"""
        table_data = [["Procedimiento", "Palabra", "Den", "Num", "% Cump", "Estado"]]

        for item in items:
            table_data.append(
                [
                    str(item.get("column", ""))[:45],
                    str(item.get("keyword", "")).upper(),
                    f"{item.get('denominador', 0):,}",
                    f"{item.get('numerador', 0):,}",
                    f"{item.get('cobertura_porcentaje', 0):.1f}%",
                    item.get("semaforizacion", SEMAFORO_NA),
                ]
            )

        table = Table(
            table_data,
            colWidths=[
                3.2 * inch,
                0.9 * inch,
                0.9 * inch,
                0.9 * inch,
                0.9 * inch,
                1.3 * inch,
            ],
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
            estado = item.get("semaforizacion", SEMAFORO_NA)
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
    def build_temporal_table(years_dict: Dict[str, Any]) -> Table:
        """Construye tabla temporal"""
        table_data = [["Periodo", "Tipo", "Den", "Num", "% Cump", "Estado"]]

        for year_str in sorted(years_dict.keys(), key=lambda y: int(y), reverse=True):
            year_info = years_dict[year_str]
            TableBuilder._add_year_and_months(table_data, year_str, year_info)

        table = Table(
            table_data,
            colWidths=[
                1.6 * inch,
                0.7 * inch,
                1 * inch,
                1 * inch,
                1 * inch,
                1.2 * inch,
            ],
        )

        style = TableBuilder._get_temporal_table_style(years_dict)
        table.setStyle(TableStyle(style))

        return table

    @staticmethod
    def _add_year_and_months(
        table_data: List, year_str: str, year_info: Dict[str, Any]
    ):
        """Agrega filas de año y meses"""
        year_den = year_info.get("total_den") or year_info.get("denominador") or 0
        year_num = year_info.get("total_num") or year_info.get("numerador") or 0
        year_pct = year_info.get("pct") or 0.0

        table_data.append(
            [
                year_str,
                "AÑO",
                f"{year_den:,}",
                f"{year_num:,}",
                f"{year_pct:.1f}%",
                year_info.get("semaforizacion", SEMAFORO_NA),
            ]
        )

        months_dict = year_info.get("months", {})
        sorted_months = sorted(months_dict.items(), key=lambda m: m[1].get("month", 0))

        for month_name, month_info in sorted_months:
            month_den = month_info.get("denominador") or month_info.get("den") or 0
            month_num = month_info.get("numerador") or month_info.get("num") or 0
            month_pct = month_info.get("pct") or 0.0

            table_data.append(
                [
                    month_name,
                    "MES",
                    f"{month_den:,}",
                    f"{month_num:,}",
                    f"{month_pct:.1f}%",
                    month_info.get("semaforizacion", SEMAFORO_NA),
                ]
            )

    @staticmethod
    def _get_temporal_table_style(years_dict: Dict[str, Any]) -> List:
        """Genera estilo para tabla temporal"""
        style = [
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

        row_idx = 1
        for year_str in sorted(years_dict.keys(), key=lambda y: int(y), reverse=True):
            style.append(
                ("BACKGROUND", (0, row_idx), (-1, row_idx), colors.HexColor("#e6f7ff"))
            )
            style.append(("FONTNAME", (0, row_idx), (1, row_idx), "Helvetica-Bold"))
            row_idx += 1

            months_count = len(years_dict[year_str].get("months", {}))
            row_idx += months_count

        return style
