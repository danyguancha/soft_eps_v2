import csv
import json
import os
import tempfile
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

# Importaciones para PDF
try:
    from reportlab.lib import colors
    from reportlab.lib.colors import HexColor
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import inch
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False
    print("⚠️ ReportLab no disponible. Instalar con: pip install reportlab")


class ReportExporter:
    """
    📤 SERVICIO PARA EXPORTAR REPORTES EN CSV Y PDF
    
    Funcionalidades:
    - Exportar CSV con separador punto y coma, codificación Latin1
    - Exportar PDF profesional con semaforización por colores
    - Incluir datos temporales y estadísticas globales
    - Personalización de contenido mediante archivo JSON
    - Múltiples formatos de salida
    """
    
    def __init__(self):
        self.config_path = "config/report_config.json"
        self.temp_files = {}
        self.ensure_config_exists()
    
    def ensure_config_exists(self):
        """Crear archivo de configuración si no existe"""
        try:
            if not os.path.exists(self.config_path):
                os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
                
                default_config = {
                    "pdf_config": {
                        "title": "Reporte de Indicadores de Salud",
                        "subtitle": "Análisis de Cobertura por Edad y Procedimientos",
                        "organization": "Sistema de Salud - Análisis Epidemiológico",
                        "description": "Este reporte presenta el análisis detallado de los indicadores de salud segmentados por grupos de edad específicos, incluyendo numeradores, denominadores y porcentajes de cobertura con semaforización por desempeño.",
                        "methodology": "La metodología utilizada se basa en la lógica de Excel donde el denominador mensual incluye tanto los registros del mes específico como los registros vacíos de la población elegible. El denominador anual se calcula como la suma de registros de todo el año más los registros vacíos contados una sola vez.",
                        "interpretation": {
                            "Óptimo": "Desempeño excelente (≥90%). Cumplimiento óptimo de los indicadores.",
                            "Aceptable": "Buen desempeño (75-89%). Cumplimiento aceptable con oportunidades de mejora.",
                            "Deficiente": "Desempeño bajo (60-74%). Requiere intervención inmediata.",
                            "Muy Deficiente": "Desempeño muy bajo (<60%). Requiere intervención urgente y prioritaria.",
                            "NA": "Sin datos disponibles para el análisis."
                        },
                        "footer_text": "Reporte generado automáticamente por el Sistema de Análisis de Indicadores de Salud",
                        "contact_info": "Para más información contacte al área de epidemiología",
                        "max_activities_table": 30,
                        "colors": {
                            "header": "#2E86AB",
                            "row_alt1": "#F8F9FA",
                            "row_alt2": "#FFFFFF",
                            "optimo": "#4CAF50",
                            "aceptable": "#FF9800",
                            "deficiente": "#FF5722",
                            "muy_deficiente": "#F44336",
                            "na": "#9E9E9E"
                        }
                    },
                    "csv_config": {
                        "separator": ";",
                        "encoding": "latin1",
                        "include_temporal": True,
                        "include_semaforization": True,
                        "decimal_separator": ",",
                        "thousands_separator": "."
                    }
                }
                
                with open(self.config_path, 'w', encoding='utf-8') as f:
                    json.dump(default_config, f, ensure_ascii=False, indent=2)
                    
                print(f"✅ Archivo de configuración creado: {self.config_path}")
        except Exception as e:
            print(f"⚠️ Error creando configuración: {e}")
    
    def load_config(self) -> Dict[str, Any]:
        """Cargar configuración desde JSON"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"❌ Error cargando configuración: {e}")
            return {
                "pdf_config": {
                    "title": "Reporte de Indicadores",
                    "max_activities_table": 30,
                    "colors": {}
                },
                "csv_config": {"separator": ";", "encoding": "latin1", "include_temporal": True}
            }
    
    def export_to_csv(self, report_data: Dict[str, Any], output_path: str, include_temporal: bool = True) -> Dict[str, str]:
        """📄 EXPORTAR REPORTE A CSV - Separador: ; - Codificación: Latin1"""
        try:
            config = self.load_config()
            csv_config = config.get('csv_config', {})
            
            separator = csv_config.get('separator', ';')
            encoding = csv_config.get('encoding', 'latin1')
            
            exported_files = {}
            
            # ✅ ARCHIVO PRINCIPAL CON ACTIVIDADES
            main_file = self._export_main_activities_csv(
                report_data, output_path.replace('.csv', '.csv'), 
                separator, encoding
            )
            if main_file:
                exported_files['actividades'] = main_file
            
            # ✅ ARCHIVO DE ESTADÍSTICAS GLOBALES
            stats_file = self._export_global_statistics_csv(
                report_data, output_path.replace('.csv', 'csv'),
                separator, encoding
            )
            if stats_file:
                exported_files['estadisticas'] = stats_file
            
            # ✅ ARCHIVO TEMPORAL SI ESTÁ HABILITADO
            if include_temporal and csv_config.get('include_temporal', True):
                temporal_file = self._export_temporal_csv(
                    report_data, output_path.replace('.csv', '.csv'), 
                    separator, encoding
                )
                if temporal_file:
                    exported_files['temporal'] = temporal_file
            
            # ✅ ARCHIVO DE RESUMEN TOTALES
            totals_file = self._export_totals_by_keyword_csv(
                report_data, output_path.replace('.csv', '.csv'),
                separator, encoding  
            )
            if totals_file:
                exported_files['totales'] = totals_file
            
            print(f"✅ Exportación CSV completada:")
            for key, file_path in exported_files.items():
                print(f"   📄 {key.title()}: {file_path}")
            
            return exported_files
            
        except Exception as e:
            print(f"❌ Error exportando CSV: {e}")
            raise Exception(f"Error en exportación CSV: {e}")
    
    def _export_main_activities_csv(self, report_data: Dict[str, Any], output_path: str, separator: str, encoding: str) -> str:
        """Exportar actividades principales a CSV"""
        try:
            items = report_data.get('items', [])
            if not items:
                return ""
            
            # Headers principales con información completa
            headers = [
                'Procedimiento/Consulta', 'Palabra_Clave', 'Rango_Edad',
                'Denominador', 'Numerador', 'Sin_Datos', 'Porcentaje_Cobertura',
                'Semaforizacion', 'Color', 'Descripcion', 'Metodo', 
                'Corte_Fecha', 'Min_Age', 'Max_Age', 'Unit'
            ]
            
            # Preparar datos
            rows = []
            for item in items:
                # Extraer información de rango de edad
                age_info = item.get('age_range_extracted', {})
                
                row = [
                    str(item.get('column', '')).replace(separator, ','),  # Escapar separador
                    str(item.get('keyword', '')),
                    str(item.get('age_range', '')),
                    str(item.get('denominador', 0)),
                    str(item.get('numerador', 0)),
                    str(item.get('sin_datos', 0)),
                    str(round(item.get('cobertura_porcentaje', 0.0), 2)).replace('.', ','),  # Decimal español
                    str(item.get('semaforizacion', '')),
                    str(item.get('color', '')),
                    str(item.get('descripcion', '')).replace(separator, ','),
                    str(item.get('metodo', '')),
                    str(item.get('corte_fecha', '')),
                    str(age_info.get('min_age', '')),
                    str(age_info.get('max_age', '')),
                    str(age_info.get('unit', ''))
                ]
                rows.append(row)
            
            # Escribir archivo
            with open(output_path, 'w', newline='', encoding=encoding, errors='replace') as f:
                writer = csv.writer(f, delimiter=separator)
                writer.writerow(headers)
                writer.writerows(rows)
            
            print(f"✅ Archivo de actividades exportado: {len(rows)} registros")
            return output_path
            
        except Exception as e:
            print(f"❌ Error exportando actividades CSV: {e}")
            return ""
    
    def _export_global_statistics_csv(self, report_data: Dict[str, Any], output_path: str, separator: str, encoding: str) -> str:
        """Exportar estadísticas globales a CSV"""
        try:
            global_stats = report_data.get('global_statistics', {})
            if not global_stats:
                return ""
            
            # Headers de estadísticas
            headers = ['Metrica', 'Valor', 'Descripcion']
            
            rows = [
                ['Total_Actividades', str(global_stats.get('total_actividades', 0)), 'Número total de actividades analizadas'],
                ['Denominador_Global', str(global_stats.get('total_denominador_global', 0)), 'Suma total de denominadores'],
                ['Numerador_Global', str(global_stats.get('total_numerador_global', 0)), 'Suma total de numeradores'],
                ['Sin_Datos_Global', str(global_stats.get('total_sin_datos_global', 0)), 'Total de registros sin datos'],
                ['Cobertura_Global_Porcentaje', str(round(global_stats.get('cobertura_global_porcentaje', 0.0), 2)).replace('.', ','), 'Porcentaje de cobertura general'],
                ['Actividades_100_Pct', str(global_stats.get('actividades_100_pct_cobertura', 0)), 'Actividades con 100% de cobertura'],
                ['Actividades_Menos_50_Pct', str(global_stats.get('actividades_menos_50_pct_cobertura', 0)), 'Actividades con menos del 50% de cobertura'],
                ['Mejor_Cobertura', str(round(global_stats.get('mejor_cobertura', 0.0), 2)).replace('.', ','), 'Mejor porcentaje de cobertura encontrado'],
                ['Peor_Cobertura', str(round(global_stats.get('peor_cobertura', 0.0), 2)).replace('.', ','), 'Peor porcentaje de cobertura encontrado'],
                ['Cobertura_Promedio', str(round(global_stats.get('cobertura_promedio', 0.0), 2)).replace('.', ','), 'Promedio de cobertura de todas las actividades']
            ]
            
            # Información adicional del reporte
            rows.extend([
                ['Corte_Fecha', str(report_data.get('corte_fecha', '')), 'Fecha de corte utilizada para el análisis'],
                ['Metodo', str(report_data.get('metodo', '')), 'Metodología utilizada para el cálculo'],
                ['Version', str(report_data.get('version', '')), 'Versión del sistema utilizada'],
                ['Engine', str(report_data.get('engine', '')), 'Motor de procesamiento utilizado'],
                ['Ultra_Fast', str(report_data.get('ultra_fast', False)), 'Modo de procesamiento rápido activado'],
                ['Temporal_Columns', str(report_data.get('temporal_columns', 0)), 'Número de columnas con análisis temporal']
            ])
            
            # Escribir archivo
            with open(output_path, 'w', newline='', encoding=encoding, errors='replace') as f:
                writer = csv.writer(f, delimiter=separator)
                writer.writerow(headers)
                writer.writerows(rows)
            
            print(f"✅ Archivo de estadísticas exportado: {len(rows)} métricas")
            return output_path
            
        except Exception as e:
            print(f"❌ Error exportando estadísticas CSV: {e}")
            return ""
    
    def _export_temporal_csv(self, report_data: Dict[str, Any], output_path: str, separator: str, encoding: str) -> str:
        """Exportar datos temporales a CSV separado"""
        try:
            temporal_data = report_data.get('temporal_data', {})
            
            if not temporal_data:
                return ""
            
            # Headers temporales completos
            headers = [
                'Procedimiento/Consulta', 'Palabra_Clave', 'Rango_Edad', 
                'Año', 'Mes', 'Mes_Numero', 'Periodo', 'Tipo_Periodo',
                'Denominador', 'Numerador', 'Sin_Datos', 'Porcentaje',
                'Semaforizacion', 'Color', 'Color_Name', 'Descripcion'
            ]
            
            rows = []
            
            for key, data in temporal_data.items():
                column = str(data.get('column', '')).replace(separator, ',')
                keyword = str(data.get('keyword', ''))
                age_range = str(data.get('age_range', ''))
                
                years = data.get('years', {})
                for year_str, year_data in years.items():
                    # Fila del año
                    year_row = [
                        column, keyword, age_range,
                        str(year_str), '', '', str(year_str), 'AÑO',
                        str(year_data.get('total_den', 0)),
                        str(year_data.get('total_num', 0)),
                        str(year_data.get('sin_datos', 0)),
                        str(round(year_data.get('pct', 0.0), 2)).replace('.', ','),
                        str(year_data.get('semaforizacion', '')),
                        str(year_data.get('color', '')),
                        str(year_data.get('color_name', '')),
                        str(year_data.get('descripcion', '')).replace(separator, ',')
                    ]
                    rows.append(year_row)
                    
                    # Filas de los meses
                    months = year_data.get('months', {})
                    for month_name, month_data in months.items():
                        month_row = [
                            column, keyword, age_range,
                            str(year_str), str(month_name), str(month_data.get('month', '')), 
                            f"{month_name} {year_str}", 'MES',
                            str(month_data.get('denominador', month_data.get('den', 0))),
                            str(month_data.get('numerador', month_data.get('num', 0))),
                            str(month_data.get('sin_datos', 0)),
                            str(round(month_data.get('pct', month_data.get('cobertura_porcentaje', 0.0)), 2)).replace('.', ','),
                            str(month_data.get('semaforizacion', '')),
                            str(month_data.get('color', '')),
                            str(month_data.get('color_name', '')),
                            str(month_data.get('descripcion', '')).replace(separator, ',')
                        ]
                        rows.append(month_row)
            
            if not rows:
                return ""
            
            # Escribir archivo temporal
            with open(output_path, 'w', newline='', encoding=encoding, errors='replace') as f:
                writer = csv.writer(f, delimiter=separator)
                writer.writerow(headers)
                writer.writerows(rows)
            
            print(f"✅ Archivo temporal exportado: {len(rows)} registros")
            return output_path
            
        except Exception as e:
            print(f"❌ Error exportando CSV temporal: {e}")
            return ""
    
    def _export_totals_by_keyword_csv(self, report_data: Dict[str, Any], output_path: str, separator: str, encoding: str) -> str:
        """Exportar totales por palabra clave a CSV"""
        try:
            totals_by_keyword = report_data.get('totals_by_keyword', {})
            
            if not totals_by_keyword:
                return ""
            
            # Headers para totales
            headers = [
                'Palabra_Clave', 'Count_Total', 'Numerador_Total', 
                'Denominador_Total', 'Actividades_Count', 'Cobertura_Promedio'
            ]
            
            rows = []
            for keyword, totals in totals_by_keyword.items():
                row = [
                    str(keyword),
                    str(totals.get('count', 0)),
                    str(totals.get('numerador', 0)),
                    str(totals.get('denominador', 0)),
                    str(totals.get('actividades', 0)),
                    str(round(totals.get('cobertura_promedio', 0.0), 2)).replace('.', ',')
                ]
                rows.append(row)
            
            if not rows:
                return ""
            
            # Escribir archivo
            with open(output_path, 'w', newline='', encoding=encoding, errors='replace') as f:
                writer = csv.writer(f, delimiter=separator)
                writer.writerow(headers)
                writer.writerows(rows)
            
            print(f"✅ Archivo de totales por palabra clave exportado: {len(rows)} registros")
            return output_path
            
        except Exception as e:
            print(f"❌ Error exportando totales CSV: {e}")
            return ""
    
    def export_to_pdf(self, report_data: Dict[str, Any], output_path: str, include_temporal: bool = True) -> str:
        """📄 EXPORTAR REPORTE A PDF - Configuración desde JSON"""
        if not REPORTLAB_AVAILABLE:
            print("❌ ReportLab no disponible. Generando PDF simple.")
            return self._export_simple_pdf(report_data, output_path)
            
        try:
            config = self.load_config()
            pdf_config = config.get('pdf_config', {})
            
            # Crear documento PDF profesional
            doc = SimpleDocTemplate(
                output_path, 
                pagesize=A4, 
                rightMargin=50, leftMargin=50,
                topMargin=50, bottomMargin=50
            )
            
            # Obtener estilos
            styles = getSampleStyleSheet()
            story = []
            
            # ✅ TÍTULO PRINCIPAL
            title_text = pdf_config.get('title', 'Reporte de Indicadores de Nota Técnica')
            title = Paragraph(f"<b>{title_text}</b>", styles['Title'])
            story.append(title)
            story.append(Spacer(1, 20))
            
            # ✅ SUBTÍTULO
            if pdf_config.get('subtitle'):
                subtitle = Paragraph(pdf_config['subtitle'], styles['Heading2'])
                story.append(subtitle)
                story.append(Spacer(1, 15))
            
            # ✅ ORGANIZACIÓN
            if pdf_config.get('organization'):
                org = Paragraph(pdf_config['organization'], styles['Normal'])
                story.append(org)
                story.append(Spacer(1, 20))
            
            # ✅ INFORMACIÓN DEL REPORTE
            self._add_report_info(story, report_data, styles)
            
            # ✅ DESCRIPCIÓN Y METODOLOGÍA
            self._add_description_and_methodology(story, pdf_config, styles)
            
            # ✅ ESTADÍSTICAS GLOBALES
            self._add_global_statistics_table(story, report_data, pdf_config, styles)
            
            # ✅ TABLA PRINCIPAL DE ACTIVIDADES
            self._add_activities_table(story, report_data, pdf_config, styles)
            
            # ✅ INTERPRETACIÓN DE SEMAFORIZACIÓN
            self._add_semaforization_guide(story, pdf_config, styles)
            
            # ✅ PIE DE PÁGINA
            self._add_footer_info(story, pdf_config, styles)
            
            # Construir el documento
            doc.build(story)
            
            print(f"✅ Exportación PDF completada: {output_path}")
            return output_path
            
        except Exception as e:
            print(f"❌ Error exportando PDF avanzado: {e}")
            # Fallback a PDF simple
            return self._export_simple_pdf(report_data, output_path)
    
    def _add_report_info(self, story: List, report_data: Dict[str, Any], styles):
        """Agregar información básica del reporte"""
        fecha_generacion = datetime.now().strftime("%d/%m/%Y %H:%M")
        
        info_data = [
            ["Fecha de generación: ", fecha_generacion],
            ["Fecha de corte: ", report_data.get('corte_fecha', 'N/A')],
            ["Total de actividades: ", f"{len(report_data.get('items', []))} actividades"],
        ]
        
        # Filtros geográficos
        geographic_filters = report_data.get('geographic_filters', {})
        if geographic_filters:
            for key, value in geographic_filters.items():
                if value and value not in ['', 'Todos', None]:
                    info_data.append([f"<b>{key.title()}:</b>", str(value)])
        
        # Filtros de palabras clave
        rules = report_data.get('rules', {})
        keywords = rules.get('keywords', [])
        if keywords:
            info_data.append(["<b>Palabras clave:</b>", ", ".join(keywords)])
        
        info_table = Table(info_data, colWidths=[2*inch, 4*inch])
        info_table.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
        ]))
        
        story.append(info_table)
        story.append(Spacer(1, 20))
    
    def _add_description_and_methodology(self, story: List, pdf_config: Dict, styles):
        """Agregar descripción y metodología"""
        if pdf_config.get('description'):
            story.append(Paragraph("<b>📋 Descripción del Análisis</b>", styles['Heading3']))
            story.append(Paragraph(pdf_config['description'], styles['Normal']))
            story.append(Spacer(1, 15))
        
        if pdf_config.get('methodology'):
            story.append(Paragraph("<b>🔬 Metodología</b>", styles['Heading3']))
            story.append(Paragraph(pdf_config['methodology'], styles['Normal']))
            story.append(Spacer(1, 20))
    
    def _add_global_statistics_table(self, story: List, report_data: Dict[str, Any], pdf_config: Dict, styles):
        """Agregar tabla de estadísticas globales"""
        global_stats = report_data.get('global_statistics', {})
        if not global_stats:
            return
        
        story.append(Paragraph("<b>📊 Estadísticas Globales</b>", styles['Heading2']))
        story.append(Spacer(1, 10))
        
        # Preparar datos de estadísticas
        stats_data = [
            ["Métrica", "Valor", "Descripción"],
        ]
        
        metrics = [
            ("Denominador Total", global_stats.get('total_denominador_global', 0), "Total de niños/as en el programa"),
            ("Numerador Total", global_stats.get('total_numerador_global', 0), "Total de actividades cumplidas"),
            ("Cobertura Global", f"{global_stats.get('cobertura_global_porcentaje', 0):.1f}%", "Porcentaje de cumplimiento general"),
            ("Actividades Óptimas", global_stats.get('actividades_100_pct_cobertura', 0), "Actividades con 100% de cobertura"),
            ("Actividades Deficientes", global_stats.get('actividades_menos_50_pct_cobertura', 0), "Actividades con menos del 50%"),
            ("Rango de Cobertura", f"{global_stats.get('peor_cobertura', 0):.1f}% - {global_stats.get('mejor_cobertura', 0):.1f}%", "Desde la peor hasta la mejor")
        ]
        
        for metric, value, description in metrics:
            if isinstance(value, (int, float)) and value > 999:
                value_str = f"{value:,}".replace(',', '.')
            else:
                value_str = str(value)
            
            stats_data.append([metric, value_str, description])
        
        stats_table = Table(stats_data, colWidths=[2*inch, 1.5*inch, 2.5*inch])
        stats_table.setStyle(TableStyle([
            # Header
            ('BACKGROUND', (0, 0), (-1, 0), HexColor(pdf_config.get('colors', {}).get('header', '#4472C4'))),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            
            # Data rows
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [HexColor('#F2F2F2'), HexColor('#FFFFFF')]),
            ('GRID', (0, 0), (-1, -1), 1, colors.lightgrey),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
            ('RIGHTPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ]))
        
        story.append(stats_table)
        story.append(Spacer(1, 20))
    
    def _add_activities_table(self, story: List, report_data: Dict[str, Any], pdf_config: Dict, styles):
        """Agregar tabla principal de actividades"""
        items = report_data.get('items', [])
        if not items:
            return
        
        story.append(Paragraph("<b>📋 Detalle de Actividades por Rango de Edad</b>", styles['Heading2']))
        story.append(Spacer(1, 10))
        
        # Preparar headers de tabla
        table_data = [
            ["Actividad", "Palabra", "Rango Edad", 
             "Den.", "Num.", "Cob.%", "Estado"]
        ]
        
        # Ordenar items por cobertura (descendente)
        max_activities = pdf_config.get('max_activities_table', 30)
        sorted_items = sorted(items, key=lambda x: x.get('cobertura_porcentaje', 0), reverse=True)[:max_activities]
        
        colors_config = pdf_config.get('colors', {})
        
        for item in sorted_items:
            # Preparar datos de la fila
            actividad = item.get('column', 'N/A')
            if len(actividad) > 35:
                actividad = actividad[:32] + "..."
            
            palabra_clave = item.get('keyword', 'N/A').upper()[:8]
            rango_edad = item.get('age_range', 'N/A')[:15]
            
            denominador = item.get('denominador', 0)
            numerador = item.get('numerador', 0)
            cobertura = item.get('cobertura_porcentaje', 0)
            
            # Estado de semaforización
            estado = item.get('semaforizacion', 'NA')
            if estado == 'NA':
                if cobertura >= 90:
                    estado = "Óptimo"
                elif cobertura >= 70:
                    estado = "Aceptable"
                elif cobertura >= 50:
                    estado = "Deficiente"
                else:
                    estado = "Muy Def."
            
            table_data.append([
                actividad,
                palabra_clave,
                rango_edad,
                f"{denominador:,}".replace(',', '.') if denominador > 999 else str(denominador),
                f"{numerador:,}".replace(',', '.') if numerador > 999 else str(numerador),
                f"{cobertura:.1f}%",
                estado
            ])
        
        # Crear tabla con estilo
        activities_table = Table(table_data, 
                               colWidths=[2.2*inch, 0.7*inch, 1*inch, 
                                         0.6*inch, 0.6*inch, 0.5*inch, 0.7*inch])
        
        # Aplicar estilos base
        table_style = [
            # Header
            ('BACKGROUND', (0, 0), (-1, 0), HexColor(colors_config.get('header', '#2E86AB'))),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 8),
            
            # Data rows
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 7),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [
                HexColor(colors_config.get('row_alt1', '#F8F9FA')), 
                HexColor(colors_config.get('row_alt2', '#FFFFFF'))
            ]),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.lightgrey),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('LEFTPADDING', (0, 0), (-1, -1), 4),
            ('RIGHTPADDING', (0, 0), (-1, -1), 4),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            
            # Alinear columnas numéricas
            ('ALIGN', (3, 1), (5, -1), 'RIGHT'),  # Den, Num, Cob
        ]
        
        # Agregar colores por semaforización
        for i, row in enumerate(table_data[1:], 1):
            try:
                cobertura_text = row[5]  # Columna de cobertura
                cobertura_val = float(cobertura_text.replace('%', ''))
                
                # Colores según configuración
                if cobertura_val >= 90:
                    color = HexColor(colors_config.get('optimo', '#4CAF50'))
                elif cobertura_val >= 70:
                    color = HexColor(colors_config.get('aceptable', '#FF9800'))
                elif cobertura_val >= 50:
                    color = HexColor(colors_config.get('deficiente', '#FF5722'))
                else:
                    color = HexColor(colors_config.get('muy_deficiente', '#F44336'))
                
                # Aplicar color al estado
                table_style.append(('BACKGROUND', (6, i), (6, i), color))
                table_style.append(('TEXTCOLOR', (6, i), (6, i), colors.white))
                
                # Color suave en porcentaje
                bg_color = HexColor(f"{color.hexval()}100")  # 100% transparencia
                table_style.append(('BACKGROUND', (5, i), (5, i), bg_color))
                
            except (ValueError, IndexError):
                continue
        
        activities_table.setStyle(TableStyle(table_style))
        story.append(activities_table)
        
        # Nota si se limitaron las actividades
        if len(items) > max_activities:
            note = Paragraph(
                f"<i>Nota: Se muestran las {max_activities} actividades con mayor cobertura de un total de {len(items)}.</i>", 
                styles['Normal']
            )
            story.append(Spacer(1, 10))
            story.append(note)
        
        story.append(Spacer(1, 20))
    
    def _add_semaforization_guide(self, story: List, pdf_config: Dict, styles):
        """Agregar guía de interpretación de semaforización"""
        interpretation = pdf_config.get('interpretation', {})
        if not interpretation:
            return
        
        story.append(Paragraph("<b>🚦 Guía de Interpretación - Semaforización</b>", styles['Heading3']))
        story.append(Spacer(1, 10))
        
        # Crear tabla de interpretación
        interp_data = [["Estado", "Descripción"]]
        
        for estado, descripcion in interpretation.items():
            interp_data.append([estado, descripcion])
        
        interp_table = Table(interp_data, colWidths=[1.5*inch, 4.5*inch])
        interp_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), HexColor('#2E86AB')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.lightgrey),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
            ('RIGHTPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ]))
        
        story.append(interp_table)
        story.append(Spacer(1, 20))
    
    def _add_footer_info(self, story: List, pdf_config: Dict, styles):
        """Agregar información del pie de página"""
        footer_info = f"""
        <b>📈 Información Adicional:</b><br/>
        • Los datos presentados corresponden al análisis automático del sistema.<br/>
        • La cobertura se calcula como: (Numerador ÷ Denominador) × 100.<br/>
        • Denominador: Población elegible que debería recibir cada actividad según su edad.<br/>
        • Numerador: Población que efectivamente recibió la actividad registrada.<br/>
        • La semaforización se basa en umbrales de desempeño predefinidos.<br/><br/>
        
        <b>🏥 {pdf_config.get('footer_text', 'Reporte generado automáticamente')}</b><br/>
        {pdf_config.get('contact_info', '')}<br/>
        <i>Fecha de generación: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</i>
        """
        
        footer = Paragraph(footer_info, styles['Normal'])
        story.append(footer)
    
    def _export_simple_pdf(self, report_data: Dict[str, Any], output_path: str) -> str:
        """Generar PDF simple como fallback cuando ReportLab no está disponible"""
        try:
            # Crear archivo de texto con extensión PDF para compatibilidad
            with open(output_path.replace('.pdf', '.txt'), 'w', encoding='utf-8') as f:
                f.write("REPORTE DE INDICADORES DE SALUD\n")
                f.write("="*50 + "\n\n")
                
                # Información básica
                f.write(f"Fecha de generación: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
                f.write(f"Fecha de corte: {report_data.get('corte_fecha', 'N/A')}\n")
                f.write(f"Total de actividades: {len(report_data.get('items', []))}\n\n")
                
                # Estadísticas globales
                global_stats = report_data.get('global_statistics', {})
                if global_stats:
                    f.write("ESTADÍSTICAS GLOBALES:\n")
                    f.write("-" * 25 + "\n")
                    f.write(f"Denominador total: {global_stats.get('total_denominador_global', 0):,}\n")
                    f.write(f"Numerador total: {global_stats.get('total_numerador_global', 0):,}\n")
                    f.write(f"Cobertura global: {global_stats.get('cobertura_global_porcentaje', 0):.1f}%\n")
                    f.write(f"Actividades óptimas: {global_stats.get('actividades_100_pct_cobertura', 0)}\n")
                    f.write(f"Actividades deficientes: {global_stats.get('actividades_menos_50_pct_cobertura', 0)}\n\n")
                
                # Detalle de actividades
                f.write("DETALLE DE ACTIVIDADES:\n")
                f.write("-" * 25 + "\n")
                
                items = sorted(report_data.get('items', []), 
                             key=lambda x: x.get('cobertura_porcentaje', 0), reverse=True)
                
                for i, item in enumerate(items[:30], 1):
                    f.write(f"{i:2d}. {item.get('column', 'N/A')[:50]}\n")
                    f.write(f"    Palabra: {item.get('keyword', 'N/A')} | Edad: {item.get('age_range', 'N/A')}\n")
                    f.write(f"    Den: {item.get('denominador', 0):,} | Num: {item.get('numerador', 0):,} | Cob: {item.get('cobertura_porcentaje', 0):.1f}%\n")
                    f.write(f"    Estado: {item.get('semaforizacion', 'N/A')}\n\n")
                
                f.write("\n" + "="*50 + "\n")
                f.write("Reporte generado por el Sistema de Análisis de Indicadores de Salud\n")
            
            print(f"✅ PDF simple (texto) generado: {output_path.replace('.pdf', '.txt')}")
            return output_path.replace('.pdf', '.txt')
            
        except Exception as e:
            print(f"❌ Error generando PDF simple: {e}")
            return ""
    
    def export_report(self, report_data: Dict[str, Any], base_filename: str, 
                    export_csv: bool = True, export_pdf: bool = True, 
                    include_temporal: bool = True) -> Dict[str, Any]:
        """📤 MÉTODO PRINCIPAL DE EXPORTACIÓN - CORREGIDO"""
        try:
            results = {
                "success": False,
                "files": {},
                "download_links": {},
                "message": "",
                "execution_time_seconds": 0
            }
            
            start_time = datetime.now()
            timestamp = start_time.strftime("%Y%m%d_%H%M%S")
            
            # Crear directorio temporal
            temp_dir = tempfile.mkdtemp(prefix="report_export_")
            print(f"📁 Directorio temporal creado: {temp_dir}")
            
            exported_files = {}
            
            # ✅ EXPORTAR CSV CORREGIDO
            if export_csv:
                try:
                    csv_base_path = os.path.join(temp_dir, f"{base_filename}_{timestamp}")
                    csv_files = self.export_to_csv(report_data, f"{csv_base_path}.csv", include_temporal)
                    
                    for csv_type, csv_path in csv_files.items():
                        file_id = str(uuid.uuid4())
                        
                        # ✅ CORRECCIÓN: Formato correcto del nombre
                        original_name = f"{base_filename}_{timestamp}_{csv_type}.csv"
                        
                        self.temp_files[file_id] = {
                            'file_path': csv_path,
                            'original_name': original_name,  # ✅ Extensión .csv correcta
                            'format': 'csv',
                            'created_at': datetime.now()
                        }
                        exported_files[f'csv_{csv_type}'] = csv_path
                        results['download_links'][f'csv_{csv_type}'] = f"/technical-note/reports/download/{file_id}"
                    
                except Exception as e:
                    print(f"❌ Error en exportación CSV: {e}")
            
            # ✅ EXPORTAR PDF (sin cambios)
            if export_pdf:
                try:
                    pdf_path = os.path.join(temp_dir, f"{base_filename}_{timestamp}.pdf")
                    pdf_file = self.export_to_pdf(report_data, pdf_path, include_temporal)
                    
                    if pdf_file:
                        file_id = str(uuid.uuid4())
                        self.temp_files[file_id] = {
                            'file_path': pdf_file,
                            'original_name': f"{base_filename}_{timestamp}.pdf",  # ✅ Ya estaba correcto
                            'format': 'pdf',
                            'created_at': datetime.now()
                        }
                        exported_files['pdf'] = pdf_file
                        results['download_links']['pdf'] = f"/technical-note/reports/download/{file_id}"
                    
                except Exception as e:
                    print(f"❌ Error en exportación PDF: {e}")
            
            # Actualizar resultados
            execution_time = (datetime.now() - start_time).total_seconds()
            
            results.update({
                "success": len(exported_files) > 0,
                "files": exported_files,
                "message": f"Reporte exportado exitosamente con {len(exported_files)} archivo(s)" if exported_files else "No se pudieron generar archivos",
                "execution_time_seconds": execution_time
            })
            
            print(f"✅ Exportación completada en {execution_time:.2f} segundos")
            print(f"📄 Archivos generados: {list(exported_files.keys())}")
            
            return results
            
        except Exception as e:
            print(f"❌ Error en exportación general: {e}")
            return {
                "success": False,
                "files": {},
                "download_links": {},
                "message": f"Error exportando reporte: {str(e)}",
                "execution_time_seconds": 0
            }

    
    def get_temp_file(self, file_id: str) -> Optional[Dict[str, Any]]:
        """Obtener información de archivo temporal por ID"""
        return self.temp_files.get(file_id)
    
    def cleanup_temp_file(self, file_id: str) -> bool:
        """Limpiar archivo temporal específico"""
        try:
            if file_id in self.temp_files:
                file_info = self.temp_files[file_id]
                file_path = file_info['file_path']
                
                if os.path.exists(file_path):
                    os.remove(file_path)
                    print(f"🧹 Archivo temporal eliminado: {file_path}")
                
                del self.temp_files[file_id]
                return True
            return False
        except Exception as e:
            print(f"❌ Error limpiando archivo temporal {file_id}: {e}")
            return False
    
    def cleanup_old_temp_files(self, max_age_minutes: int = 30):
        """Limpiar archivos temporales antiguos"""
        try:
            current_time = datetime.now()
            to_remove = []
            
            for file_id, file_info in self.temp_files.items():
                age = (current_time - file_info['created_at']).total_seconds() / 60
                if age > max_age_minutes:
                    to_remove.append(file_id)
            
            for file_id in to_remove:
                self.cleanup_temp_file(file_id)
            
            if to_remove:
                print(f"🧹 Limpiados {len(to_remove)} archivos temporales antiguos")
            
        except Exception as e:
            print(f"❌ Error en limpieza automática: {e}")




