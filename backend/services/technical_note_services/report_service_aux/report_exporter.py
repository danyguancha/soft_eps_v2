# services/technical_note_services/report_service_aux/report_exporter.py
import csv
import json
import os
import tempfile
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

# Importaciones para PDF
try:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
    from reportlab.lib.enums import TA_CENTER, TA_LEFT
    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False
    print("⚠️ ReportLab no disponible. Instalar con: pip install reportlab")


class ReportExporter:
    """
    📤 SERVICIO SIMPLIFICADO PARA EXPORTAR REPORTES
    - CSV Temporal con jerarquía (ACTIVIDAD → AÑO → MES)
    - PDF profesional con semaforización
    """
    
    def __init__(self):
        self.temp_dir = None
        self.temp_files_registry: Dict[str, Dict[str, Any]] = {}
    
    def _ensure_temp_dir(self) -> str:
        """Crea directorio temporal si no existe"""
        if not self.temp_dir or not os.path.exists(self.temp_dir):
            self.temp_dir = tempfile.mkdtemp(prefix="report_export_")
            print(f"📁 Directorio temporal: {self.temp_dir}")
        return self.temp_dir
    
    def export_report(
        self,
        report_data: Dict[str, Any],
        base_filename: str = "reporte",
        export_csv: bool = True,
        export_pdf: bool = False,
        include_temporal: bool = True
    ) -> Dict[str, Any]:
        """
        ✅ EXPORTA REPORTE EN CSV TEMPORAL Y/O PDF
        """
        try:
            start_time = datetime.now()
            
            print(f"\n🚀 ========== EXPORTACIÓN ==========")
            print(f"📋 Archivo: {base_filename}")
            print(f"📊 CSV: {export_csv}, PDF: {export_pdf}")
            print(f"⏰ Temporal: {include_temporal}")
            
            temp_dir = self._ensure_temp_dir()
            
            files = {}
            download_links = {}
            
            # ✅ EXPORTAR CSV TEMPORAL
            if export_csv:
                csv_path = self._export_temporal_csv(
                    report_data=report_data,
                    output_path=os.path.join(temp_dir, f"{base_filename}_temporal.csv"),
                    separator=';',
                    encoding='latin1'
                )
                
                if csv_path and os.path.exists(csv_path):
                    file_id = str(uuid.uuid4())
                    self.temp_files_registry[file_id] = {
                        'file_path': csv_path,
                        'original_name': f"{base_filename}_temporal.csv",
                        'created_at': datetime.now()
                    }
                    files['csv_temporal'] = csv_path
                    download_links['csv_temporal'] = f"/technical-note/reports/download/{file_id}"
                    print(f"✅ CSV Temporal generado: {csv_path}")
                else:
                    print(f"⚠️ CSV Temporal no generado")
            
            # ✅ EXPORTAR PDF
            if export_pdf and REPORTLAB_AVAILABLE:
                pdf_path = self._export_pdf(
                    report_data=report_data,
                    output_path=os.path.join(temp_dir, f"{base_filename}.pdf"),
                    include_temporal=include_temporal
                )
                
                if pdf_path and os.path.exists(pdf_path):
                    file_id = str(uuid.uuid4())
                    self.temp_files_registry[file_id] = {
                        'file_path': pdf_path,
                        'original_name': f"{base_filename}.pdf",
                        'created_at': datetime.now()
                    }
                    files['pdf'] = pdf_path
                    download_links['pdf'] = f"/technical-note/reports/download/{file_id}"
                    print(f"✅ PDF generado: {pdf_path}")
            
            elapsed = (datetime.now() - start_time).total_seconds()
            
            print(f"✅ Exportación completada en {elapsed:.2f}s")
            print(f"📄 Archivos: {list(files.keys())}")
            print(f"=====================================\n")
            
            return {
                'success': True,
                'message': f'Exportación completada: {len(files)} archivo(s)',
                'files': files,
                'download_links': download_links,
                'execution_time_seconds': elapsed
            }
            
        except Exception as e:
            print(f"❌ Error en exportación: {e}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'message': f'Error en exportación: {str(e)}',
                'files': {},
                'download_links': {}
            }
    
    def _export_temporal_csv(
        self, 
        report_data: Dict[str, Any], 
        output_path: str, 
        separator: str, 
        encoding: str
    ) -> str:
        """
        ✅ EXPORTAR CSV TEMPORAL CON JERARQUÍA
        Estructura: ACTIVIDAD → AÑO → MES
        """
        try:
            temporal_data = report_data.get('temporal_data', {})
            items = report_data.get('items', [])
            
            print(f"📊 Exportando temporal: {len(items)} actividades, {len(temporal_data)} claves")
            
            if not temporal_data:
                print("⚠️ Sin datos temporales")
                return ""
            
            # ✅ HEADERS
            headers = [
                'Procedimiento/Consulta',
                'Palabra Clave',
                'Tipo',
                'Periodo',
                'Denominador',
                'Numerador',
                '% Cumplimiento',
                'Estado',
                'Descripcion'
            ]
            
            rows = []
            
            # ✅ PROCESAR CADA ACTIVIDAD
            for key, temporal_info in temporal_data.items():
                column = str(temporal_info.get('column', '')).replace(separator, ' ')
                keyword = str(temporal_info.get('keyword', ''))
                
                # ✅ BUSCAR TOTALES GLOBALES
                global_item = None
                for item in items:
                    if (item.get('column') == temporal_info.get('column') and 
                        item.get('keyword') == temporal_info.get('keyword') and 
                        item.get('age_range') == temporal_info.get('age_range')):
                        global_item = item
                        break
                
                # ✅ FILA ACTIVIDAD GLOBAL
                if global_item:
                    rows.append([
                        column,
                        keyword,
                        'ACTIVIDAD',
                        column,
                        str(global_item.get('denominador', 0)),
                        str(global_item.get('numerador', 0)),
                        str(round(global_item.get('cobertura_porcentaje', 0.0), 1)).replace('.', ','),
                        global_item.get('semaforizacion', 'NA'),
                        global_item.get('descripcion', '').replace(separator, ' ')
                    ])
                
                # ✅ PROCESAR AÑOS
                years_dict = temporal_info.get('years', {})
                
                for year_str in sorted(years_dict.keys(), key=lambda y: int(y), reverse=True):
                    year_info = years_dict[year_str]
                    
                    # DATOS DEL AÑO
                    year_den = year_info.get('total_den') or year_info.get('denominador') or 0
                    year_num = year_info.get('total_num') or year_info.get('numerador') or 0
                    year_pct = year_info.get('pct') or year_info.get('cobertura_porcentaje') or 0.0
                    
                    rows.append([
                        column,
                        keyword,
                        'AÑO',
                        year_str,
                        str(year_den),
                        str(year_num),
                        str(round(year_pct, 1)).replace('.', ','),
                        year_info.get('semaforizacion', 'NA'),
                        year_info.get('descripcion', '').replace(separator, ' ')
                    ])
                    
                    # ✅ PROCESAR MESES
                    months_dict = year_info.get('months', {})
                    sorted_months = sorted(months_dict.items(), key=lambda m: m[1].get('month', 0))
                    
                    for month_name, month_info in sorted_months:
                        month_den = month_info.get('denominador') or month_info.get('den') or 0
                        month_num = month_info.get('numerador') or month_info.get('num') or 0
                        month_pct = month_info.get('pct') or month_info.get('cobertura_porcentaje') or 0.0
                        
                        rows.append([
                            column,
                            keyword,
                            'MES',
                            f"{month_name} {year_str}",
                            str(month_den),
                            str(month_num),
                            str(round(month_pct, 1)).replace('.', ','),
                            month_info.get('semaforizacion', 'NA'),
                            month_info.get('descripcion', '').replace(separator, ' ')
                        ])
                
                # Separador entre actividades
                rows.append([''] * len(headers))
            
            # ✅ ESCRIBIR CSV
            with open(output_path, 'w', newline='', encoding=encoding, errors='replace') as f:
                writer = csv.writer(f, delimiter=separator)
                writer.writerow(headers)
                writer.writerows(rows)
            
            print(f"✅ CSV Temporal: {len(rows)} filas")
            return output_path
            
        except Exception as e:
            print(f"❌ Error CSV temporal: {e}")
            import traceback
            traceback.print_exc()
            return ""
    
    def _export_pdf(
        self,
        report_data: Dict[str, Any],
        output_path: str,
        include_temporal: bool = True
    ) -> str:
        """
        ✅ EXPORTAR PDF COMPLETO CON TEMPORAL
        """
        try:
            if not REPORTLAB_AVAILABLE:
                print("⚠️ ReportLab no disponible")
                return ""
            
            items = report_data.get('items', [])
            global_stats = report_data.get('global_statistics', {})
            temporal_data = report_data.get('temporal_data', {})
            corte_fecha = report_data.get('corte_fecha', 'No especificada')
            filename = report_data.get('filename', 'Reporte')
            
            print(f"📄 Generando PDF completo: {len(items)} actividades")
            
            doc = SimpleDocTemplate(
                output_path,
                pagesize=landscape(A4),
                rightMargin=30,
                leftMargin=30,
                topMargin=30,
                bottomMargin=30
            )
            
            elements = []
            styles = getSampleStyleSheet()
            
            # ✅ ESTILO TÍTULO
            title_style = ParagraphStyle(
                'CustomTitle',
                parent=styles['Heading1'],
                fontSize=18,
                textColor=colors.HexColor('#1890ff'),
                spaceAfter=10,
                alignment=TA_CENTER
            )
            
            subtitle_style = ParagraphStyle(
                'Subtitle',
                parent=styles['Normal'],
                fontSize=11,
                textColor=colors.grey,
                spaceAfter=20,
                alignment=TA_CENTER
            )
            
            # ✅ ENCABEZADO
            elements.append(Paragraph("📊 Reporte de Indicadores - Primera Infancia", title_style))
            elements.append(Paragraph(f"Archivo: {filename} | Fecha corte: {corte_fecha}", subtitle_style))
            elements.append(Spacer(1, 0.2 * inch))
            
            # ✅ ESTADÍSTICAS GLOBALES
            if global_stats:
                elements.append(Paragraph("📈 Estadísticas Globales", styles['Heading2']))
                elements.append(Spacer(1, 0.1 * inch))
                
                stats_data = [
                    ['Métrica', 'Valor'],
                    ['Total Actividades', str(global_stats.get('total_actividades', 0))],
                    ['Denominador Global', f"{global_stats.get('total_denominador_global', 0):,}"],
                    ['Numerador Global', f"{global_stats.get('total_numerador_global', 0):,}"],
                    ['Cobertura Global', f"{global_stats.get('cobertura_global_porcentaje', 0):.1f}%"],
                    ['Mejor Cobertura', f"{global_stats.get('mejor_cobertura', 0):.1f}%"],
                    ['Peor Cobertura', f"{global_stats.get('peor_cobertura', 0):.1f}%"],
                    ['Actividades Óptimas (100%)', str(global_stats.get('actividades_100_pct_cobertura', 0))],
                    ['Actividades Deficientes (<50%)', str(global_stats.get('actividades_menos_50_pct_cobertura', 0))]
                ]
                
                stats_table = Table(stats_data, colWidths=[4*inch, 2.5*inch])
                stats_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1890ff')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 11),
                    ('FONTSIZE', (0, 1), (-1, -1), 10),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('TOPPADDING', (0, 1), (-1, -1), 8),
                    ('BOTTOMPADDING', (0, 1), (-1, -1), 8),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('GRID', (0, 0), (-1, -1), 1, colors.grey),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE')
                ]))
                
                elements.append(stats_table)
                elements.append(Spacer(1, 0.3 * inch))
            
            # ✅ TABLA DE ACTIVIDADES PRINCIPALES
            elements.append(Paragraph("📋 Actividades Evaluadas", styles['Heading2']))
            elements.append(Spacer(1, 0.1 * inch))
            
            table_data = [['Procedimiento', 'Palabra', 'Den', 'Num', '% Cump', 'Estado']]
            
            for item in items:
                table_data.append([
                    str(item.get('column', ''))[:45],
                    str(item.get('keyword', '')).upper(),
                    f"{item.get('denominador', 0):,}",
                    f"{item.get('numerador', 0):,}",
                    f"{item.get('cobertura_porcentaje', 0):.1f}%",
                    item.get('semaforizacion', 'NA')
                ])
            
            activities_table = Table(
                table_data, 
                colWidths=[3.2*inch, 0.9*inch, 0.9*inch, 0.9*inch, 0.9*inch, 1.3*inch]
            )
            
            table_style = [
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1890ff')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('FONTSIZE', (0, 1), (-1, -1), 9),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('TOPPADDING', (0, 1), (-1, -1), 6),
                ('BOTTOMPADDING', (0, 1), (-1, -1), 6),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE')
            ]
            
            # ✅ COLORES DE SEMAFORIZACIÓN
            for i, item in enumerate(items, start=1):
                estado = item.get('semaforizacion', 'NA')
                color_map = {
                    'Óptimo': '#4CAF50',
                    'Aceptable': '#FF9800',
                    'Deficiente': '#FF5722',
                    'Muy Deficiente': '#F44336',
                    'NA': '#9E9E9E'
                }
                color_hex = color_map.get(estado, '#9E9E9E')
                
                try:
                    bg_color = colors.HexColor(color_hex)
                    bg_color.alpha = 0.3
                    table_style.append(('BACKGROUND', (5, i), (5, i), bg_color))
                    table_style.append(('TEXTCOLOR', (5, i), (5, i), colors.HexColor(color_hex)))
                    table_style.append(('FONTNAME', (5, i), (5, i), 'Helvetica-Bold'))
                except:
                    pass
            
            activities_table.setStyle(TableStyle(table_style))
            elements.append(activities_table)
            
            # ✅ ANÁLISIS TEMPORAL (SI ESTÁ HABILITADO)
            if include_temporal and temporal_data:
                elements.append(PageBreak())
                elements.append(Paragraph("📅 Análisis Temporal por Actividad", styles['Heading2']))
                elements.append(Spacer(1, 0.2 * inch))
                
                for key, temporal_info in temporal_data.items():
                    column = str(temporal_info.get('column', ''))
                    keyword = str(temporal_info.get('keyword', ''))
                    years_dict = temporal_info.get('years', {})
                    
                    if not years_dict:
                        continue
                    
                    # Título de la actividad
                    activity_title = f"🔹 {column} ({keyword.upper()})"
                    elements.append(Paragraph(activity_title, styles['Heading3']))
                    elements.append(Spacer(1, 0.1 * inch))
                    
                    # Tabla temporal
                    temporal_table_data = [['Periodo', 'Tipo', 'Den', 'Num', '% Cump', 'Estado']]
                    
                    for year_str in sorted(years_dict.keys(), key=lambda y: int(y), reverse=True):
                        year_info = years_dict[year_str]
                        
                        # Fila del año
                        year_den = year_info.get('total_den') or year_info.get('denominador') or 0
                        year_num = year_info.get('total_num') or year_info.get('numerador') or 0
                        year_pct = year_info.get('pct') or 0.0
                        
                        temporal_table_data.append([
                            year_str,
                            'AÑO',
                            f"{year_den:,}",
                            f"{year_num:,}",
                            f"{year_pct:.1f}%",
                            year_info.get('semaforizacion', 'NA')
                        ])
                        
                        # Filas de meses
                        months_dict = year_info.get('months', {})
                        sorted_months = sorted(months_dict.items(), key=lambda m: m[1].get('month', 0))
                        
                        for month_name, month_info in sorted_months:
                            month_den = month_info.get('denominador') or month_info.get('den') or 0
                            month_num = month_info.get('numerador') or month_info.get('num') or 0
                            month_pct = month_info.get('pct') or 0.0
                            
                            temporal_table_data.append([
                                month_name,
                                'MES',
                                f"{month_den:,}",
                                f"{month_num:,}",
                                f"{month_pct:.1f}%",
                                month_info.get('semaforizacion', 'NA')
                            ])
                    
                    temporal_table = Table(
                        temporal_table_data,
                        colWidths=[1.8*inch, 0.8*inch, 1*inch, 1*inch, 1*inch, 1.3*inch]
                    )
                    
                    temporal_style = [
                        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#52c41a')),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('FONTSIZE', (0, 0), (-1, 0), 9),
                        ('FONTSIZE', (0, 1), (-1, -1), 8),
                        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                        ('TOPPADDING', (0, 1), (-1, -1), 5),
                        ('BOTTOMPADDING', (0, 1), (-1, -1), 5)
                    ]
                    
                    # Resaltar filas de años
                    row_idx = 1
                    for year_str in sorted(years_dict.keys(), key=lambda y: int(y), reverse=True):
                        temporal_style.append(('BACKGROUND', (0, row_idx), (-1, row_idx), colors.HexColor('#e6f7ff')))
                        temporal_style.append(('FONTNAME', (0, row_idx), (1, row_idx), 'Helvetica-Bold'))
                        row_idx += 1
                        months_count = len(years_dict[year_str].get('months', {}))
                        
                        # Resaltar filas de meses
                        for _ in range(months_count):
                            temporal_style.append(('BACKGROUND', (0, row_idx), (-1, row_idx), colors.HexColor('#f6ffed')))
                            row_idx += 1
                    
                    temporal_table.setStyle(TableStyle(temporal_style))
                    elements.append(temporal_table)
                    elements.append(Spacer(1, 0.2 * inch))
            
            # ✅ CONSTRUIR PDF
            doc.build(elements)
            
            print(f"✅ PDF completo generado: {output_path}")
            return output_path
            
        except Exception as e:
            print(f"❌ Error generando PDF: {e}")
            import traceback
            traceback.print_exc()
            return ""

    
    def get_temp_file(self, file_id: str) -> Optional[Dict[str, Any]]:
        """Obtiene información de archivo temporal"""
        return self.temp_files_registry.get(file_id)
    
    def cleanup_old_temp_files(self, max_age_minutes: int = 30):
        """Limpia archivos temporales antiguos"""
        try:
            now = datetime.now()
            to_delete = []
            
            for file_id, file_info in self.temp_files_registry.items():
                age = now - file_info['created_at']
                if age > timedelta(minutes=max_age_minutes):
                    try:
                        if os.path.exists(file_info['file_path']):
                            os.remove(file_info['file_path'])
                        to_delete.append(file_id)
                    except Exception as e:
                        print(f"⚠️ Error eliminando {file_id}: {e}")
            
            for file_id in to_delete:
                del self.temp_files_registry[file_id]
            
            if to_delete:
                print(f"🗑️ Limpieza: {len(to_delete)} archivos eliminados")
                
        except Exception as e:
            print(f"❌ Error en limpieza: {e}")
