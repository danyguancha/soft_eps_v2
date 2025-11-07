# services/technical_note_services/report_service_aux/pdf_exporter.py
import io
import os
from typing import Any, Dict, List, Optional
from datetime import datetime

try:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
    from reportlab.lib.enums import TA_CENTER
    from reportlab.pdfgen import canvas
    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False
    print("ReportLab no disponible. Instalar con: pip install reportlab")


class NumberedCanvas(canvas.Canvas):
    """
    Canvas personalizado para agregar marca de agua (texto e imagen) y numeración en cada página
    """
    
    def __init__(self, *args, **kwargs):
        # Extraer parámetros personalizados
        self.watermark_text = kwargs.pop('watermark_text', None)
        self.watermark_image = kwargs.pop('watermark_image', None)
        self.watermark_opacity = kwargs.pop('watermark_opacity', 0.1)
        self.show_page_numbers = kwargs.pop('show_page_numbers', True)
        self.footer_text = kwargs.pop('footer_text', None)
        
        # Configuración de imagen
        self.image_width = kwargs.pop('image_width', 2 * inch)
        self.image_height = kwargs.pop('image_height', 2 * inch)
        self.image_position = kwargs.pop('image_position', 'center')
        
        canvas.Canvas.__init__(self, *args, **kwargs)
        self._saved_page_states = []
    
    def showPage(self):
        """Sobrescribe showPage para agregar marca de agua antes de cada página"""
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()
    
    def save(self):
        """Sobrescribe save para agregar marca de agua en todas las páginas"""
        num_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            
            # Dibujar imagen de marca de agua (si existe)
            if self.watermark_image:
                self.draw_watermark_image()
            
            # Dibujar texto de marca de agua (si existe)
            if self.watermark_text:
                self.draw_watermark_text()
            
            # Dibujar footer
            self.draw_page_footer(self._pageNumber, num_pages)
            
            canvas.Canvas.showPage(self)
        canvas.Canvas.save(self)
    
    def draw_watermark_image(self):
        """Dibuja imagen como marca de agua en la página"""
        if not self.watermark_image or not os.path.exists(self.watermark_image):
            return
        
        self.saveState()
        
        # Obtener dimensiones de la página
        page_width = self._pagesize[0]
        page_height = self._pagesize[1]
        
        # Calcular posición según configuración
        if self.image_position == 'center':
            x = (page_width - self.image_width) / 2
            y = (page_height - self.image_height) / 2
        elif self.image_position == 'top-right':
            x = page_width - self.image_width - 50
            y = page_height - self.image_height - 50
        elif self.image_position == 'top-left':
            x = 50
            y = page_height - self.image_height - 50
        elif self.image_position == 'bottom-right':
            x = page_width - self.image_width - 50
            y = 50
        elif self.image_position == 'bottom-left':
            x = 50
            y = 50
        else:  # center por defecto
            x = (page_width - self.image_width) / 2
            y = (page_height - self.image_height) / 2
        
        # Dibujar imagen con transparencia
        try:
            self.setFillAlpha(self.watermark_opacity)
            self.drawImage(
                self.watermark_image,
                x, y,
                width=self.image_width,
                height=self.image_height,
                mask='auto',
                preserveAspectRatio=True
            )
        except Exception as e:
            print(f"Error dibujando marca de agua imagen: {e}")
        
        self.restoreState()
    
    def draw_watermark_text(self):
        """Dibuja texto como marca de agua diagonal en el centro de la página"""
        self.saveState()
        
        # Obtener dimensiones de la página
        page_width = self._pagesize[0]
        page_height = self._pagesize[1]
        
        # Configurar marca de agua
        self.setFont('Helvetica-Bold', 60)
        self.setFillColorRGB(0.5, 0.5, 0.5, alpha=self.watermark_opacity)
        
        # Calcular posición central
        center_x = page_width / 2
        center_y = page_height / 2
        
        # Rotar 45 grados
        self.translate(center_x, center_y)
        self.rotate(45)
        
        # Dibujar texto centrado
        text_width = self.stringWidth(self.watermark_text, 'Helvetica-Bold', 60)
        self.drawString(-text_width / 2, 0, self.watermark_text)
        
        self.restoreState()
    
    def draw_page_footer(self, page_num: int, total_pages: int):
        """Dibuja footer con número de página y texto personalizado"""
        self.saveState()
        
        page_width = self._pagesize[0]
        
        # Footer izquierdo: Texto personalizado
        if self.footer_text:
            self.setFont('Helvetica', 8)
            self.setFillColorRGB(0.3, 0.3, 0.3)
            self.drawString(30, 15, self.footer_text)
        
        # Footer derecho: Número de página
        if self.show_page_numbers:
            self.setFont('Helvetica', 8)
            self.setFillColorRGB(0.3, 0.3, 0.3)
            page_text = f"Página {page_num} de {total_pages}"
            text_width = self.stringWidth(page_text, 'Helvetica', 8)
            self.drawString(page_width - text_width - 30, 15, page_text)
        
        self.restoreState()


class PDFExporter:
    """
    📄 EXPORTADOR PDF EN MEMORIA CON MARCA DE AGUA (TEXTO E IMAGEN)
    - Exporta reportes con estadísticas globales y análisis temporal
    - Marca de agua personalizada (texto e imagen) en cada página
    - Numeración automática de páginas
    """
    
    def __init__(
        self, 
        watermark_text: Optional[str] = None,
        watermark_image: Optional[str] = None,
        watermark_opacity: float = 0.1,
        show_page_numbers: bool = True,
        image_width: float = 2 * inch,
        image_height: float = 2 * inch,
        image_position: str = 'center'
    ):
        """
        Inicializa el exportador PDF
        
        Args:
            watermark_text: Texto de la marca de agua (opcional)
            watermark_image: Ruta a la imagen de marca de agua (PNG, JPG) (opcional)
            watermark_opacity: Opacidad de la marca de agua (0.0 a 1.0)
            show_page_numbers: Mostrar numeración de páginas
            image_width: Ancho de la imagen en pulgadas
            image_height: Alto de la imagen en pulgadas
            image_position: Posición de la imagen ('center', 'top-right', 'top-left', 'bottom-right', 'bottom-left')
        """
        self.watermark_text = watermark_text
        self.watermark_image = watermark_image
        self.watermark_opacity = watermark_opacity
        self.show_page_numbers = show_page_numbers
        self.image_width = image_width
        self.image_height = image_height
        self.image_position = image_position
        
        # Validar que la imagen exista si se proporcionó
        if self.watermark_image and not os.path.exists(self.watermark_image):
            print(f"Imagen de marca de agua no encontrada: {self.watermark_image}")
            self.watermark_image = None
        
        self.color_map = {
            'Óptimo': '#4CAF50',
            'Aceptable': '#FF9800',
            'Deficiente': '#FF5722',
            'Muy Deficiente': '#F44336',
            'NA': '#9E9E9E'
        }
    
    def export_report(
        self,
        report_data: Dict[str, Any],
        include_temporal: bool = True
    ) -> io.BytesIO:
        """
        Exporta reporte completo en memoria con marca de agua
        
        Args:
            report_data: Datos del reporte
            include_temporal: Incluir análisis temporal
            
        Returns:
            BytesIO con contenido PDF listo para descarga
        """
        try:
            if not REPORTLAB_AVAILABLE:
                print("ReportLab no disponible")
                return None
            
            items = report_data.get('items', [])
            watermark_info = []
            
            if self.watermark_text:
                watermark_info.append(f"texto: '{self.watermark_text}'")
            if self.watermark_image:
                watermark_info.append(f"imagen: {os.path.basename(self.watermark_image)}")
            
            watermark_desc = " + ".join(watermark_info) if watermark_info else "sin marca de agua"
            
            print(f"📄 Generando PDF en memoria con {watermark_desc}: {len(items)} actividades")
            
            # Crear buffer en memoria
            pdf_buffer = io.BytesIO()
            
            # Preparar footer personalizado
            footer_text = f"Generado el {datetime.now().strftime('%d/%m/%Y %H:%M')} | Sistema de Indicadores"
            
            # Crear documento con canvas personalizado
            doc = SimpleDocTemplate(
                pdf_buffer,
                pagesize=landscape(A4),
                rightMargin=30,
                leftMargin=30,
                topMargin=30,
                bottomMargin=30
            )
            
            # Construir elementos
            elements = []
            self._add_header(elements, report_data)
            self._add_global_statistics(elements, report_data)
            self._add_activities_table(elements, items)
            
            if include_temporal:
                self._add_temporal_analysis(elements, report_data)
            
            # Generar PDF con canvas personalizado (marca de agua)
            doc.build(
                elements,
                canvasmaker=lambda *args, **kwargs: NumberedCanvas(
                    *args,
                    watermark_text=self.watermark_text,
                    watermark_image=self.watermark_image,
                    watermark_opacity=self.watermark_opacity,
                    show_page_numbers=self.show_page_numbers,
                    footer_text=footer_text,
                    image_width=self.image_width,
                    image_height=self.image_height,
                    image_position=self.image_position,
                    **kwargs
                )
            )
            
            # Mover puntero al inicio
            pdf_buffer.seek(0)
            
            print(f"PDF con {watermark_desc} generado en memoria")
            return pdf_buffer
            
        except Exception as e:
            print(f"Error generando PDF: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _add_header(self, elements: List, report_data: Dict[str, Any]):
        """Agrega encabezado del documento"""
        styles = getSampleStyleSheet()
        
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
        
        filename = report_data.get('filename', 'Reporte')
        corte_fecha = report_data.get('corte_fecha', 'No especificada')
        
        elements.append(Paragraph("Reporte de Indicadores - Primera Infancia", title_style))
        elements.append(Paragraph(f"Archivo: {filename} | Fecha corte: {corte_fecha}", subtitle_style))
        elements.append(Spacer(1, 0.2 * inch))
    
    def _add_global_statistics(self, elements: List, report_data: Dict[str, Any]):
        """Agrega tabla de estadísticas globales"""
        global_stats = report_data.get('global_statistics', {})
        
        if not global_stats:
            return
        
        styles = getSampleStyleSheet()
        elements.append(Paragraph("Estadísticas Globales", styles['Heading2']))
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
    
    def _add_activities_table(self, elements: List, items: List[Dict[str, Any]]):
        """Agrega tabla de actividades principales"""
        styles = getSampleStyleSheet()
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
        
        table_style = self._get_activities_table_style(items)
        activities_table.setStyle(TableStyle(table_style))
        elements.append(activities_table)
    
    def _get_activities_table_style(self, items: List[Dict[str, Any]]) -> List:
        """Genera estilo de tabla de actividades con semaforización"""
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
        
        # Aplicar colores de semaforización
        for i, item in enumerate(items, start=1):
            estado = item.get('semaforizacion', 'NA')
            color_hex = self.color_map.get(estado, '#9E9E9E')
            
            try:
                bg_color = colors.HexColor(color_hex)
                bg_color.alpha = 0.3
                table_style.append(('BACKGROUND', (5, i), (5, i), bg_color))
                table_style.append(('TEXTCOLOR', (5, i), (5, i), colors.HexColor(color_hex)))
                table_style.append(('FONTNAME', (5, i), (5, i), 'Helvetica-Bold'))
            except:
                pass
        
        return table_style
    
    def _add_temporal_analysis(self, elements: List, report_data: Dict[str, Any]):
        """Agrega análisis temporal por actividad"""
        temporal_data = report_data.get('temporal_data', {})
        
        if not temporal_data:
            return
        
        styles = getSampleStyleSheet()
        elements.append(PageBreak())
        elements.append(Paragraph("📅 Análisis Temporal por Actividad", styles['Heading2']))
        elements.append(Spacer(1, 0.2 * inch))
        
        for key, temporal_info in temporal_data.items():
            self._add_temporal_activity(elements, temporal_info, styles)
    
    def _add_temporal_activity(
        self,
        elements: List,
        temporal_info: Dict[str, Any],
        styles: Any
    ):
        """Agrega tabla temporal de una actividad"""
        column = str(temporal_info.get('column', ''))
        keyword = str(temporal_info.get('keyword', ''))
        years_dict = temporal_info.get('years', {})
        
        if not years_dict:
            return
        
        # Título de la actividad
        activity_title = f"🔹 {column} ({keyword.upper()})"
        elements.append(Paragraph(activity_title, styles['Heading3']))
        elements.append(Spacer(1, 0.1 * inch))
        
        # Construir tabla temporal
        temporal_table_data = [['Periodo', 'Tipo', 'Den', 'Num', '% Cump', 'Estado']]
        
        for year_str in sorted(years_dict.keys(), key=lambda y: int(y), reverse=True):
            year_info = years_dict[year_str]
            self._add_year_and_months(temporal_table_data, year_str, year_info)
        
        temporal_table = Table(
            temporal_table_data,
            colWidths=[1.8*inch, 0.8*inch, 1*inch, 1*inch, 1*inch, 1.3*inch]
        )
        
        temporal_style = self._get_temporal_table_style(years_dict)
        temporal_table.setStyle(TableStyle(temporal_style))
        elements.append(temporal_table)
        elements.append(Spacer(1, 0.2 * inch))
    
    def _add_year_and_months(
        self,
        table_data: List,
        year_str: str,
        year_info: Dict[str, Any]
    ):
        """Agrega filas de año y meses a la tabla"""
        # Fila del año
        year_den = year_info.get('total_den') or year_info.get('denominador') or 0
        year_num = year_info.get('total_num') or year_info.get('numerador') or 0
        year_pct = year_info.get('pct') or 0.0
        
        table_data.append([
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
            
            table_data.append([
                month_name,
                'MES',
                f"{month_den:,}",
                f"{month_num:,}",
                f"{month_pct:.1f}%",
                month_info.get('semaforizacion', 'NA')
            ])
    
    def _get_temporal_table_style(self, years_dict: Dict[str, Any]) -> List:
        """Genera estilo de tabla temporal"""
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
        
        # Resaltar filas de años y meses
        row_idx = 1
        for year_str in sorted(years_dict.keys(), key=lambda y: int(y), reverse=True):
            temporal_style.append(('BACKGROUND', (0, row_idx), (-1, row_idx), colors.HexColor('#e6f7ff')))
            temporal_style.append(('FONTNAME', (0, row_idx), (1, row_idx), 'Helvetica-Bold'))
            row_idx += 1
            
            months_count = len(years_dict[year_str].get('months', {}))
            for _ in range(months_count):
                temporal_style.append(('BACKGROUND', (0, row_idx), (-1, row_idx), colors.HexColor('#f6ffed')))
                row_idx += 1
        
        return temporal_style
