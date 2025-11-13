# services/technical_note_services/report_service_aux/pdf_exporter.py
import io
import os
import json
from typing import Any, Dict, Optional
from datetime import datetime

from services.technical_note_services.pdf_exporter_aux.canvas_utils import NumberedCanvas
from services.technical_note_services.pdf_exporter_aux.pdf_config import DEFAULT_CONFIG, DEFAULT_IMAGE_HEIGHT, DEFAULT_IMAGE_WIDTH, DEFAULT_WATERMARK_OPACITY
from services.technical_note_services.pdf_exporter_aux.pdf_styles import PDFStyleManager
from services.technical_note_services.pdf_exporter_aux.section_builders import SectionBuilder

try:
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.platypus import SimpleDocTemplate, PageBreak
    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False
    print("ReportLab no disponible. Instalar con: pip install reportlab")




class PDFExporter:
    """Orquestador principal para exportación de reportes PDF"""
    
    def __init__(
        self, 
        config_path: Optional[str] = None,
        watermark_text: Optional[str] = None,
        watermark_image: Optional[str] = None,
        watermark_opacity: float = DEFAULT_WATERMARK_OPACITY,
        show_page_numbers: bool = True,
        image_width: float = DEFAULT_IMAGE_WIDTH,
        image_height: float = DEFAULT_IMAGE_HEIGHT,
        image_position: str = 'center'
    ):
        self.watermark_text = watermark_text
        self.watermark_image = watermark_image
        self.watermark_opacity = watermark_opacity
        self.show_page_numbers = show_page_numbers
        self.image_width = image_width
        self.image_height = image_height
        self.image_position = image_position
        
        self.pdf_config = self._load_config(config_path)
        self.style_manager = PDFStyleManager()
        self.section_builder = SectionBuilder(self.style_manager)
        
        self._validate_watermark_image()
    
    def _load_config(self, config_path: Optional[str]) -> Dict[str, Any]:
        """Carga configuración desde archivo JSON"""
        if config_path and os.path.exists(config_path):
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    return config.get('pdf_config', DEFAULT_CONFIG)
            except Exception as e:
                print(f"Error cargando configuración: {e}")
        
        return DEFAULT_CONFIG.copy()
    
    def _validate_watermark_image(self):
        """Valida existencia de imagen de marca de agua"""
        if self.watermark_image and not os.path.exists(self.watermark_image):
            print(f"Imagen de marca de agua no encontrada: {self.watermark_image}")
            self.watermark_image = None
    
    def export_report(
        self,
        report_data: Dict[str, Any],
        include_temporal: bool = True
    ) -> Optional[io.BytesIO]:
        """Exporta reporte completo a PDF"""
        if not REPORTLAB_AVAILABLE:
            print("ReportLab no disponible")
            return None
        
        try:
            items = report_data.get('items', [])
            print(f"📄 Generando PDF con análisis: {len(items)} actividades")
            
            pdf_buffer = io.BytesIO()
            doc = self._create_document(pdf_buffer)
            elements = self._build_report_elements(report_data, include_temporal)
            
            footer_text = self._get_footer_text()
            doc.build(elements, canvasmaker=self._get_canvas_maker(footer_text))
            
            pdf_buffer.seek(0)
            print("PDF generado con análisis automático")
            return pdf_buffer
            
        except Exception as e:
            print(f"Error generando PDF: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _create_document(self, pdf_buffer: io.BytesIO) -> SimpleDocTemplate:
        """Crea documento PDF"""
        return SimpleDocTemplate(
            pdf_buffer,
            pagesize=landscape(A4),
            rightMargin=30,
            leftMargin=30,
            topMargin=30,
            bottomMargin=30
        )
    
    def _build_report_elements(self, report_data: Dict[str, Any], include_temporal: bool) -> list:
        """Construye todos los elementos del reporte"""
        elements = []
        
        # Secciones principales
        self.section_builder.build_header(elements, self.pdf_config)
        self.section_builder.build_metadata(elements, self.pdf_config, report_data)
        self.section_builder.build_methodology(elements, self.pdf_config)
        self.section_builder.build_interpretation_guide(elements, self.pdf_config)
        
        # Estadísticas y análisis
        global_stats = report_data.get('global_statistics', {})
        self.section_builder.build_global_statistics(elements, global_stats)
        self.section_builder.build_global_analysis(elements, global_stats)
        
        # Actividades
        items = report_data.get('items', [])
        self.section_builder.build_activities_section(elements, items)
        self.section_builder.build_activities_analysis(elements, items)
        
        # Análisis temporal
        if include_temporal:
            temporal_data = report_data.get('temporal_data', {})
            if temporal_data:
                elements.append(PageBreak())
                self.section_builder.build_temporal_section(elements, temporal_data)
        
        # Footer
        if self.pdf_config.get('contact_info'):
            self.section_builder.build_footer_info(elements, self.pdf_config)
        
        return elements
    
    def _get_footer_text(self) -> str:
        """Obtiene texto del footer"""
        return self.pdf_config.get(
            'footer_text',
            f"Generado el {datetime.now().strftime('%d/%m/%Y %H:%M')}"
        )
    
    def _get_canvas_maker(self, footer_text: str):
        """Retorna función para crear canvas personalizado"""
        return lambda *args, **kwargs: NumberedCanvas(
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
