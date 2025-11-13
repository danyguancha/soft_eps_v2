# services/technical_note_services/report_service_aux/section_builders.py
from typing import List, Dict, Any
from datetime import datetime
from reportlab.platypus import Paragraph, Spacer
from reportlab.lib.units import inch

from services.technical_note_services.pdf_exporter_aux.analysis_generator import AnalysisGenerator
from services.technical_note_services.pdf_exporter_aux.pdf_styles import PDFStyleManager
from services.technical_note_services.pdf_exporter_aux.table_builders import TableBuilder



class SectionBuilder:
    """Constructor de secciones del reporte PDF"""
    
    def __init__(self, style_manager: PDFStyleManager):
        self.style_manager = style_manager
        self.table_builder = TableBuilder()
        self.analysis_generator = AnalysisGenerator()
    
    def build_header(self, elements: List, pdf_config: Dict[str, Any]):
        """Construye encabezado del reporte"""
        elements.append(Paragraph(
            "Reporte de Evaluación de Nota Técnica",
            self.style_manager.get_main_title_style()
        ))
        
        if pdf_config.get('subtitle'):
            elements.append(Paragraph(
                pdf_config['subtitle'],
                self.style_manager.get_subtitle_style()
            ))
        
        if pdf_config.get('organization'):
            elements.append(Paragraph(
                pdf_config['organization'],
                self.style_manager.get_organization_style()
            ))
        
        elements.append(Spacer(1, 0.15 * inch))
    
    def build_metadata(self, elements: List, pdf_config: Dict[str, Any], report_data: Dict[str, Any]):
        """Construye sección de metadatos"""
        filename = report_data.get('filename', 'Reporte')
        corte_fecha = report_data.get('corte_fecha', 'No especificada')
        
        metadata_text = (
            f"<b>Archivo:</b> {filename} | "
            f"<b>Fecha corte:</b> {corte_fecha} | "
            f"<b>Generado:</b> {datetime.now().strftime('%d/%m/%Y %H:%M')}"
        )
        
        elements.append(Paragraph(metadata_text, self.style_manager.get_metadata_style()))
        
        if pdf_config.get('description'):
            elements.append(Spacer(1, 0.1 * inch))
            elements.append(Paragraph(
                pdf_config['description'],
                self.style_manager.get_description_style()
            ))
        
        elements.append(Spacer(1, 0.15 * inch))
    
    def build_methodology(self, elements: List, pdf_config: Dict[str, Any]):
        """Construye sección de metodología"""
        if not pdf_config.get('methodology'):
            return
        
        elements.append(Paragraph(
            "📊 Metodología",
            self.style_manager.get_section_title_style()
        ))
        elements.append(Paragraph(
            pdf_config['methodology'],
            self.style_manager.get_section_text_style()
        ))
    
    def build_interpretation_guide(self, elements: List, pdf_config: Dict[str, Any]):
        """Construye guía de interpretación"""
        interpretation = pdf_config.get('interpretation', {})
        
        if not interpretation:
            return
        
        elements.append(Paragraph(
            "🚦 Guía de Interpretación - Semaforización",
            self.style_manager.get_section_title_style()
        ))
        elements.append(Spacer(1, 0.05 * inch))
        
        table = self.table_builder.build_interpretation_table(interpretation)
        elements.append(table)
        elements.append(Spacer(1, 0.2 * inch))
    
    def build_global_statistics(self, elements: List, global_stats: Dict[str, Any]):
        """Construye sección de estadísticas globales"""
        if not global_stats:
            return
        
        elements.append(Paragraph(
            "📈 Estadísticas Globales",
            self.style_manager.get_header_style()
        ))
        
        table = self.table_builder.build_statistics_table(global_stats)
        elements.append(table)
        elements.append(Spacer(1, 0.15 * inch))
    
    def build_global_analysis(self, elements: List, global_stats: Dict[str, Any]):
        """Construye análisis global"""
        if not global_stats:
            return
        
        elements.append(Paragraph(
            "💡 Análisis General",
            self.style_manager.get_analysis_title_style()
        ))
        
        analysis_texts = self.analysis_generator.generate_global_analysis(global_stats)
        
        for text in analysis_texts:
            elements.append(Paragraph(text, self.style_manager.get_analysis_text_style()))
        
        elements.append(Spacer(1, 0.2 * inch))
    
    def build_activities_section(self, elements: List, items: List[Dict[str, Any]]):
        """Construye sección de actividades"""
        elements.append(Paragraph(
            "📋 Detalle de Actividades Evaluadas",
            self.style_manager.get_header_style()
        ))
        
        table = self.table_builder.build_activities_table(items)
        elements.append(table)
        elements.append(Spacer(1, 0.15 * inch))
    
    def build_activities_analysis(self, elements: List, items: List[Dict[str, Any]]):
        """Construye análisis de actividades"""
        if not items:
            return
        
        elements.append(Paragraph(
            "💡 Análisis de Actividades",
            self.style_manager.get_analysis_title_style()
        ))
        
        analysis_texts = self.analysis_generator.generate_activities_analysis(items)
        
        for text in analysis_texts:
            elements.append(Paragraph(text, self.style_manager.get_analysis_text_style()))
        
        elements.append(Spacer(1, 0.2 * inch))
    
    def build_temporal_section(self, elements: List, temporal_data: Dict[str, Any]):
        """Construye sección temporal"""
        if not temporal_data:
            return
        
        elements.append(Paragraph(
            "📅 Análisis Temporal por Actividad",
            self.style_manager.get_header_style()
        ))
        
        for key, temporal_info in temporal_data.items():
            self._build_temporal_activity(elements, temporal_info)
    
    def _build_temporal_activity(self, elements: List, temporal_info: Dict[str, Any]):
        """Construye actividad temporal individual"""
        column = str(temporal_info.get('column', ''))
        keyword = str(temporal_info.get('keyword', ''))
        years_dict = temporal_info.get('years', {})
        
        if not years_dict:
            return
        
        activity_title = f"🔹 {column} ({keyword.upper()})"
        elements.append(Paragraph(activity_title, self.style_manager.get_activity_title_style()))
        
        table = self.table_builder.build_temporal_table(years_dict)
        elements.append(table)
        elements.append(Spacer(1, 0.1 * inch))
        
        # Análisis temporal
        analysis_texts = self.analysis_generator.generate_temporal_analysis(years_dict)
        
        for text in analysis_texts:
            elements.append(Paragraph(text, self.style_manager.get_temporal_analysis_style()))
        
        elements.append(Spacer(1, 0.15 * inch))
    
    def build_footer_info(self, elements: List, pdf_config: Dict[str, Any]):
        """Construye información de contacto"""
        if not pdf_config.get('contact_info'):
            return
        
        elements.append(Spacer(1, 0.3 * inch))
        elements.append(Paragraph(
            "📞 Información de Contacto",
            self.style_manager.get_section_title_style()
        ))
        elements.append(Spacer(1, 0.1 * inch))
        elements.append(Paragraph(
            pdf_config['contact_info'],
            self.style_manager.get_contact_style()
        ))
