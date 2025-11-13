# services/technical_note_services/report_service_aux/pdf_styles.py
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY


class PDFStyleManager:
    """Gestor centralizado de estilos para el PDF"""
    
    def __init__(self):
        self.base_styles = getSampleStyleSheet()
    
    def get_main_title_style(self) -> ParagraphStyle:
        """Estilo para título principal"""
        return ParagraphStyle(
            'MainTitle',
            parent=self.base_styles['Heading1'],
            fontSize=20,
            textColor=colors.HexColor('#1890ff'),
            spaceAfter=8,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        )
    
    def get_subtitle_style(self) -> ParagraphStyle:
        """Estilo para subtítulo"""
        return ParagraphStyle(
            'Subtitle',
            parent=self.base_styles['Normal'],
            fontSize=13,
            textColor=colors.HexColor('#595959'),
            spaceAfter=6,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        )
    
    def get_organization_style(self) -> ParagraphStyle:
        """Estilo para organización"""
        return ParagraphStyle(
            'OrgStyle',
            parent=self.base_styles['Normal'],
            fontSize=11,
            textColor=colors.grey,
            spaceAfter=15,
            alignment=TA_CENTER
        )
    
    def get_metadata_style(self) -> ParagraphStyle:
        """Estilo para metadatos"""
        return ParagraphStyle(
            'Metadata',
            parent=self.base_styles['Normal'],
            fontSize=9,
            textColor=colors.HexColor('#8c8c8c'),
            spaceAfter=4,
            alignment=TA_CENTER
        )
    
    def get_description_style(self) -> ParagraphStyle:
        """Estilo para descripción"""
        return ParagraphStyle(
            'DescStyle',
            parent=self.base_styles['Normal'],
            fontSize=9,
            textColor=colors.black,
            spaceAfter=12,
            alignment=TA_JUSTIFY,
            leftIndent=40,
            rightIndent=40,
            leading=12
        )
    
    def get_section_title_style(self) -> ParagraphStyle:
        """Estilo para títulos de sección"""
        return ParagraphStyle(
            'SectionTitle',
            parent=self.base_styles['Heading3'],
            fontSize=11,
            textColor=colors.HexColor('#1890ff'),
            spaceAfter=8,
            fontName='Helvetica-Bold'
        )
    
    def get_section_text_style(self) -> ParagraphStyle:
        """Estilo para texto de sección"""
        return ParagraphStyle(
            'SectionText',
            parent=self.base_styles['Normal'],
            fontSize=9,
            textColor=colors.black,
            spaceAfter=12,
            alignment=TA_JUSTIFY,
            leftIndent=15,
            rightIndent=15,
            leading=12
        )
    
    def get_header_style(self) -> ParagraphStyle:
        """Estilo para encabezados"""
        return ParagraphStyle(
            'HeaderStyle',
            parent=self.base_styles['Heading2'],
            fontSize=13,
            textColor=colors.HexColor('#1890ff'),
            spaceAfter=10,
            fontName='Helvetica-Bold'
        )
    
    def get_analysis_title_style(self) -> ParagraphStyle:
        """Estilo para títulos de análisis"""
        return ParagraphStyle(
            'AnalysisTitle',
            parent=self.base_styles['Heading3'],
            fontSize=11,
            textColor=colors.HexColor('#52c41a'),
            spaceAfter=8,
            fontName='Helvetica-Bold'
        )
    
    def get_analysis_text_style(self) -> ParagraphStyle:
        """Estilo para texto de análisis"""
        return ParagraphStyle(
            'Analysis',
            parent=self.base_styles['Normal'],
            fontSize=9,
            textColor=colors.black,
            spaceAfter=8,
            alignment=TA_JUSTIFY,
            leading=12,
            leftIndent=15,
            rightIndent=15
        )
    
    def get_activity_title_style(self) -> ParagraphStyle:
        """Estilo para títulos de actividad"""
        return ParagraphStyle(
            'ActivityTitle',
            parent=self.base_styles['Heading3'],
            fontSize=11,
            textColor=colors.HexColor('#262626'),
            spaceAfter=8,
            fontName='Helvetica-Bold'
        )
    
    def get_temporal_analysis_style(self) -> ParagraphStyle:
        """Estilo para análisis temporal"""
        return ParagraphStyle(
            'TemporalAnalysis',
            parent=self.base_styles['Normal'],
            fontSize=8,
            textColor=colors.black,
            spaceAfter=6,
            alignment=TA_JUSTIFY,
            leading=11,
            leftIndent=12,
            rightIndent=12
        )
    
    def get_contact_style(self) -> ParagraphStyle:
        """Estilo para información de contacto"""
        return ParagraphStyle(
            'Contact',
            parent=self.base_styles['Normal'],
            fontSize=9,
            textColor=colors.grey,
            spaceAfter=8,
            alignment=TA_CENTER
        )
