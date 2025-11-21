# services/technical_note_services/report_service_aux/report_exporter.py
import io
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
try:
    from reportlab.lib.units import inch
except ImportError:
    inch = 72  # 1 pulgada = 72 puntos
from services.technical_note_services.report_service_aux.csv_exporter import CSVExporter
from services.technical_note_services.report_service_aux.pdf_exporter import PDFExporter


class ReportExporter:
    """
    ORQUESTADOR DE EXPORTACIÓN EN MEMORIA (SIN ARCHIVOS TEMPORALES)
    
    Responsabilidades:
    - Coordina exportación de CSV y PDF en memoria
    - Gestiona archivos en memoria con registro temporal
    - Provee enlaces de descarga
    
    Ventajas en producción:
    - No depende de rutas del sistema operativo
    - No deja archivos huérfanos en disco
    - Funciona en cualquier entorno (Windows, Linux, Docker, etc.)
    """
    
    def __init__(self):
        """Inicializa el orquestador"""
        self.temp_files_registry: Dict[str, Dict[str, Any]] = {}
        
        # Inyección de dependencias
        self.csv_exporter = CSVExporter(separator=';', encoding='latin1')
        self.pdf_exporter = PDFExporter(
            watermark_image="assets/mallamas.png",
            watermark_opacity=0.1,
            image_width=6 * inch,
            image_height=6 * inch,
            image_position='center',
            show_page_numbers=True
        )
    
    def export_report(
        self,
        report_data: Dict[str, Any],
        base_filename: str = "reporte",
        export_csv: bool = True,
        export_pdf: bool = False,
        include_temporal: bool = True
    ) -> Dict[str, Any]:
        """
        Orquesta la exportación de reportes EN MEMORIA
        
        Args:
            report_data: Datos del reporte a exportar
            base_filename: Nombre base del archivo (sin extensión)
            export_csv: Exportar en formato CSV
            export_pdf: Exportar en formato PDF
            include_temporal: Incluir análisis temporal en PDF
            
        Returns:
            Diccionario con información de archivos generados y enlaces de descarga
        """
        try:
            start_time = datetime.now()
            
            print("========== EXPORTACIÓN EN MEMORIA ==========")
            print(f"Archivo: {base_filename}")
            print(f"CSV: {export_csv}, PDF: {export_pdf}")
            print(f"Temporal: {include_temporal}")
            
            files = {}
            download_links = {}
            
            # Exportar CSV
            if export_csv:
                csv_result = self._export_csv(report_data, base_filename)
                if csv_result:
                    files.update(csv_result['files'])
                    download_links.update(csv_result['links'])
            
            # Exportar PDF
            if export_pdf:
                pdf_result = self._export_pdf(report_data, base_filename, include_temporal)
                if pdf_result:
                    files.update(pdf_result['files'])
                    download_links.update(pdf_result['links'])
            
            elapsed = (datetime.now() - start_time).total_seconds()            
            return {
                'success': True,
                'message': f'Exportación completada: {len(files)} archivo(s)',
                'files': files,
                'download_links': download_links,
                'execution_time_seconds': elapsed
            }
            
        except Exception as e:
            print(f"Error en exportación: {e}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'message': f'Error en exportación: {str(e)}',
                'files': {},
                'download_links': {}
            }
    
    def _export_csv(
        self,
        report_data: Dict[str, Any],
        base_filename: str
    ) -> Optional[Dict[str, Any]]:
        """Exporta CSV en memoria y registra"""
        csv_buffer = self.csv_exporter.export_temporal_report(report_data)
        
        if csv_buffer:
            file_id = str(uuid.uuid4())
            self.temp_files_registry[file_id] = {
                'content': csv_buffer,
                'filename': f"{base_filename}_temporal.csv",
                'content_type': 'text/csv',
                'created_at': datetime.now()
            }
            
            print(f"CSV generado en memoria: ID={file_id}")
            
            return {
                'files': {'csv_temporal': file_id},
                'links': {'csv_temporal': f"/technical-note/reports/download/{file_id}"}
            }
        
        print("CSV Temporal no generado")
        return None
    
    def _export_pdf(
        self,
        report_data: Dict[str, Any],
        base_filename: str,
        include_temporal: bool
    ) -> Optional[Dict[str, Any]]:
        """Exporta PDF en memoria y registra"""
        pdf_buffer = self.pdf_exporter.export_report(report_data, include_temporal)
        
        if pdf_buffer:
            file_id = str(uuid.uuid4())
            self.temp_files_registry[file_id] = {
                'content': pdf_buffer,
                'filename': f"{base_filename}.pdf",
                'content_type': 'application/pdf',
                'created_at': datetime.now()
            }
            
            print(f"PDF generado en memoria: ID={file_id}")
            
            return {
                'files': {'pdf': file_id},
                'links': {'pdf': f"/technical-note/reports/download/{file_id}"}
            }
        
        print("PDF no generado")
        return None
    
    def get_temp_file(self, file_id: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene archivo en memoria por ID
        
        Args:
            file_id: UUID del archivo
            
        Returns:
            Diccionario con content (BytesIO), filename y content_type
        """
        return self.temp_files_registry.get(file_id)
    
    def cleanup_old_temp_files(self, max_age_minutes: int = 30):
        """
        Limpia archivos en memoria antiguos
        
        Args:
            max_age_minutes: Edad máxima en minutos antes de eliminar
        """
        try:
            now = datetime.now()
            to_delete = []
            
            for file_id, file_info in self.temp_files_registry.items():
                age = now - file_info['created_at']
                if age > timedelta(minutes=max_age_minutes):
                    to_delete.append(file_id)
            
            for file_id in to_delete:
                del self.temp_files_registry[file_id]
            
            if to_delete:
                print(f"🗑️ Limpieza: {len(to_delete)} archivos en memoria eliminados")
                
        except Exception as e:
            print(f"Error en limpieza: {e}")
