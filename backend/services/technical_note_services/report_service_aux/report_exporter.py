# services/technical_note_services/report_service_aux/report_exporter.py
import io
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

try:
    from reportlab.lib.units import inch
except ImportError:
    inch = 72  # 1 pulgada = 72 puntos

from services.technical_note_services.report_service_aux.excel_exporter import ExcelExporter
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
        self.excel_exporter  = ExcelExporter()
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
        include_detailed: bool = True
    ) -> Dict[str, Any]:
        """
        Orquesta la exportación de reportes EN MEMORIA
        
        Args:
            report_data: Datos del reporte a exportar
            base_filename: Nombre base del archivo (sin extensión)
            export_csv: Exportar en formato CSV
            export_pdf: Exportar en formato PDF
            include_detailed: Incluir análisis detallado en PDF
            
        Returns:
            Diccionario con información de archivos generados y enlaces de descarga
        """
        try:
            start_time = datetime.now()
            
            print("========== EXPORTACIÓN EN MEMORIA ==========")
            print(f"Archivo: {base_filename}")
            print(f"CSV: {export_csv}, PDF: {export_pdf}")
            print(f"Análisis detallado: {include_detailed}")
            
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
                pdf_result = self._export_pdf(report_data, base_filename, include_detailed)
                if pdf_result:
                    files.update(pdf_result['files'])
                    download_links.update(pdf_result['links'])
            
            elapsed = (datetime.now() - start_time).total_seconds()
            
            print(f"✅ Exportación completada en {elapsed:.2f}s")
            print(f"Archivos generados: {len(files)}")
            print("=" * 44)
            
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
    
    def _export_csv(self, report_data, base_filename):
        """Exporta Excel en lugar de CSV"""
        try:
            print("📊 Generando Excel...")
            excel_buffer = self.excel_exporter.export_report(report_data)
            
            if excel_buffer:
                file_id = str(uuid.uuid4())
                self.temp_files_registry[file_id] = {
                    'content': excel_buffer,
                    'filename': f"{base_filename}.xlsx", 
                    'content_type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    'created_at': datetime.now()
                }
                
                print(f"✅ Excel generado en memoria: ID={file_id[:8]}...")
                
                return {
                    'files': {'excel': file_id},
                    'links': {'excel': f"/technical-note/reports/download/{file_id}"}
                }
            
            print("⚠️ Excel no generado")
            return None
            
        except Exception as e:
            print(f"❌ Error generando Excel: {e}")
            return None
    
    def _export_pdf(
        self,
        report_data: Dict[str, Any],
        base_filename: str,
        include_detailed: bool
    ) -> Optional[Dict[str, Any]]:
        """Exporta PDF en memoria y registra"""
        try:
            print("📄 Generando PDF...")
            pdf_buffer = self.pdf_exporter.export_report(report_data, include_detailed)
            
            if pdf_buffer:
                file_id = str(uuid.uuid4())
                self.temp_files_registry[file_id] = {
                    'content': pdf_buffer,
                    'filename': f"{base_filename}.pdf",
                    'content_type': 'application/pdf',
                    'created_at': datetime.now()
                }
                
                print(f"✅ PDF generado en memoria: ID={file_id[:8]}...")
                
                return {
                    'files': {'pdf': file_id},
                    'links': {'pdf': f"/technical-note/reports/download/{file_id}"}
                }
            
            print("⚠️ PDF no generado")
            return None
            
        except Exception as e:
            print(f"❌ Error generando PDF: {e}")
            return None
    
    def get_temp_file(self, file_id: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene archivo en memoria por ID
        
        Args:
            file_id: UUID del archivo
            
        Returns:
            Diccionario con content (BytesIO), filename y content_type
        """
        file_info = self.temp_files_registry.get(file_id)
        
        if file_info:
            # Verificar edad del archivo
            age = datetime.now() - file_info['created_at']
            if age > timedelta(hours=1):
                # Archivo muy antiguo, eliminarlo
                del self.temp_files_registry[file_id]
                print(f"🗑️ Archivo expirado eliminado: {file_id[:8]}...")
                return None
        
        return file_info
    
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
            else:
                print(f"✓ Limpieza: No hay archivos antiguos (edad máx: {max_age_minutes}min)")
                
        except Exception as e:
            print(f"❌ Error en limpieza: {e}")
    
    def get_registry_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas del registro de archivos temporales"""
        total_files = len(self.temp_files_registry)
        
        if total_files == 0:
            return {
                'total_files': 0,
                'oldest_file_age_minutes': 0,
                'newest_file_age_minutes': 0,
                'total_size_bytes': 0
            }
        
        now = datetime.now()
        ages = [(now - info['created_at']).total_seconds() / 60 for info in self.temp_files_registry.values()]
        sizes = [len(info['content'].getvalue()) for info in self.temp_files_registry.values()]
        
        return {
            'total_files': total_files,
            'oldest_file_age_minutes': max(ages),
            'newest_file_age_minutes': min(ages),
            'total_size_bytes': sum(sizes)
        }


# Instancia global del exportador
report_exporter = ReportExporter()
