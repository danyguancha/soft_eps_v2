import pandas as pd
import os
import json
from datetime import datetime
from typing import List, Optional
from models.schemas import ExportFormat, ExportRequest
from services.filter_service import FilterService
from services.sort_service import SortService

class ExportService:
    EXPORT_DIR = "exports"
    
    @staticmethod
    def ensure_export_directory():
        """Asegura que el directorio de exportación exista"""
        if not os.path.exists(ExportService.EXPORT_DIR):
            os.makedirs(ExportService.EXPORT_DIR)
        return ExportService.EXPORT_DIR
    
    @staticmethod
    def export_data(df: pd.DataFrame, request: ExportRequest) -> dict:
        """Exporta DataFrame procesado según las especificaciones"""
        export_dir = ExportService.ensure_export_directory()
        
        # Aplicar filtros si están presentes
        if request.filters:
            df = FilterService.apply_filters(df, request.filters)
        
        # Aplicar búsqueda global
        if request.search:
            df = FilterService.apply_search(df, request.search)
        
        # Aplicar ordenamiento
        if request.sort:
            df = SortService.apply_sort(df, request.sort)
        
        # Seleccionar columnas específicas si se especifican
        if request.selected_columns:
            available_columns = [col for col in request.selected_columns if col in df.columns]
            df = df[available_columns]
        
        # Generar nombre de archivo
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_filename = request.filename or f"export_{request.file_id}_{timestamp}"
        
        if request.format == ExportFormat.CSV:
            return ExportService._export_csv(df, export_dir, base_filename, request.include_headers)
        elif request.format == ExportFormat.EXCEL:
            return ExportService._export_excel(df, export_dir, base_filename, request.include_headers)
        elif request.format == ExportFormat.JSON:
            return ExportService._export_json(df, export_dir, base_filename)
        else:
            raise ValueError(f"Formato de exportación no soportado: {request.format}")
    
    @staticmethod
    def _export_csv(df: pd.DataFrame, export_dir: str, base_filename: str, include_headers: bool) -> dict:
        """Exporta a formato CSV"""
        filename = f"{base_filename}.csv"
        file_path = os.path.join(export_dir, filename)
        
        df.to_csv(file_path, index=False, header=include_headers, encoding='utf-8-sig')
        
        return {
            "filename": filename,
            "file_path": file_path,
            "rows_exported": len(df),
            "format": "csv"
        }
    
    @staticmethod
    def _export_excel(df: pd.DataFrame, export_dir: str, base_filename: str, include_headers: bool) -> dict:
        """Exporta a formato Excel"""
        filename = f"{base_filename}.xlsx"
        file_path = os.path.join(export_dir, filename)
        
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, header=include_headers, sheet_name='Data')
        
        return {
            "filename": filename,
            "file_path": file_path,
            "rows_exported": len(df),
            "format": "excel"
        }
    
    @staticmethod
    def _export_json(df: pd.DataFrame, export_dir: str, base_filename: str) -> dict:
        """Exporta a formato JSON"""
        filename = f"{base_filename}.json"
        file_path = os.path.join(export_dir, filename)
        
        # Convertir DataFrame a JSON con manejo de NaN
        data = df.fillna("").to_dict(orient="records")
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        return {
            "filename": filename,
            "file_path": file_path,
            "rows_exported": len(df),
            "format": "json"
        }
    
    @staticmethod
    def get_export_info(filename: str) -> dict:
        """Obtiene información sobre un archivo exportado"""
        file_path = os.path.join(ExportService.EXPORT_DIR, filename)
        
        if not os.path.exists(file_path):
            raise FileNotFoundError("Archivo exportado no encontrado")
        
        file_stats = os.stat(file_path)
        
        return {
            "filename": filename,
            "file_path": file_path,
            "size_bytes": file_stats.st_size,
            "created_at": datetime.fromtimestamp(file_stats.st_ctime).isoformat(),
            "modified_at": datetime.fromtimestamp(file_stats.st_mtime).isoformat()
        }
    
    @staticmethod
    def cleanup_old_exports(days_old: int = 7):
        """Limpia archivos exportados antiguos"""
        if not os.path.exists(ExportService.EXPORT_DIR):
            return {"deleted": 0, "message": "Directorio de exportación no existe"}
        
        cutoff_time = datetime.now().timestamp() - (days_old * 24 * 60 * 60)
        deleted_count = 0
        
        for filename in os.listdir(ExportService.EXPORT_DIR):
            file_path = os.path.join(ExportService.EXPORT_DIR, filename)
            if os.path.isfile(file_path) and os.path.getctime(file_path) < cutoff_time:
                try:
                    os.remove(file_path)
                    deleted_count += 1
                except Exception:
                    pass
        
        return {
            "deleted": deleted_count,
            "message": f"Eliminados {deleted_count} archivos de más de {days_old} días"
        }
