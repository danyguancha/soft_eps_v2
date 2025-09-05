# controllers/files_controllers/cross_handler.py (REEMPLAZADO CORE)
from typing import Dict, Any, List
import pandas as pd
import time
from controllers.files_controllers.storage_manager import FileStorageManager
from services.duckdb_service import duckdb_service


class CrossHandler:
    """✅ CrossHandler ULTRA-RÁPIDO usando DuckDB para cruces"""
    
    def __init__(self, storage_manager: FileStorageManager):
        self.storage_manager = storage_manager

    def perform_cross(self, request) -> Dict[str, Any]:
        """✅ CRUCE ULTRA-RÁPIDO usando JOIN optimizado en DuckDB"""
        try:
            file1_info = self.storage_manager.get_file_info(request.file1_key)
            file2_info = self.storage_manager.get_file_info(request.file2_key)
            
            if not file1_info or not file2_info:
                raise ValueError("Uno o ambos archivos no fueron encontrados")
            
            total_rows = (file1_info.get('total_rows', 0) + file2_info.get('total_rows', 0))
            
            print(f"🚀 CRUCE ULTRA-RÁPIDO DuckDB iniciado:")
            print(f"📊 Archivo 1: {file1_info['original_name']} ({file1_info.get('total_rows', 0):,} filas)")
            print(f"📊 Archivo 2: {file2_info['original_name']} ({file2_info.get('total_rows', 0):,} filas)")
            
            # ✅ USAR DUCKDB para cruce ultra-rápido
            result = duckdb_service.cross_files_ultra_fast(
                file1_id=request.file1_key,
                file2_id=request.file2_key,
                key_column_file1=request.key_column_file1,
                key_column_file2=request.key_column_file2,
                join_type=getattr(request, 'cross_type', 'LEFT').upper(),
                columns_to_include=getattr(request, 'columns_to_include', None)
            )
            
            return result
                
        except Exception as e:
            raise ValueError(f"Error en cruce ultra-rápido: {str(e)}")

    def suggest_compatible_columns(self, request) -> Dict[str, Any]:
        """Sugiere columnas compatibles (simplificado para DuckDB)"""
        try:
            file1_info = self.storage_manager.get_file_info(request.file1_key)
            file2_info = self.storage_manager.get_file_info(request.file2_key)
            
            if not file1_info or not file2_info:
                raise ValueError("Uno o ambos archivos no fueron encontrados")
            
            # Sugerencia simple basada en nombres de columna
            suggestions = []
            
            for col1 in file1_info["columns"]:
                for col2 in file2_info["columns"]:
                    if col1.lower() == col2.lower():
                        suggestions.append({
                            "left_column": col1,
                            "right_column": col2,
                            "combined_score": 1.0,
                            "recommendation": "✅ Nombres idénticos - Altamente recomendado"
                        })
            
            return {
                "success": True,
                "suggestions": suggestions[:10],
                "ultra_fast": True
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_available_columns(self, file_id: str, sheet_name: str = None) -> Dict[str, Any]:
        """Obtiene columnas disponibles"""
        file_info = self.storage_manager.get_file_info(file_id)
        if not file_info:
            raise ValueError("Archivo no encontrado")
        
        return {
            "file_id": file_id,
            "sheet_name": sheet_name,
            "columns": file_info["columns"],
            "ultra_fast": True
        }

# Instancia
storage_manager = FileStorageManager()
cross_handler_instance = CrossHandler(storage_manager)
