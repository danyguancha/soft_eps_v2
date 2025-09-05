# controllers/technical_note_controller/technical_note.py (REEMPLAZADO CORE)
import os
from typing import Dict, Any, List, Optional
from fastapi import HTTPException
from services.duckdb_service import duckdb_service


class TechnicalNoteController:
    """✅ Controlador ULTRA-RÁPIDO usando DuckDB para archivos técnicos"""
    
    def __init__(self, storage_manager):
        self.storage_manager = storage_manager
        self.static_files_dir = "technical_note"
        self.loaded_technical_files = {}  # Cache de archivos técnicos cargados
    
    def read_technical_file_data_paginated(
        self, 
        filename: str, 
        page: int = 1, 
        page_size: int = 1000,
        sheet_name: Optional[str] = None,
        filters: Optional[List[Dict[str, Any]]] = None,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None
    ) -> Dict[str, Any]:
        """✅ PAGINACIÓN ULTRA-RÁPIDA con DuckDB"""
        try:
            file_path = os.path.join(self.static_files_dir, filename)
            
            if not os.path.exists(file_path):
                raise HTTPException(status_code=404, detail=f"Archivo no encontrado: {filename}")
            
            # ✅ CARGAR A DUCKDB si no está cargado
            file_key = f"technical_{filename}"
            
            if file_key not in self.loaded_technical_files:
                print(f"🚀 Cargando archivo técnico a DuckDB: {filename}")
                
                # Convertir a Parquet y cargar a DuckDB
                _, ext = os.path.splitext(filename)
                parquet_result = duckdb_service.convert_file_to_parquet(
                    file_path, file_key, filename, ext.replace('.', '')
                )
                
                if parquet_result["success"]:
                    table_name = duckdb_service.load_parquet_to_table(
                        file_key, parquet_result["parquet_path"]
                    )
                    
                    self.loaded_technical_files[file_key] = {
                        "table_name": table_name,
                        "columns": parquet_result["columns"],
                        "total_rows": parquet_result["total_rows"]
                    }
                else:
                    raise HTTPException(status_code=500, detail="Error cargando archivo técnico")
            
            # ✅ CONSULTA ULTRA-RÁPIDA
            result = duckdb_service.query_data_ultra_fast(
                file_id=file_key,
                filters=filters,
                search=search,
                sort_by=sort_by,
                sort_order=sort_order or "asc",
                page=page,
                page_size=page_size
            )
            
            technical_info = self.loaded_technical_files[file_key]
            
            return {
                "success": True,
                "filename": filename,
                "display_name": self._generate_display_name(filename),
                "description": self._generate_description(filename),
                "data": result["data"],
                "columns": technical_info["columns"],
                "pagination": result["pagination"],
                "filters_applied": filters or [],
                "search_applied": search,
                "sort_applied": {"column": sort_by, "order": sort_order} if sort_by else None,
                "file_info": {
                    "total_rows": technical_info["total_rows"],
                    "total_columns": len(technical_info["columns"]),
                    "processing_method": "duckdb_ultra_fast",
                    "ultra_fast": True,
                    "engine": "DuckDB"
                }
            }
            
        except HTTPException:
            raise
        except Exception as e:
            print(f"❌ Error en consulta ultra-rápida: {e}")
            raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

    def get_column_unique_values(
        self, 
        filename: str, 
        column_name: str,
        sheet_name: Optional[str] = None,
        limit: int = 1000
    ) -> Dict[str, Any]:
        """✅ VALORES ÚNICOS ULTRA-RÁPIDOS con DuckDB"""
        try:
            # Asegurarse de que el archivo esté cargado
            file_key = f"technical_{filename}"
            
            if file_key not in self.loaded_technical_files:
                # Cargar archivo si no está en memoria
                self.read_technical_file_data_paginated(filename, page=1, page_size=1)
            
            # ✅ OBTENER VALORES ÚNICOS ULTRA-RÁPIDO
            unique_values = duckdb_service.get_unique_values_ultra_fast(
                file_key, column_name, limit
            )
            
            return {
                "filename": filename,
                "column_name": column_name,
                "unique_values": unique_values,
                "total_unique": len(unique_values),
                "limited": len(unique_values) >= limit,
                "limit_applied": limit,
                "ultra_fast": True,
                "engine": "DuckDB"
            }
            
        except Exception as e:
            print(f"❌ Error obteniendo valores únicos ultra-rápidos: {e}")
            raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

    def get_available_static_files(self) -> List[Dict[str, Any]]:
        """Lista archivos técnicos disponibles"""
        try:
            if not os.path.exists(self.static_files_dir):
                return []
            
            available_files = []
            supported_extensions = {'.csv', '.xlsx', '.xls'}
            
            for filename in os.listdir(self.static_files_dir):
                file_path = os.path.join(self.static_files_dir, filename)
                
                if not os.path.isfile(file_path):
                    continue
                
                _, ext = os.path.splitext(filename)
                if ext.lower() not in supported_extensions:
                    continue
                
                file_size = os.path.getsize(file_path)
                
                available_files.append({
                    "filename": filename,
                    "display_name": self._generate_display_name(filename),
                    "file_path": file_path,
                    "extension": ext.lower().replace('.', ''),
                    "file_size": file_size,
                    "description": self._generate_description(filename),
                    "ultra_fast_ready": True
                })
            
            return available_files
            
        except Exception as e:
            print(f"❌ Error listando archivos técnicos: {e}")
            return []

    def get_technical_file_metadata(self, filename: str) -> Dict[str, Any]:
        """Obtiene metadatos ultra-rápidos"""
        try:
            file_key = f"technical_{filename}"
            
            if file_key not in self.loaded_technical_files:
                # Cargar para obtener metadatos
                self.read_technical_file_data_paginated(filename, page=1, page_size=1)
            
            technical_info = self.loaded_technical_files[file_key]
            file_path = os.path.join(self.static_files_dir, filename)
            
            return {
                "filename": filename,
                "display_name": self._generate_display_name(filename),
                "description": self._generate_description(filename),
                "total_rows": technical_info["total_rows"],
                "total_columns": len(technical_info["columns"]),
                "columns": technical_info["columns"],
                "file_size": os.path.getsize(file_path),
                "ultra_fast": True,
                "engine": "DuckDB",
                "recommended_page_size": min(1000, max(100, technical_info["total_rows"] // 100)) if technical_info["total_rows"] > 0 else 1000
            }
            
        except Exception as e:
            print(f"❌ Error obteniendo metadatos: {e}")
            raise HTTPException(status_code=500, detail=f"Error metadatos: {str(e)}")

    def _generate_display_name(self, filename: str) -> str:
        """Genera nombre display amigable"""
        name_map = {
            "AdolescenciaNueva.csv": "Adolescencia",
            "AdultezNueva.csv": "Adultez", 
            "InfanciaNueva.csv": "Infancia",
            "JuventudNueva.csv": "Juventud",
            "PrimeraInfanciaNueva.csv": "Primera Infancia",
            "VejezNueva.csv": "Vejez"
        }
        
        return name_map.get(filename, filename.replace('.csv', '').replace('.xlsx', '').replace('.xls', ''))
    
    def _generate_description(self, filename: str) -> str:
        """Genera descripción del archivo"""
        desc_map = {
            "AdolescenciaNueva.csv": "Datos de población adolescente",
            "AdultezNueva.csv": "Datos de población adulta",
            "InfanciaNueva.csv": "Datos de población infantil", 
            "JuventudNueva.csv": "Datos de población joven",
            "PrimeraInfanciaNueva.csv": "Datos de primera infancia",
            "VejezNueva.csv": "Datos de población adulto mayor"
        }
        
        return desc_map.get(filename, f"Archivo técnico: {filename}")


def get_technical_note_controller():
    from controllers.files_controllers.storage_manager import storage_manager
    return TechnicalNoteController(storage_manager)


technical_note_controller = get_technical_note_controller()
