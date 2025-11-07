# controllers/file_info_handler.py
import os
from typing import Dict, Any

import pandas as pd
from controllers.files_controllers.storage_manager import FileStorageManager

class FileInfoHandler:
    def __init__(self, storage_manager: FileStorageManager):
        self.storage_manager = storage_manager
    
    def get_file_info(self, file_id: str) -> Dict[str, Any]:
        """Obtiene información básica del archivo"""
        file_info = self.storage_manager.get_file_info(file_id)
        if not file_info:
            raise ValueError("Archivo no encontrado")
        
        # Si no tiene columnas guardadas, cargarlas del archivo físico
        if "columns" not in file_info:
            file_path = file_info.get("path")
            if file_path and os.path.exists(file_path):
                try:
                    # Cargar el archivo para obtener las columnas
                    if file_path.endswith('.csv'):
                        # SOLUCIÓN 1: Detectar automáticamente el delimitador
                        df = pd.read_csv(file_path, nrows=0, sep=None, engine='python')
                        columns = df.columns.tolist()
                        
                        # SOLUCIÓN 2 (Alternativa): Especificar punto y coma
                        # df = pd.read_csv(file_path, nrows=0, sep=';')
                        # columns = df.columns.tolist()
                        
                    elif file_path.endswith(('.xlsx', '.xls')):
                        df = pd.read_excel(file_path, nrows=0)
                        columns = df.columns.tolist()
                    else:
                        columns = []
                    
                    # Actualizar file_info con las columnas
                    file_info["columns"] = columns
                    
                    # Guardar en storage para próximas veces
                    self.storage_manager.store_file_info(file_id, file_info)
                except Exception as e:
                    print(f"Error leyendo columnas de {file_id}: {e}")
                    columns = []
            else:
                columns = []
        else:
            columns = file_info["columns"]
        
        return {
            "file_id": file_id,
            "original_name": file_info.get("original_name", file_id),
            "columns": columns,
            "sheets": file_info.get("sheets", []),
            "total_rows": file_info.get("total_rows", 0)
        }

    
    def list_all_files(self) -> Dict[str, Any]:
        """Lista todos los archivos cargados"""
        storage = self.storage_manager.list_technical_files()       
        
        return {"files": storage, "total": len(storage)}
    
    def delete_file(self, file_id: str) -> Dict[str, Any]:
        """Elimina archivo del sistema"""
        success = self.storage_manager.remove_file(file_id)
        if not success:
            raise ValueError("Archivo no encontrado")
        
        return {"message": "Archivo eliminado exitosamente"}
