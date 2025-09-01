# controllers/upload_handler.py
import uuid
import os
import aiofiles
from fastapi import UploadFile, HTTPException
from typing import Dict, Any, Optional
from services.csv_service import CSVService
from services.excel_service import ExcelService
from controllers.files_controllers.storage_manager import FileStorageManager


class UploadHandler:
    def __init__(self, storage_manager: FileStorageManager):
        self.storage_manager = storage_manager
        self.max_file_size = 5 * 1024 * 1024 * 1024  # 5GB límite
        self.chunk_size = 8192  # 8KB chunks para streaming
        self.processing_chunk_size = 10000  # Registros por chunk
        
        self.file_services = {
            "csv": CSVService(),
            "xlsx": ExcelService(),
            "xls": ExcelService()
        }
    

    async def upload_file(self, file: UploadFile) -> Dict[str, Any]:
        """Procesa archivos grandes usando streaming"""
        if not file.filename:
            raise HTTPException(status_code=400, detail="Nombre de archivo requerido")
            
        ext = file.filename.split('.')[-1].lower()
        if ext not in self.file_services:
            raise HTTPException(
                status_code=400, 
                detail=f"Tipo de archivo no soportado: {ext}"
            )
        
        file_id = str(uuid.uuid4())
        upload_dir = self.storage_manager.ensure_upload_directory()
        file_path = os.path.join(upload_dir, f"{file_id}.{ext}")
        
        try:
            # Streaming write
            await self._save_file_streaming(file, file_path)
            
            # Cargar usando tu estructura existente
            service = self.file_services[ext]
            file_obj = service.load(file_path)
            
            # Obtener información eficientemente
            if ext == "csv":
                return await self._process_csv_efficient(service, file_obj, file_id, file.filename, file_path)
            else:
                return await self._process_excel_efficient(service, file_obj, file_id, file.filename, file_path)
                
        except Exception as e:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except:
                    pass
            raise HTTPException(status_code=500, detail=f"Error procesando archivo: {str(e)}")


    async def _save_file_streaming(self, file: UploadFile, file_path: str):
        """Guarda archivo usando streaming"""
        total_size = 0
        
        try:
            async with aiofiles.open(file_path, 'wb') as f:
                while True:
                    chunk = await file.read(self.chunk_size)
                    if not chunk:
                        break
                    
                    total_size += len(chunk)
                    
                    if total_size > self.max_file_size:
                        raise HTTPException(
                            status_code=413, 
                            detail="Archivo excede el tamaño máximo permitido"
                        )
                    
                    await f.write(chunk)
                    
        except Exception as e:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except:
                    pass
            raise e


    async def _process_csv_efficient(self, service: CSVService, file_obj, file_id: str, original_name: str, file_path: str):
        """Procesa CSV de manera eficiente usando tus clases existentes"""
        
        # Usar los nuevos métodos eficientes
        file_info_data = service.get_file_info(file_obj)
        sheets = service.get_sheets(file_obj)
        columns = service.get_columns(file_obj)
        
        # Obtener muestra para cache inicial
        sample_df = file_obj.get_sample_data(100)  
        
        file_info = {
            "path": file_path,
            "ext": "csv",
            "original_name": original_name,
            "sheets": sheets,
            "default_sheet": None,
            "columns": columns,
            "total_rows": file_info_data['total_rows'],
            "separator": file_info_data['delimiter'],
            "chunk_size": self.processing_chunk_size
        }
        
        self.storage_manager.store_file_info(file_id, file_info)
        
        # Cachear muestra inicial
        cache_key = self.storage_manager.generate_cache_key(file_id, "default")
        self.storage_manager.cache_dataframe(cache_key, sample_df)
        
        return {
            "file_id": file_id,
            "columns": columns,
            "sheets": sheets,
            "total_rows": file_info_data['total_rows'],
            "separator": file_info_data['delimiter']
        }


    async def _process_excel_efficient(self, service: ExcelService, file_obj, file_id: str, original_name: str, file_path: str):
        """Procesa Excel de manera eficiente usando tus clases existentes"""
        
        file_info_data = service.get_file_info(file_obj)
        sheets = service.get_sheets(file_obj)
        default_sheet = sheets[0] if sheets else None
        
        if not default_sheet:
            raise HTTPException(status_code=400, detail="No se encontraron hojas en el archivo Excel")
        
        columns = service.get_columns(file_obj, default_sheet)
        
        # Obtener muestra para cache inicial
        sample_df = file_obj.get_sample_data(default_sheet, 100)
        
        file_info = {
            "path": file_path,
            "ext": "xlsx",
            "original_name": original_name,
            "sheets": sheets,
            "default_sheet": default_sheet,
            "columns": columns,
            "total_rows": file_info_data['sheets_info'][default_sheet]['total_rows'],
            "chunk_size": self.processing_chunk_size
        }
        
        self.storage_manager.store_file_info(file_id, file_info)
        
        # Cachear muestra inicial
        cache_key = self.storage_manager.generate_cache_key(file_id, default_sheet)
        self.storage_manager.cache_dataframe(cache_key, sample_df)
        
        return {
            "file_id": file_id,
            "columns": columns,
            "sheets": sheets,
            "total_rows": file_info_data['sheets_info'][default_sheet]['total_rows']
        }


    def get_data_chunk(
        self, 
        file_id: str, 
        start_row: int = 0, 
        chunk_size: Optional[int] = None, 
        sheet_name: Optional[str] = None
    ):
        """Obtiene datos por chunks usando tu estructura existente"""
        file_info = self.storage_manager.get_file_info(file_id)
        if not file_info:
            raise HTTPException(status_code=404, detail="Archivo no encontrado")
        
        file_path = file_info["path"]
        ext = file_info["ext"]
        actual_chunk_size = chunk_size or file_info.get("chunk_size", self.processing_chunk_size)
        
        try:
            service = self.file_services[ext]
            file_obj = service.load(file_path)
            
            if ext == "csv":
                return service.get_data_chunked(
                    file_obj, 
                    start_row=start_row, 
                    nrows=actual_chunk_size
                )
            else:
                target_sheet = sheet_name or file_info["default_sheet"]
                return service.get_data_chunked(
                    file_obj,
                    target_sheet,
                    start_row=start_row,
                    nrows=actual_chunk_size
                )
                
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error cargando chunk: {str(e)}")


    def is_supported_file(self, filename: str) -> bool:
        """Verifica si el tipo de archivo es soportado"""
        ext = filename.split('.')[-1].lower()
        return ext in self.file_services
storage_manager = FileStorageManager()
upload_handler_instance = UploadHandler(storage_manager)