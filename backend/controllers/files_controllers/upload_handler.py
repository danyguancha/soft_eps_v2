# controllers/upload_handler.py
import uuid
import os
import aiofiles
from fastapi import UploadFile, HTTPException
from typing import Dict, Any, Optional, List
from services.csv_service import CSVService
from services.excel_service import ExcelService
from controllers.files_controllers.storage_manager import FileStorageManager

class UploadHandler:
    def __init__(self, storage_manager: FileStorageManager):
        self.storage_manager = storage_manager
        self.max_file_size = 5 * 1024 * 1024 * 1024  # 5GB límite
        self.chunk_size = 8192  # 8KB chunks para streaming
        
        # ✅ CONFIGURACIONES ADAPTATIVAS
        self.small_file_threshold = 50000    # < 50K filas = cargar completo
        self.medium_file_threshold = 200000  # < 200K filas = cache inteligente
        self.large_file_chunk_size = 50000   # Chunks para archivos grandes
        
        self.file_services = {
            "csv": CSVService(),
            "xlsx": ExcelService(),
            "xls": ExcelService()
        }

    def get_loading_strategy(self, total_rows: int, total_columns: int) -> str:
        """Determina la estrategia de carga según el tamaño del archivo"""
        if total_rows <= self.small_file_threshold:
            strategy = "FULL_LOAD"  # Carga completa inmediata
        elif total_rows <= self.medium_file_threshold:
            strategy = "SMART_CACHE"  # Cache con muestras + carga bajo demanda
        else:
            strategy = "CHUNK_STREAM"  # Streaming chunks para máximo rendimiento
            
        print(f"📊 Estrategia: {strategy} (filas: {total_rows:,}, columnas: {total_columns})")
        return strategy

    async def upload_file(self, file: UploadFile) -> Dict[str, Any]:
        """Procesa archivos con verificación completa de guardado"""
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
        
        print(f"🔄 INICIANDO UPLOAD:")
        print(f"   - Archivo: {file.filename}")
        print(f"   - ID: {file_id}")
        print(f"   - Path destino: {file_path}")
        print(f"   - Upload dir: {upload_dir}")
        
        try:
            # ✅ GUARDADO CON VERIFICACIÓN COMPLETA
            await self._save_file_streaming(file, file_path)
            
            # ✅ TRIPLE VERIFICACIÓN POST-GUARDADO
            if not os.path.exists(file_path):
                raise HTTPException(
                    status_code=500, 
                    detail=f"FALLO CRÍTICO: Archivo no guardado en {file_path}"
                )
            
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                raise HTTPException(
                    status_code=500,
                    detail="FALLO CRÍTICO: Archivo guardado pero está vacío"
                )
            
            print(f"✅ VERIFICACIÓN EXITOSA:")
            print(f"   - Archivo existe: {os.path.exists(file_path)}")
            print(f"   - Tamaño final: {file_size:,} bytes")
            print(f"   - Path completo: {file_path}")
            
            # ✅ CONTINUAR SOLO SI EL ARCHIVO ESTÁ REALMENTE GUARDADO
            service = self.file_services[ext]
            file_obj = service.load(file_path)
            
            if ext == "csv":
                result = await self._process_csv_efficient(service, file_obj, file_id, file.filename, file_path)
            else:
                result = await self._process_excel_efficient(service, file_obj, file_id, file.filename, file_path)
            
            print(f"✅ UPLOAD COMPLETADO: {file.filename}")
            return result
                
        except Exception as e:
            print(f"❌ ERROR EN UPLOAD: {str(e)}")
            # Limpiar archivo parcial si existe
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    print(f"🗑️ Archivo parcial limpiado: {file_path}")
                except:
                    pass
            
            raise HTTPException(status_code=500, detail=f"Error procesando archivo: {str(e)}")

    async def _save_file_streaming(self, file: UploadFile, file_path: str):
        """Guarda archivo con verificación completa"""
        total_size = 0
        
        print(f"💾 Iniciando guardado: {file_path}")
        print(f"💾 Directorio destino: {os.path.dirname(file_path)}")
        print(f"💾 Directorio existe: {os.path.exists(os.path.dirname(file_path))}")
        
        try:
            # ✅ ASEGURAR QUE EL DIRECTORIO EXISTE
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # ✅ RESETEAR EL STREAM DEL ARCHIVO AL INICIO
            await file.seek(0)
            
            # ✅ USAR ESCRITURA SÍNCRONA MÁS CONFIABLE
            with open(file_path, 'wb') as f:
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
                    
                    f.write(chunk)  # ✅ Escritura síncrona directa
            
            # ✅ VERIFICACIÓN INMEDIATA POST-ESCRITURA
            if not os.path.exists(file_path):
                raise HTTPException(
                    status_code=500,
                    detail=f"ERROR: Archivo no se guardó en {file_path}"
                )
            
            final_size = os.path.getsize(file_path)
            print(f"💾 ✅ Archivo guardado exitosamente:")
            print(f"   - Path: {file_path}")
            print(f"   - Tamaño: {final_size:,} bytes")
            print(f"   - Verificado: {os.path.exists(file_path)}")
            
            if final_size == 0:
                raise HTTPException(
                    status_code=500,
                    detail="ERROR: Archivo guardado pero está vacío"
                )
            
        except Exception as e:
            print(f"💾 ❌ Error durante guardado: {e}")
            # Limpiar archivo parcial
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    print(f"💾 🗑️ Archivo parcial eliminado")
                except:
                    pass
            raise e

    async def _process_csv_efficient(self, service: CSVService, file_obj, file_id: str, original_name: str, file_path: str):
        """Procesa CSV de manera eficiente usando tus clases existentes"""
        
        # Usar los nuevos métodos eficientes
        file_info_data = service.get_file_info(file_obj)
        sheets = service.get_sheets(file_obj)
        columns = service.get_columns(file_obj)
        
        # ✅ Obtener muestra para cache inicial (solo para preview)
        sample_df = file_obj.get_sample_data(1000)  
        
        file_info = {
            "path": file_path,
            "ext": "csv",
            "original_name": original_name,
            "sheets": sheets,
            "default_sheet": None,
            "columns": columns,
            "total_rows": file_info_data['total_rows'],
            "separator": file_info_data['delimiter'],
            "strategy": self.get_loading_strategy(file_info_data['total_rows'], len(columns))
        }
        
        self.storage_manager.store_file_info(file_id, file_info)
        
        # Cachear muestra inicial solo para previews
        cache_key = self.storage_manager.generate_cache_key(file_id, "preview")
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
        total_rows = file_info_data['sheets_info'][default_sheet]['total_rows']
        
        # ✅ Obtener muestra para cache inicial (solo para preview)
        sample_df = file_obj.get_sample_data(default_sheet, 1000)
        
        file_info = {
            "path": file_path,
            "ext": "xlsx",
            "original_name": original_name,
            "sheets": sheets,
            "default_sheet": default_sheet,
            "columns": columns,
            "total_rows": total_rows,
            "strategy": self.get_loading_strategy(total_rows, len(columns))
        }
        
        self.storage_manager.store_file_info(file_id, file_info)
        
        # Cachear muestra inicial solo para previews
        cache_key = self.storage_manager.generate_cache_key(file_id, f"{default_sheet}_preview")
        self.storage_manager.cache_dataframe(cache_key, sample_df)
        
        return {
            "file_id": file_id,
            "columns": columns,
            "sheets": sheets,
            "total_rows": total_rows
        }

    def get_data_adaptive(
        self, 
        file_id: str, 
        sheet_name: Optional[str] = None,
        columns_needed: Optional[List[str]] = None  # ✅ Solo cargar columnas necesarias
    ):
        """Carga adaptativa según el tamaño del archivo - SIEMPRE COMPLETO"""
        file_info = self.storage_manager.get_file_info(file_id)
        if not file_info:
            raise HTTPException(status_code=404, detail="Archivo no encontrado")
        
        total_rows = file_info.get('total_rows', 0)
        total_columns = len(file_info.get('columns', []))
        strategy = file_info.get('strategy', self.get_loading_strategy(total_rows, total_columns))
        
        print(f"🔄 Cargando con estrategia: {strategy} ({total_rows:,} filas)")
        
        if strategy == "FULL_LOAD":
            # Archivos pequeños: carga completa
            return self._load_full_file(file_info, sheet_name, columns_needed)
        
        elif strategy == "SMART_CACHE":
            # Archivos medianos: verificar cache primero
            return self._load_with_smart_cache(file_id, file_info, sheet_name, columns_needed)
        
        else:  # CHUNK_STREAM
            # Archivos grandes: carga optimizada pero completa
            return self._load_full_optimized(file_info, sheet_name, columns_needed)

    def _load_full_file(self, file_info, sheet_name, columns_needed):
        """Carga completa para archivos pequeños"""
        service = self.file_services[file_info["ext"]]
        file_obj = service.load(file_info["path"])
        
        if file_info["ext"] == "csv":
            df = service.get_data(file_obj)
        else:
            target_sheet = sheet_name or file_info["default_sheet"]
            df = service.get_data(file_obj, sheet_name=target_sheet)
        
        # ✅ Filtrar solo columnas necesarias si se especifican
        if columns_needed and all(col in df.columns for col in columns_needed):
            df = df[columns_needed]
            print(f"📋 Columnas filtradas: {len(columns_needed)} de {len(df.columns)} originales")
        
        print(f"✅ Carga completa: {len(df):,} filas, {len(df.columns)} columnas")
        return df

    def _load_with_smart_cache(self, file_id, file_info, sheet_name, columns_needed):
        """Cache inteligente para archivos medianos"""
        target_sheet = sheet_name or file_info.get("default_sheet", "default")
        cache_key = f"{file_id}_{target_sheet}_full"
        
        # Verificar cache completo
        cached_df = self.storage_manager.get_cached_dataframe(cache_key)
        if cached_df is not None:
            print(f"✅ Obtenido desde cache: {len(cached_df):,} filas")
            if columns_needed and all(col in cached_df.columns for col in columns_needed):
                return cached_df[columns_needed]
            return cached_df
        
        # Cargar completo y cachear
        df = self._load_full_file(file_info, sheet_name, columns_needed)
        self.storage_manager.cache_dataframe(cache_key, df)
        return df

    def _load_full_optimized(self, file_info, sheet_name, columns_needed):
        """Carga optimizada pero completa para archivos grandes"""
        service = self.file_services[file_info["ext"]]
        file_obj = service.load(file_info["path"])
        
        print(f"🚀 Carga optimizada iniciando...")
        
        if file_info["ext"] == "csv":
            df = service.get_data(file_obj)
        else:
            target_sheet = sheet_name or file_info["default_sheet"]
            df = service.get_data(file_obj, sheet_name=target_sheet)
        
        # ✅ Filtrar columnas temprano para optimizar memoria
        if columns_needed and all(col in df.columns for col in columns_needed):
            df = df[columns_needed]
            print(f"📋 Columnas optimizadas: {len(columns_needed)} de {len(df.columns)} originales")
        
        print(f"✅ Carga optimizada completada: {len(df):,} filas")
        return df

    # ✅ MANTENER COMPATIBILIDAD CON CÓDIGO EXISTENTE
    def get_data_chunk(
        self, 
        file_id: str, 
        start_row: int = 0, 
        chunk_size: Optional[int] = None, 
        sheet_name: Optional[str] = None
    ):
        """Obtiene datos - si chunk_size es None, devuelve archivo completo"""
        if chunk_size is None:
            # ✅ Cargar archivo completo usando carga adaptativa
            return self.get_data_adaptive(file_id, sheet_name)
        
        # Comportamiento original para chunks específicos
        file_info = self.storage_manager.get_file_info(file_id)
        if not file_info:
            raise HTTPException(status_code=404, detail="Archivo no encontrado")
        
        file_path = file_info["path"]
        ext = file_info["ext"]
        
        try:
            service = self.file_services[ext]
            file_obj = service.load(file_path)
            
            if ext == "csv":
                return service.get_data_chunked(
                    file_obj, 
                    start_row=start_row, 
                    nrows=chunk_size
                )
            else:
                target_sheet = sheet_name or file_info["default_sheet"]
                return service.get_data_chunked(
                    file_obj,
                    target_sheet,
                    start_row=start_row,
                    nrows=chunk_size
                )
                
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error cargando chunk: {str(e)}")

    def get_full_data(
        self, 
        file_id: str, 
        sheet_name: Optional[str] = None
    ):
        """Carga el archivo completo sin límites"""
        return self.get_data_adaptive(file_id, sheet_name)

    def is_supported_file(self, filename: str) -> bool:
        """Verifica si el tipo de archivo es soportado"""
        ext = filename.split('.')[-1].lower()
        return ext in self.file_services

storage_manager = FileStorageManager()
upload_handler_instance = UploadHandler(storage_manager)
