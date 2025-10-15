# controllers/files_controllers/upload_handler.py 
import os
import time
from fastapi import UploadFile, HTTPException
from typing import Dict, Any, Optional, List
from services.csv_service import CSVService
from services.excel_service import ExcelService
from controllers.files_controllers.storage_manager import FileStorageManager
from services.duckdb_service.duckdb_service import duckdb_service

class UploadHandler:
    def __init__(self, storage_manager: FileStorageManager):
        self.storage_manager = storage_manager
        self.max_file_size = 5 * 1024 * 1024 * 1024  # 5GB límite
        self.chunk_size = 8192
        
        self.file_services = {
            "csv": CSVService(),
            "xlsx": ExcelService(),
            "xls": ExcelService()
        }

    async def upload_file(self, file: UploadFile) -> Dict[str, Any]:
        """Maneja la carga de archivos con detección completa de hojas en archivos Excel"""
        if not file.filename:
            raise HTTPException(status_code=400, detail="Nombre de archivo requerido")
            
        ext = file.filename.split('.')[-1].lower()
        if ext not in self.file_services:
            raise HTTPException(
                status_code=400, 
                detail=f"Tipo de archivo no soportado: {ext}"
            )
        
        original_filename = file.filename
        file_id = original_filename
        
        # Crear archivo temporal
        temp_dir = self.storage_manager.ensure_upload_directory()
        temp_filename = f"temp_{int(time.time())}_{original_filename}"
        temp_file_path = os.path.join(temp_dir, temp_filename)
        
        try:
            # Guardar archivo temporal
            await self._save_file_streaming(file, temp_file_path)
            # PROCESAR SEGÚN EL TIPO DE ARCHIVO
            columns_list = []
            sheets_list = []
            default_sheet = None
            total_rows = 0
            processing_method = "Standard"
            sheet_detection_time = 0.0
            
            if ext in ['xlsx', 'xls']:
                # PROCESAMIENTO EXCEL (tu código existente)
                sheet_start = time.time()
                try:
                    sheet_info = duckdb_service.get_excel_sheets(temp_file_path)
                    sheet_detection_time = time.time() - sheet_start
                    
                    if sheet_info["success"]:
                        sheets_list = sheet_info["sheets"]
                        default_sheet = sheet_info["default_sheet"]
                    else:
                        sheets_list = ["Sheet1"]
                        default_sheet = "Sheet1"
                        
                except Exception as e:
                    print(f"Error detectando hojas Excel: {e}")
                    sheets_list = ["Sheet1"]
                    default_sheet = "Sheet1"
                
                processing_method = "Excel_Detected"
                
            elif ext == 'csv':
                csv_result = await self._detect_and_process_csv_robust(temp_file_path)
                
                if csv_result["success"]:
                    columns_list = csv_result["columns"]
                    total_rows = csv_result["total_rows"]
                    processing_method = csv_result["method"]
                    print(f"CSV procesado: {len(columns_list)} columnas, {total_rows} filas")
                else:
                    print(f"Error procesando CSV, usando valores por defecto")
                    columns_list = []
                    total_rows = 0
                    processing_method = "CSV_Failed"
            
            # Mover archivo a ubicación final
            file_info = {
                "ext": ext,
                "original_name": original_filename,
                "sheets": sheets_list,
                "default_sheet": default_sheet,
                "upload_type": "technical_note",
                "user_uploaded": True
            }
            
            final_file_path = self.storage_manager.store_file_with_original_name(
                source_file_path=temp_file_path,
                original_filename=original_filename,
                file_info=file_info,
                overwrite=True
            )
            # Limpiar archivo temporal
            try:
                if os.path.exists(temp_file_path):
                    os.remove(temp_file_path)
            except:
                pass
            
            print(f"Archivo guardado como: {final_file_path}")
            
            # Conversión a Parquet
            parquet_result = duckdb_service.convert_file_to_parquet(
                final_file_path, file_id, original_filename, ext
            )
            
            if not parquet_result["success"]:
                error_msg = parquet_result["error"]
                if "Timeout" in error_msg:
                    raise HTTPException(status_code=413, detail=f"Archivo demasiado grande: {error_msg}")
                else:
                    raise HTTPException(status_code=500, detail=error_msg)
            
            # USAR DATOS DEL PARQUET SI NO SE OBTUVIERON DEL CSV
            if not columns_list and parquet_result.get("columns"):
                columns_list = parquet_result["columns"]
            if not total_rows and parquet_result.get("total_rows"):
                total_rows = parquet_result["total_rows"]
            
            # Carga lazy
            table_name = duckdb_service.load_parquet_lazy(
                file_id, parquet_result["parquet_path"]
            )
            
            # RESPUESTA GARANTIZADA CON TODOS LOS CAMPOS
            response = {
                "file_id": file_id,
                "filename": original_filename,
                "columns": columns_list,                    
                "sheets": sheets_list,                      
                "default_sheet": default_sheet,             
                "total_rows": total_rows,                   
                "is_excel": ext in ['xlsx', 'xls'],        
                "has_sheets": len(sheets_list) > 1,         
                "sheet_count": len(sheets_list),            
                "sheet_detection_time": sheet_detection_time, 
                "ultra_fast": True,                      
                "engine": "DuckDB + Robust Encoding",       
                "file_size_mb": round(os.path.getsize(final_file_path) / 1024 / 1024, 2),
                "processing_method": processing_method,      
                "from_cache": parquet_result.get("from_cache", False) 
            }
            
            return response
            
        except Exception as e:
            # Limpiar archivos en caso de error
            cleanup_files = [temp_file_path]
            try:
                final_path = self.storage_manager.get_file_path(original_filename)
                if os.path.exists(final_path):
                    cleanup_files.append(final_path)
            except:
                pass
                
            for cleanup_file in cleanup_files:
                try:
                    if os.path.exists(cleanup_file):
                        os.remove(cleanup_file)
                except:
                    pass
            
            try:
                self.storage_manager.remove_file_by_original_name(original_filename)
            except:
                pass
            
            raise HTTPException(status_code=500, detail=f"Error procesando archivo: {str(e)}")

    async def _save_file_streaming(self, file: UploadFile, file_path: str):
        """Guarda archivo con verificación completa y progreso"""
        total_size = 0
        
        try:
            # ASEGURAR QUE EL DIRECTORIO EXISTE
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            await file.seek(0)
            
            print(f"📤 Guardando archivo temporal: {os.path.basename(file_path)}")
            
            with open(file_path, 'wb') as f:
                while True:
                    chunk = await file.read(self.chunk_size)
                    if not chunk:
                        break
                    
                    total_size += len(chunk)
                    
                    if total_size > self.max_file_size:
                        raise HTTPException(
                            status_code=413, 
                            detail="Archivo excede el tamaño máximo permitido (5GB)"
                        )
                    
                    f.write(chunk)
                    
                    # Mostrar progreso para archivos grandes
                    if total_size % (50 * 1024 * 1024) == 0:  # Cada 50MB
                        print(f"📤 Guardando: {total_size/1024/1024:.1f}MB")
            
            if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
                raise HTTPException(
                    status_code=500,
                    detail="ERROR: Archivo no se guardó correctamente"
                )
            
            print(f"Archivo temporal guardado: {total_size/1024/1024:.1f}MB")
            
        except Exception as e:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except:
                    pass
            raise e

    def get_data_ultra_fast(
        self, 
        file_id: str, 
        filters: Optional[List[Dict[str, Any]]] = None,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "ASC",
        page: int = 1,
        page_size: int = 1000,
        selected_columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """ULTRA-OPTIMIZADO: Consulta directa usando lazy load con nombres originales"""
        
        # file_id ahora es el nombre original del archivo
        return duckdb_service.query_data_ultra_fast(
            file_id=file_id,
            filters=filters,
            search=search,
            sort_by=sort_by,
            sort_order=sort_order,
            page=page,
            page_size=page_size,
            selected_columns=selected_columns
        )

    def get_file_info(self, file_id: str) -> Optional[Dict[str, Any]]:
        """Obtiene información del archivo usando nombre original como ID"""
        return self.storage_manager.get_file_info(file_id)

    def get_file_info_by_filename(self, filename: str) -> Optional[Dict[str, Any]]:
        """Obtiene información del archivo por nombre de archivo (compatibilidad)"""
        return self.storage_manager.get_file_info_by_original_name(filename)

    def delete_file(self, file_id: str) -> bool:
        """Elimina archivo usando nombre original como ID"""
        try:
            # Limpiar de DuckDB service también
            duckdb_service.cleanup_file_data(file_id)
            
            # Eliminar del storage
            return self.storage_manager.remove_file(file_id)
        except Exception as e:
            print(f"❌ Error eliminando archivo {file_id}: {e}")
            return False

    def list_uploaded_files(self) -> List[Dict[str, Any]]:
        """Lista todos los archivos subidos en technical_note"""
        return self.storage_manager.list_technical_files()

    def file_exists(self, filename: str) -> bool:
        """Verifica si un archivo existe en technical_note"""
        return self.storage_manager.file_exists(filename)

    def get_file_path(self, filename: str) -> str:
        """Obtiene la ruta completa de un archivo"""
        return self.storage_manager.get_file_path(filename)

    # MANTENER COMPATIBILIDAD CON MÉTODOS EXISTENTES
    def get_data_adaptive(self, file_id: str, sheet_name: Optional[str] = None, columns_needed: Optional[List[str]] = None):
        """Redirige a método ultra-optimizado (compatibilidad)"""
        result = self.get_data_ultra_fast(
            file_id=file_id,
            selected_columns=columns_needed,
            page=1,
            page_size=100000  # Obtener muchos registros
        )
        return result["data"]

    def is_supported_file(self, filename: str) -> bool:
        """Verifica si el tipo de archivo es soportado"""
        if not filename:
            return False
        
        try:
            ext = filename.split('.')[-1].lower()
            return ext in self.file_services
        except:
            return False

    def get_supported_extensions(self) -> List[str]:
        """Obtiene lista de extensiones soportadas"""
        return list(self.file_services.keys())

    def clean_filename(self, filename: str) -> str:
        """Limpia el nombre del archivo para evitar problemas del sistema operativo"""
        import re
        
        # Eliminar caracteres problemáticos
        cleaned = re.sub(r'[<>:"/\\|?*]', '_', filename)
        
        # Limitar longitud
        name, ext = os.path.splitext(cleaned)
        if len(name) > 200:  # Dejar espacio para extensión
            name = name[:200]
        
        return f"{name}{ext}"

    def generate_unique_filename(self, filename: str) -> str:
        """Genera nombre único si el archivo ya existe"""
        if not self.file_exists(filename):
            return filename
        
        name, ext = os.path.splitext(filename)
        counter = 1
        
        while self.file_exists(f"{name}_{counter}{ext}"):
            counter += 1
        
        return f"{name}_{counter}{ext}"

    def get_upload_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas de archivos subidos"""
        try:
            files = self.list_uploaded_files()
            total_size = sum(f.get("file_size", 0) for f in files)
            
            extensions = {}
            for f in files:
                ext = f.get("extension", "unknown")
                extensions[ext] = extensions.get(ext, 0) + 1
            
            return {
                "total_files": len(files),
                "total_size_mb": total_size / 1024 / 1024,
                "extensions": extensions,
                "technical_note_path": self.storage_manager.get_technical_note_path()
            }
        except Exception as e:
            return {
                "error": str(e),
                "total_files": 0,
                "total_size_mb": 0,
                "extensions": {}
            }
        
    async def _detect_and_process_csv_robust(self, file_path: str) -> Dict[str, Any]:
        """Detecta encoding y procesa CSV de forma robusta"""
        try:
            import chardet
            import pandas as pd
            
            # DETECTAR ENCODING AUTOMÁTICAMENTE
            with open(file_path, 'rb') as raw_file:
                raw_data = raw_file.read(10000)  # Primeros 10KB para detectar
                encoding_result = chardet.detect(raw_data)
                detected_encoding = encoding_result.get('encoding', 'utf-8')
                confidence = encoding_result.get('confidence', 0)
            
            print(f"🔍 Encoding detectado: {detected_encoding} (confianza: {confidence:.2f})")
            
            # LISTA DE ENCODINGS A PROBAR
            encodings_to_try = [
                detected_encoding,
                'utf-8',
                'latin-1',
                'cp1252',
                'iso-8859-1',
                'utf-8-sig'
            ]
            
            # Eliminar None y duplicados
            encodings_to_try = list(dict.fromkeys([enc for enc in encodings_to_try if enc]))
            
            # PROBAR ENCODINGS HASTA ENCONTRAR UNO QUE FUNCIONE
            for encoding in encodings_to_try:
                try:
                    print(f"🔄 Intentando encoding: {encoding}")
                    # Leer solo una muestra primero para validar
                    df_sample = pd.read_csv(file_path, encoding=encoding, nrows=100)
                    
                    # Si llegamos aquí, el encoding funciona
                    print(f"Encoding exitoso: {encoding}")
                    
                    # Obtener columnas
                    columns_list = df_sample.columns.tolist()
                    
                    # Contar filas total
                    try:
                        with open(file_path, 'r', encoding=encoding) as f:
                            total_rows = sum(1 for line in f) - 1  # -1 para header
                    except:
                        total_rows = len(df_sample)
                    
                    return {
                        "success": True,
                        "columns": columns_list,
                        "total_rows": total_rows,
                        "encoding_used": encoding,
                        "method": f"CSV_Robust_{encoding}"
                    }
                    
                except Exception as e:
                    print(f"Error con encoding {encoding}: {str(e)[:100]}")
                    continue
            
            # Si todos los encodings fallaron
            print(f"❌ Todos los encodings fallaron para el CSV")
            return {
                "success": False,
                "columns": [],
                "total_rows": 0,
                "encoding_used": "failed",
                "method": "CSV_Failed"
            }
            
        except ImportError:
            print("chardet no disponible, usando encoding básico")
            # Fallback sin chardet
            try:
                df = pd.read_csv(file_path, encoding='utf-8', nrows=100)
                return {
                    "success": True,
                    "columns": df.columns.tolist(),
                    "total_rows": len(df),
                    "encoding_used": "utf-8",
                    "method": "CSV_Basic"
                }
            except:
                return {
                    "success": False,
                    "columns": [],
                    "total_rows": 0,
                    "encoding_used": "failed",
                    "method": "CSV_Failed"
                }
        
        except Exception as e:
            print(f"❌ Error crítico procesando CSV: {e}")
            return {
                "success": False,
                "columns": [],
                "total_rows": 0,
                "encoding_used": "error",
                "method": "CSV_Error"
            }

# INSTANCIAS GLOBALES
storage_manager = FileStorageManager()
upload_handler_instance = UploadHandler(storage_manager)
