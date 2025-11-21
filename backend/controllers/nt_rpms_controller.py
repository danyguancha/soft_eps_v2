"""
Controlador para procesar archivos NT RPMS y convertirlos a Parquet
"""


import os
import time
import duckdb
from typing import Dict, Any
from controllers.duckdb_controller.cache_controller import CacheController
from controllers.duckdb_controller.file_conversion_controller import FileConversionController
from controllers.extract_nt_rpms_controller import extract_nt_rpms_to_csv



class NTRPMSController:
    """Controlador para procesamiento NT RPMS con conversión a Parquet"""
    
    def __init__(self):
        """
        Inicializa el controlador sin dependencias externas
        """
        self.base_output_dir = "extract_info_nt"
        self.parquet_dir = "parquet_cache"
        self.metadata_dir = "metadata_cache"
        self.departamentos_file = "extract_info_nt/departamentos.xlsx"
        
        # Crear directorios si no existen
        os.makedirs(self.base_output_dir, exist_ok=True)
        os.makedirs(self.parquet_dir, exist_ok=True)
        os.makedirs(self.metadata_dir, exist_ok=True)


        # Validar existencia del archivo de departamentos
        if not os.path.exists(self.departamentos_file):
            print(f"⚠️ ADVERTENCIA: No se encontró {self.departamentos_file}")
            print(f"  Los códigos geográficos no serán enriquecidos")
        
        # Crear instancias de controladores
        self.cache_controller = CacheController(
            parquet_dir=self.parquet_dir,
            metadata_dir=self.metadata_dir
        )
        
        # Crear conexión DuckDB
        self.conn = duckdb.connect(database=':memory:')
        
        # Crear controlador de conversión
        self.file_converter = FileConversionController(
            conn=self.conn,
            parquet_dir=self.parquet_dir,
            cache_controller=self.cache_controller
        )
    
    def process_nt_rpms_folder(self, folder_path: str) -> Dict[str, Any]:
        """
        Procesa carpeta de archivos NT RPMS y convierte a Parquet
        
        Args:
            folder_path: Ruta de la carpeta con archivos Excel NT RPMS
        
        Returns:
            Diccionario con resultado del procesamiento y conversión
        """
        start_time = time.time()
        
        try:
            # Validar carpeta
            if not os.path.isdir(folder_path):
                return {
                    "success": False,
                    "error": f"La carpeta no existe: {folder_path}"
                }
            
            # Generar nombres de archivo
            timestamp = int(time.time())
            csv_filename = f"NT_RPMS_consolidado.csv"
            csv_path = os.path.join(self.base_output_dir, csv_filename)
            
            print("\n" + "="*60)
            print("INICIANDO PROCESAMIENTO NT RPMS")
            print("="*60)
            print(f"Carpeta origen: {folder_path}")
            print(f"Archivo CSV: {csv_path}")
            
            # PASO 1: Extraer datos de Excel a CSV
            print("\n[1/2] Extrayendo datos de archivos Excel...")
            extraction_result = extract_nt_rpms_to_csv(
                folder_path=folder_path,
                output_csv_path=csv_path,
                separator=';',
                departamentos_file=self.departamentos_file
            )
            
            if not extraction_result.get("success"):
                return {
                    "success": False,
                    "error": "Error en extracción de datos",
                    "details": extraction_result
                }
            
            extraction_time = time.time() - start_time
            print(f"\n✓ Extracción completada en {extraction_time:.2f}s")
            
            # PASO 2: Convertir CSV a Parquet
            print("\n[2/2] Convirtiendo CSV a Parquet...")
            conversion_start = time.time()
            
            # Generar file_id para el cache
            file_id = f"nt_rpms_{timestamp}"
            
            # Convertir a Parquet usando el controlador existente
            conversion_result = self.file_converter.convert_file_to_parquet(
                file_path=csv_path,
                file_id=file_id,
                original_name=csv_filename,
                ext='csv'
            )
            
            if not conversion_result.get("success"):
                return {
                    "success": False,
                    "error": "Error en conversión a Parquet",
                    "extraction_success": True,
                    "csv_path": csv_path,
                    "conversion_details": conversion_result
                }
            
            conversion_time = time.time() - conversion_start
            total_time = time.time() - start_time
            
            # Resultado final
            result = {
                "success": True,
                "csv_path": csv_path,
                "parquet_path": conversion_result["parquet_path"],
                "total_rows": conversion_result["total_rows"],
                "total_columns": len(conversion_result.get("columns", [])),
                "columns": conversion_result.get("columns", []),
                "extraction_summary": extraction_result.get("summary", {}),
                "timing": {
                    "extraction_time": extraction_time,
                    "conversion_time": conversion_time,
                    "total_time": total_time
                },
                "compression_info": {
                    "original_size_mb": conversion_result.get("original_size_mb", 0),
                    "parquet_size_mb": conversion_result.get("parquet_size_mb", 0),
                    "compression_ratio": conversion_result.get("compression_ratio", 0)
                },
                "from_cache": conversion_result.get("from_cache", False),
                "file_hash": conversion_result.get("file_hash", ""),
                "file_id": file_id
            }
            
            # Imprimir resumen final
            self._print_final_summary(result)
            
            return result
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return {
                "success": False,
                "error": f"Error inesperado: {str(e)}",
                "total_time": time.time() - start_time
            }
    
    def _print_final_summary(self, result: Dict[str, Any]):
        """Imprime resumen final del procesamiento"""
        print("\n" + "="*60)
        print("✓✓✓ PROCESAMIENTO COMPLETADO EXITOSAMENTE ✓✓✓")
        print("="*60)
        print(f"\nArchivo CSV: {result['csv_path']}")
        print(f"Archivo Parquet: {result['parquet_path']}")
        print(f"\nRegistros totales: {result['total_rows']:,}")
        print(f"Columnas: {result['total_columns']}")
        print(f"\nTiempos:")
        print(f"  - Extracción: {result['timing']['extraction_time']:.2f}s")
        print(f"  - Conversión: {result['timing']['conversion_time']:.2f}s")
        print(f"  - Total: {result['timing']['total_time']:.2f}s")
        print(f"\nCompresión:")
        print(f"  - Tamaño CSV: {result['compression_info']['original_size_mb']:.2f}MB")
        print(f"  - Tamaño Parquet: {result['compression_info']['parquet_size_mb']:.2f}MB")
        print(f"  - Ratio: {result['compression_info']['compression_ratio']:.1f}%")
        
        if result.get('from_cache'):
            print(f"\n📦 Resultado recuperado del cache")
        
        print("="*60 + "\n")
    
    def get_processing_status(self, file_hash: str) -> Dict[str, Any]:
        """
        Obtiene el estado de un procesamiento por su hash
        
        Args:
            file_hash: Hash del archivo procesado
        
        Returns:
            Estado del procesamiento y metadata
        """
        try:
            # Verificar si existe en cache
            cached_parquet = self.cache_controller.get_cached_parquet_path(file_hash)
            
            if not os.path.exists(cached_parquet):
                return {
                    "success": False,
                    "error": f"No se encontró procesamiento con hash: {file_hash}"
                }
            
            # Obtener metadata del cache
            if file_hash in self.cache_controller.file_cache:
                metadata = self.cache_controller.file_cache[file_hash]
            else:
                metadata = {}
            
            return {
                "success": True,
                "file_hash": file_hash,
                "parquet_path": cached_parquet,
                "metadata": metadata
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Error obteniendo estado: {str(e)}"
            }
    
    def list_processed_files(self) -> Dict[str, Any]:
        """
        Lista todos los archivos NT RPMS procesados
        
        Returns:
            Lista de archivos procesados con metadata
        """
        try:
            if not os.path.exists(self.base_output_dir):
                return {
                    "success": True,
                    "files": [],
                    "count": 0,
                    "message": "No se han procesado archivos NT RPMS"
                }
            
            processed_files = []
            
            # Listar archivos CSV
            csv_files = [f for f in os.listdir(self.base_output_dir) if f.endswith('.csv')]
            
            from datetime import datetime
            
            for csv_file in csv_files:
                csv_path = os.path.join(self.base_output_dir, csv_file)
                file_stat = os.stat(csv_path)
                
                # Buscar parquet correspondiente en cache
                parquet_found = False
                parquet_path = None
                
                # Buscar en parquet_cache
                if os.path.exists(self.parquet_dir):
                    for pq_file in os.listdir(self.parquet_dir):
                        if pq_file.endswith('.parquet') and csv_file.replace('.csv', '') in pq_file:
                            parquet_path = os.path.join(self.parquet_dir, pq_file)
                            parquet_found = True
                            break
                
                processed_files.append({
                    "filename": csv_file,
                    "csv_path": csv_path,
                    "parquet_path": parquet_path,
                    "has_parquet": parquet_found,
                    "size_mb": round(file_stat.st_size / (1024 * 1024), 2),
                    "created": datetime.fromtimestamp(file_stat.st_ctime).isoformat(),
                    "modified": datetime.fromtimestamp(file_stat.st_mtime).isoformat()
                })
            
            return {
                "success": True,
                "files": processed_files,
                "count": len(processed_files)
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Error listando archivos: {str(e)}"
            }



# Instancia global del controlador
nt_rpms_controller = NTRPMSController()
