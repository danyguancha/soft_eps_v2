import os
import pandas as pd
import time
import signal
import subprocess
import sys
from typing import Dict, Any
from collections import defaultdict
from utils.file_utils import FileUtils

class FileConversionController:
    """Controlador para conversión de archivos a Parquet"""
    
    def __init__(self, conn, parquet_dir: str, cache_controller):
        self.conn = conn
        self.parquet_dir = parquet_dir
        self.cache = cache_controller
        self.file_utils = FileUtils()

    def convert_file_to_parquet(self, file_path: str, file_id: str, original_name: str, ext: str) -> Dict[str, Any]:
        """Conversión con sistema de cache inteligente y progreso"""
        
        print(f"🚀 Procesamiento inteligente iniciado: {original_name}")
        start_time = time.time()
        
        # Verificar cache primero
        is_cached, file_hash, cache_metadata = self.cache.is_file_cached(file_path)
        
        if is_cached:
            return self._handle_cache_hit(file_hash, cache_metadata, start_time)
        
        # Conversión con timeout y progreso
        return self._convert_with_timeout_and_progress(file_path, file_hash, original_name, ext, start_time)

    def _handle_cache_hit(self, file_hash: str, cache_metadata: Dict[str, Any], start_time: float) -> Dict[str, Any]:
        """Maneja cuando se encuentra el archivo en cache con limpieza de columns"""
        print(f"⚡ USANDO CACHE EXISTENTE - Conversión evitada!")
        
        cached_parquet_path = self.cache.get_cached_parquet_path(file_hash)
        
        # Actualizar estadísticas de acceso
        self.cache.update_cache_access(file_hash)
        
        # Limpiar columns del cache
        columns = cache_metadata.get("columns", [])
        if isinstance(columns, list):
            columns = [str(col) if col is not None else f'col_{i}' 
                      for i, col in enumerate(columns)]
            print(f"🧹 Columns del cache limpiadas: {len(columns)} elementos")
        
        cache_time = time.time() - start_time
        
        print(f"✅ Cache utilizado en {cache_time:.3f}s:")
        print(f"   📊 {cache_metadata['total_rows']:,} filas, {len(columns)} columnas")
        print(f"   💾 Archivo original: {cache_metadata['original_size_mb']:.1f}MB")
        print(f"   🗜️ Archivo Parquet: {cache_metadata['parquet_size_mb']:.1f}MB")
        print(f"   📈 Accesos: {cache_metadata.get('access_count', 1)}")
        
        return {
            "parquet_path": cached_parquet_path,
            "total_rows": cache_metadata["total_rows"],
            "columns": columns,
            "conversion_time": cache_time,
            "original_size_mb": cache_metadata["original_size_mb"],
            "parquet_size_mb": cache_metadata["parquet_size_mb"],
            "compression_ratio": cache_metadata["compression_ratio"],
            "success": True,
            "method": "cache_hit",
            "from_cache": True,
            "file_hash": file_hash,
            "access_count": cache_metadata.get("access_count", 1)
        }

    def _convert_with_timeout_and_progress(self, file_path: str, file_hash: str, original_name: str, ext: str, start_time: float) -> Dict[str, Any]:
        """Conversión con path correcto y validación"""
        
        print(f"🔄 Archivo no encontrado en cache, iniciando conversión...")
        print(f"📁 Archivo fuente: {file_path}")
        
        # Verificar que el archivo fuente existe
        if not os.path.exists(file_path):
            raise Exception(f"Archivo fuente no encontrado: {file_path}")
        
        # Usar hash como nombre del archivo Parquet para evitar conflictos
        parquet_path = self.cache.get_cached_parquet_path(file_hash)
        print(f"📁 Archivo destino: {parquet_path}")
        
        try:
            # Configurar timeout basado en tamaño del archivo
            file_size_mb = os.path.getsize(file_path) / 1024 / 1024
            timeout_minutes = min(max(file_size_mb / 50, 5), 30)  # 5-30 minutos según tamaño
            
            print(f"⏱️ Timeout configurado: {timeout_minutes:.1f} minutos para {file_size_mb:.1f}MB")
            
            def timeout_handler(signum, frame):
                raise TimeoutError(f"Conversión tomó más de {timeout_minutes:.1f} minutos")
            
            # Solo usar timeout en sistemas Unix (no Windows)
            timeout_set = False
            if hasattr(signal, 'SIGALRM'):
                signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(int(timeout_minutes * 60))
                timeout_set = True
            
            try:
                if ext.lower() == 'csv':
                    result = self._convert_csv_to_parquet_robust(file_path, parquet_path)
                else:
                    result = self._convert_excel_to_parquet(file_path, parquet_path)
                
                if timeout_set:
                    signal.alarm(0)  # Cancelar timeout
                
                if not result["success"]:
                    return result
                
                return self._finalize_conversion(
                    parquet_path=parquet_path,
                    file_hash=file_hash,
                    original_name=original_name,
                    ext=ext,
                    result=result,
                    start_time=start_time,
                    original_file_path=file_path
                )
                    
            except TimeoutError as te:
                if timeout_set:
                    signal.alarm(0)
                print(f"⏱️ TIMEOUT: {te}")
                return {
                    "success": False,
                    "error": f"Timeout: El archivo es demasiado grande y tomó más de {timeout_minutes:.1f} minutos en procesarse. Considera dividirlo en archivos más pequeños."
                }
                
        except Exception as e:
            print(f"❌ Error en conversión con progreso: {e}")
            # Limpiar archivos parciales si existen
            if os.path.exists(parquet_path):
                try:
                    os.remove(parquet_path)
                    print(f"🗑️ Archivo Parquet parcial removido")
                except:
                    pass
            
            return {
                "success": False,
                "error": f"Error en conversión: {str(e)}"
            }

    def _convert_csv_to_parquet_robust(self, file_path: str, parquet_path: str) -> Dict[str, Any]:
        """Conversión robusta que maneja tipos mixtos sin errores"""
        
        # ESTRATEGIA 1: DuckDB nativo con all_varchar=true
        try:
            print("🔄 Estrategia 1: DuckDB nativo con all_varchar=true...")
            
            config = self.file_utils.detect_csv_encoding_and_separator(file_path)
            
            if config["success"]:
                conversion_sql = f"""
                COPY (
                    SELECT * FROM read_csv('{file_path}',
                        delim = '{config["separator"]}',
                        encoding = '{config["encoding"]}',
                        header = true,
                        all_varchar = true,
                        ignore_errors = true,
                        strict_mode = false
                    )
                ) TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'snappy')
                """
                
                self.conn.execute(conversion_sql)
                print("✅ Estrategia 1: Exitosa con DuckDB all_varchar")
                return {"success": True, "method": "duckdb_native_all_varchar"}
                
        except Exception as e:
            print(f"⚠️ Estrategia 1 falló: {e}")
        
        # ESTRATEGIA 2: Pandas ultra-robusto con tipos forzados
        try:
            print("🔄 Estrategia 2: Pandas ultra-robusto (tipos forzados)...")
            
            config = self.file_utils.detect_csv_encoding_and_separator(file_path)
            
            df = self.file_utils.robust_csv_read(
                file_path,
                encoding=config["encoding"],
                separator=config["separator"]
            )
            
            print(f"✅ Tipos de datos limpiados: todas las columnas son string")
            
            # Registrar en DuckDB
            self.conn.register('temp_csv_df', df)
            
            conversion_sql = f"""
            COPY (
                SELECT * FROM temp_csv_df
            ) TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'snappy')
            """
            
            self.conn.execute(conversion_sql)
            self.conn.execute("DROP VIEW IF EXISTS temp_csv_df")
            
            print("✅ Estrategia 2: Exitosa con pandas ultra-robusto")
            return {"success": True, "method": "pandas_ultra_robust_all_string"}
            
        except Exception as e:
            print(f"⚠️ Estrategia 2 falló: {e}")
        
        return {"success": False, "error": "Todas las estrategias de conversión CSV fallaron"}

    def _convert_excel_to_parquet(self, file_path: str, parquet_path: str) -> Dict[str, Any]:
        """Conversión de Excel con detección automática de hojas"""
        try:
            print("🔄 Conversión Excel ultra-optimizada iniciada...")
            
            # Obtener tamaño del archivo para determinar estrategia
            file_size_mb = os.path.getsize(file_path) / 1024 / 1024
            print(f"📊 Tamaño del archivo: {file_size_mb:.1f}MB")
            
            if file_size_mb > 100:  # Archivos > 100MB
                print("🚀 Archivo grande detectado - Usando estrategias ultra-optimizadas")
                return self._convert_large_excel_to_parquet(file_path, parquet_path, file_size_mb)
            else:
                # Para archivos menores, usar método estándar
                return self._convert_standard_excel_to_parquet(file_path, parquet_path)
                
        except Exception as e:
            error_msg = f"Error general convirtiendo Excel: {str(e)}"
            return {"success": False, "error": error_msg}

    def _convert_large_excel_to_parquet(self, file_path: str, parquet_path: str, file_size_mb: float) -> Dict[str, Any]:
        """Estrategias específicas para archivos Excel gigantes"""
        
        # ESTRATEGIA 1: DuckDB directo
        try:
            print(f"🔄 Estrategia Ultra 1: DuckDB directo...")
            
            conversion_sql = f"""
            COPY (
                SELECT * FROM read_xlsx('{file_path.replace('\\', '/')}', all_varchar = true)
            ) TO '{parquet_path.replace('\\', '/')}' (FORMAT 'parquet', COMPRESSION 'snappy')
            """
            
            print("🔄 Ejecutando conversión DuckDB...")
            self.conn.execute(conversion_sql)
            
            if not os.path.exists(parquet_path):
                raise Exception(f"DuckDB no creó el archivo Parquet: {parquet_path}")
            
            parquet_size = os.path.getsize(parquet_path)
            if parquet_size == 0:
                raise Exception("El archivo Parquet está vacío")
            
            print(f"✅ Estrategia Ultra 1: Exitosa con DuckDB directo")
            print(f"   📊 Archivo Parquet creado: {parquet_size/1024/1024:.1f}MB")
            
            return {"success": True, "method": "duckdb_direct"}
                
        except Exception as e1:
            print(f"⚠️ Estrategia Ultra 1 falló: {e1}")
            
            # Limpiar archivo parcial si existe
            if os.path.exists(parquet_path):
                try:
                    os.remove(parquet_path)
                except:
                    pass
        
        # ESTRATEGIA 2: Engine calamine
        if self._install_calamine_if_needed():
            try:
                print(f"🔄 Estrategia Ultra 2: Pandas con calamine...")
                
                df_excel = pd.read_excel(
                    file_path,
                    engine='calamine',
                    dtype=str,
                    na_filter=False
                )
                
                print(f"✅ Calamine cargó: {len(df_excel)} filas, {len(df_excel.columns)} columnas")
                
                # Limpieza básica
                for col in df_excel.columns:
                    df_excel[col] = df_excel[col].astype(str)
                
                df_excel = df_excel.fillna('').replace(['nan', '<NA>', 'None'], '')
                
                # Registrar en DuckDB y convertir
                self.conn.register('temp_large_excel_df', df_excel)
                
                conversion_sql = f"""
                COPY (
                    SELECT * FROM temp_large_excel_df
                ) TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'snappy')
                """
                
                self.conn.execute(conversion_sql)
                self.conn.execute("DROP VIEW IF EXISTS temp_large_excel_df")
                
                print(f"✅ Estrategia Ultra 2: Exitosa con calamine")
                return {"success": True, "method": "calamine"}
                
            except Exception as e2:
                print(f"⚠️ Estrategia Ultra 2 falló: {e2}")
        
        # Si todas las estrategias fallan
        error_msg = f"Todas las estrategias ultra-optimizadas fallaron para archivo de {file_size_mb}MB"
        return {"success": False, "error": error_msg}

    def _convert_standard_excel_to_parquet(self, file_path: str, parquet_path: str) -> Dict[str, Any]:
        """Método estándar para archivos Excel normales"""
        try:
            df_excel = pd.read_excel(
                file_path, 
                engine='openpyxl',
                dtype=str,
                na_filter=False,
                keep_default_na=False
            )
            
            # Limpieza estándar
            df_excel = df_excel.astype(str)
            df_excel = df_excel.replace(['nan', '<NA>', 'None', 'NaT'], '')
            df_excel = df_excel.fillna('')
            
            print(f"✅ Excel estándar procesado: {len(df_excel)} filas, {len(df_excel.columns)} columnas")
            
            # Registrar en DuckDB y convertir
            self.conn.register('temp_excel_df', df_excel)
            
            conversion_sql = f"""
            COPY (
                SELECT * FROM temp_excel_df
            ) TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'snappy')
            """
            
            self.conn.execute(conversion_sql)
            self.conn.execute("DROP VIEW IF EXISTS temp_excel_df")
            
            return {"success": True, "method": "standard"}
            
        except Exception as e:
            return {"success": False, "error": f"Error en conversión estándar: {str(e)}"}

    def _install_calamine_if_needed(self):
        """Instala calamine automáticamente si no está disponible"""
        try:
            
            print("✅ Calamine ya está disponible")
            return True
        except ImportError:
            print("🔄 Instalando python-calamine para máximo rendimiento...")
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", "python-calamine"])
                print("✅ Calamine instalado exitosamente")
                return True
            except Exception as e:
                print(f"⚠️ No se pudo instalar calamine: {e}")
                return False

    def _finalize_conversion(self, parquet_path: str, file_hash: str, original_name: str, ext: str, result: Dict[str, Any], start_time: float, original_file_path: str) -> Dict[str, Any]:
        """Finaliza la conversión con path correcto del archivo original"""
        
        # Verificar que el archivo Parquet se haya creado correctamente
        if not os.path.exists(parquet_path):
            raise Exception(f"El archivo Parquet no se creó: {parquet_path}")
        
        print(f"✅ Archivo Parquet creado exitosamente: {os.path.getsize(parquet_path)/1024/1024:.1f}MB")
        
        # Obtener estadísticas del Parquet generado
        try:
            stats_sql = f"SELECT COUNT(*) as total_rows FROM read_parquet('{parquet_path}')"
            total_rows = self.conn.execute(stats_sql).fetchone()[0]
            print(f"📊 Parquet validado: {total_rows:,} filas")
        except Exception as e:
            raise Exception(f"El archivo Parquet está corrupto: {e}")
        
        # Obtener columnas
        try:
            columns_sql = f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')"
            columns_result = self.conn.execute(columns_sql).fetchall()
            columns = [row[0] for row in columns_result]
            print(f"📋 Columnas detectadas: {len(columns)}")
        except Exception as e:
            raise Exception(f"No se pudieron obtener las columnas del Parquet: {e}")
        
        conversion_time = time.time() - start_time
        
        # Usar el path correcto del archivo original
        if os.path.exists(original_file_path):
            original_size = os.path.getsize(original_file_path)
            print(f"📁 Archivo original encontrado: {original_size/1024/1024:.1f}MB")
        else:
            print(f"⚠️ Archivo original no encontrado: {original_file_path}")
            # Usar estimación basada en el tamaño del Parquet
            parquet_size = os.path.getsize(parquet_path)
            original_size = parquet_size * 4  # Estimación conservadora
            print(f"📊 Tamaño estimado desde Parquet: {original_size/1024/1024:.1f}MB")
        
        parquet_size = os.path.getsize(parquet_path)
        compression_ratio = (1 - parquet_size / original_size) * 100
        
        # Guardar en cache para futuras utilizaciones
        cache_metadata = {
            "original_name": original_name,
            "extension": ext,
            "total_rows": total_rows,
            "columns": columns,
            "conversion_time": conversion_time,
            "original_size_mb": original_size/1024/1024,
            "parquet_size_mb": parquet_size/1024/1024,
            "compression_ratio": compression_ratio,
            "method": result.get("method", "unknown"),
            "parquet_path": parquet_path,
            "validated": True
        }
        
        self.cache.save_cache_metadata(file_hash, cache_metadata)
        
        print(f"✅ Conversión y cache completados en {conversion_time:.2f}s:")
        print(f"   📊 {total_rows:,} filas, {len(columns)} columnas")
        print(f"   💾 Tamaño original: {original_size/1024/1024:.1f}MB")
        print(f"   🗜️ Tamaño Parquet: {parquet_size/1024/1024:.1f}MB")
        print(f"   📉 Compresión: {compression_ratio:.1f}%")
        print(f"   🔑 Hash: {file_hash}")
        
        return {
            "parquet_path": parquet_path,
            "total_rows": total_rows,
            "columns": columns,
            "conversion_time": conversion_time,
            "original_size_mb": original_size/1024/1024,
            "parquet_size_mb": parquet_size/1024/1024,
            "compression_ratio": compression_ratio,
            "success": True,
            "method": result.get("method", "unknown"),
            "from_cache": False,
            "file_hash": file_hash,
            "cached": True,
            "validated": True
        }
