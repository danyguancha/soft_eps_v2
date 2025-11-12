# controllers/file_conversion_controller.py
import os
import pandas as pd
import time
import signal
import subprocess
import sys
import threading
from typing import Dict, Any, Optional
from utils.file_utils import FileUtils


class TimeoutException(Exception):
    """Excepción personalizada para timeout"""
    pass


class FileConversionController:
    """Controlador para conversión de archivos a Parquet"""
    
    def __init__(self, conn, parquet_dir: str, cache_controller):
        self.conn = conn
        self.parquet_dir = parquet_dir
        self.cache = cache_controller
        self.file_utils = FileUtils()
        self._temp_views = []  # Track de vistas temporales para limpieza

    def _cleanup_temp_views(self):
        """Limpia todas las vistas temporales creadas"""
        for view_name in self._temp_views:
            try:
                self.conn.execute(f"DROP VIEW IF EXISTS {view_name}")
                print(f"✓ Vista temporal {view_name} eliminada")
            except Exception as e:
                print(f"Error limpiando vista {view_name}: {e}")
        self._temp_views.clear()

    def convert_file_to_parquet(self, file_path: str, file_id: str, original_name: str, ext: str) -> Dict[str, Any]:
        """Conversión con sistema de cache inteligente y progreso"""
        start_time = time.time()
        
        # Verificar cache primero
        is_cached, file_hash, cache_metadata = self.cache.is_file_cached(file_path)
        
        if is_cached:
            return self._handle_cache_hit(file_hash, cache_metadata, start_time)
        
        # Conversión con timeout y progreso
        return self._convert_with_timeout_and_progress(file_path, file_hash, original_name, ext, start_time)

    def _handle_cache_hit(self, file_hash: str, cache_metadata: Dict[str, Any], start_time: float) -> Dict[str, Any]:
        """Maneja cuando se encuentra el archivo en cache con limpieza de columns"""        
        cached_parquet_path = self.cache.get_cached_parquet_path(file_hash)
        
        # Actualizar estadísticas de acceso
        self.cache.update_cache_access(file_hash)
        
        # Limpiar columns del cache
        columns = cache_metadata.get("columns", [])
        if isinstance(columns, list):
            columns = [str(col) if col is not None else f'col_{i}' 
                      for i, col in enumerate(columns)]
            print(f"✓ Columns del cache limpiadas: {len(columns)} elementos")
        
        cache_time = time.time() - start_time

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
        """Conversión con timeout y validación"""
        
        # Verificar que el archivo fuente existe
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"El archivo fuente no existe: {file_path}")
        
        parquet_path = self.cache.get_cached_parquet_path(file_hash)
        
        try:
            # Configurar timeout basado en tamaño del archivo
            file_size_mb = os.path.getsize(file_path) / 1024 / 1024
            timeout_minutes = min(max(file_size_mb / 50, 5), 30)
            
            print(f"📁 Archivo: {file_size_mb:.2f}MB | Timeout: {timeout_minutes:.1f}min")
            
            # Wrapper para conversión con timeout
            result_container = {"result": None, "error": None, "completed": False}
            
            def convert_worker():
                try:
                    if ext.lower() == 'csv':
                        result_container["result"] = self._convert_csv_to_parquet_robust(file_path, parquet_path)
                    else:
                        result_container["result"] = self._convert_excel_to_parquet(file_path, parquet_path)
                    result_container["completed"] = True
                except Exception as e:
                    result_container["error"] = str(e)
                    result_container["completed"] = True
            
            # Ejecutar en thread con timeout
            thread = threading.Thread(target=convert_worker, daemon=True)
            thread.start()
            thread.join(timeout=timeout_minutes * 60)
            
            # Verificar si completó
            if not result_container["completed"]:
                print(f"⏱️ TIMEOUT: Conversión excedió {timeout_minutes:.1f} minutos")
                self._cleanup_temp_views()
                return {
                    "success": False,
                    "error": f"Timeout: El archivo tomó más de {timeout_minutes:.1f} minutos. Divide el archivo en partes más pequeñas."
                }
            
            # Verificar si hubo error
            if result_container["error"]:
                print(f"Error en worker: {result_container['error']}")
                self._cleanup_temp_views()
                return {
                    "success": False,
                    "error": f"Error en conversión: {result_container['error']}"
                }
            
            result = result_container["result"]
            
            if not result or not result.get("success"):
                self._cleanup_temp_views()
                return result or {"success": False, "error": "Conversión falló sin resultado"}
            
            # Finalizar conversión
            return self._finalize_conversion(
                parquet_path=parquet_path,
                file_hash=file_hash,
                original_name=original_name,
                ext=ext,
                result=result,
                start_time=start_time,
                original_file_path=file_path
            )
                    
        except Exception as e:
            print(f"Error general en conversión: {e}")
            self._cleanup_temp_views()
            
            # Limpiar archivos parciales
            if os.path.exists(parquet_path):
                try:
                    os.remove(parquet_path)
                    print(f"🗑️ Archivo parcial eliminado: {parquet_path}")
                except Exception:
                    pass
            
            return {
                "success": False,
                "error": f"Error en conversión: {str(e)}"
            }

    def _convert_csv_to_parquet_robust(self, file_path: str, parquet_path: str) -> Dict[str, Any]:
        """Conversión robusta con limpieza de recursos mejorada"""
        
        print("\n" + "="*60)
        print("🔄 INICIANDO CONVERSIÓN CSV → PARQUET")
        print("="*60)
        
        # ESTRATEGIA 1: DuckDB nativo puro (más rápido)
        print("\n📌 Estrategia 1: DuckDB nativo con all_varchar=true")
        try:
            config = self.file_utils.detect_csv_encoding_and_separator(file_path)
            
            if config["success"]:
                print(f"✓ Encoding: {config['encoding']}, Separador: '{config['separator']}'")
                
                conversion_sql = f"""
                COPY (
                    SELECT * FROM read_csv('{file_path}',
                        delim = '{config["separator"]}',
                        encoding = '{config["encoding"]}',
                        header = true,
                        all_varchar = true,
                        ignore_errors = true,
                        strict_mode = false,
                        parallel = true
                    )
                ) TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'snappy')
                """
                
                print("Ejecutando conversión DuckDB...")
                self.conn.execute(conversion_sql)
                
                # Verificar que se creó el archivo
                if os.path.exists(parquet_path) and os.path.getsize(parquet_path) > 0:
                    print("Estrategia 1: EXITOSA")
                    return {"success": True, "method": "duckdb_native"}
                else:
                    print("Archivo Parquet vacío o no creado")
                    raise ValueError("Parquet vacío")
                
        except Exception as e:
            print(f"Estrategia 1 falló: {e}")
            # Limpiar archivo parcial
            if os.path.exists(parquet_path):
                try:
                    os.remove(parquet_path)
                except Exception:
                    pass
        
        # ESTRATEGIA 2: Pandas con registro temporal en DuckDB
        print("\n📌 Estrategia 2: Pandas → DuckDB con limpieza de recursos")
        view_name = None
        try:
            config = self.file_utils.detect_csv_encoding_and_separator(file_path)
            
            print("Leyendo CSV con Pandas...")
            df = self.file_utils.robust_csv_read(
                file_path,
                encoding=config["encoding"],
                separator=config["separator"]
            )
            
            print(f"✓ DataFrame creado: {len(df)} filas, {len(df.columns)} columnas")
            
            # Nombre único para la vista temporal
            view_name = f'temp_csv_df_{int(time.time() * 1000)}'
            self._temp_views.append(view_name)
            
            print(f"Registrando vista temporal: {view_name}")
            self.conn.register(view_name, df)
            
            # Liberar DataFrame de memoria
            del df
            
            print("Convirtiendo a Parquet...")
            conversion_sql = f"""
            COPY (
                SELECT * FROM {view_name}
            ) TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'snappy')
            """
            
            self.conn.execute(conversion_sql)
            
            # Limpiar vista inmediatamente después de usar
            print("Limpiando vista temporal...")
            self.conn.execute(f"DROP VIEW IF EXISTS {view_name}")
            if view_name in self._temp_views:
                self._temp_views.remove(view_name)
            
            # Verificar resultado
            if os.path.exists(parquet_path) and os.path.getsize(parquet_path) > 0:
                print("Estrategia 2: EXITOSA")
                return {"success": True, "method": "pandas_robust"}
            else:
                raise ValueError("Parquet vacío después de conversión")
            
        except Exception as e:
            print(f"Estrategia 2 falló: {e}")
            
            # Limpieza exhaustiva
            if view_name:
                try:
                    self.conn.execute(f"DROP VIEW IF EXISTS {view_name}")
                    if view_name in self._temp_views:
                        self._temp_views.remove(view_name)
                    print(f"✓ Vista {view_name} limpiada")
                except Exception as cleanup_error:
                    print(f"Error limpiando vista: {cleanup_error}")
            
            # Limpiar archivo parcial
            if os.path.exists(parquet_path):
                try:
                    os.remove(parquet_path)
                except Exception:
                    pass
        
        # ESTRATEGIA 3: Fallback con chunks (para archivos muy grandes)
        print("\n📌 Estrategia 3: Pandas con chunks + concatenación")
        try:
            config = self.file_utils.detect_csv_encoding_and_separator(file_path)
            
            print("Leyendo CSV en chunks...")
            chunk_size = 50000  # 50k filas por chunk
            chunks = []
            
            for i, chunk in enumerate(pd.read_csv(
                file_path,
                encoding=config["encoding"],
                sep=config["separator"],
                dtype=str,
                na_filter=False,
                chunksize=chunk_size,
                low_memory=False,
                on_bad_lines='skip'
            )):
                print(f"   Chunk {i+1}: {len(chunk)} filas")
                chunks.append(chunk)
                
                # Límite de memoria: max 10 chunks
                if len(chunks) >= 10:
                    break
            
            if not chunks:
                raise ValueError("No se pudieron leer chunks del CSV")
            
            print(f"Concatenando {len(chunks)} chunks...")
            df_final = pd.concat(chunks, ignore_index=True)
            del chunks
            
            print("Guardando a Parquet directamente...")
            df_final.to_parquet(
                parquet_path,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            
            del df_final
            
            if os.path.exists(parquet_path) and os.path.getsize(parquet_path) > 0:
                print("Estrategia 3: EXITOSA")
                return {"success": True, "method": "pandas_chunks"}
            else:
                raise ValueError("Parquet vacío")
                
        except Exception as e:
            print(f"Estrategia 3 falló: {e}")
            if os.path.exists(parquet_path):
                try:
                    os.remove(parquet_path)
                except Exception:
                    pass
        
        print("\nTodas las estrategias fallaron")
        return {
            "success": False,
            "error": "Todas las estrategias de conversión CSV fallaron. El archivo puede estar corrupto o ser demasiado grande."
        }

    def _convert_excel_to_parquet(self, file_path: str, parquet_path: str) -> Dict[str, Any]:
        """Conversión de Excel con detección automática de hojas"""
        try:
            file_size_mb = os.path.getsize(file_path) / 1024 / 1024
            
            print(f"\nConvirtiendo Excel: {file_size_mb:.2f}MB")
            
            if file_size_mb > 100:
                return self._convert_large_excel_to_parquet(file_path, parquet_path, file_size_mb)
            else:
                return self._convert_standard_excel_to_parquet(file_path, parquet_path)
                
        except Exception as e:
            print(f"Error convirtiendo Excel: {e}")
            self._cleanup_temp_views()
            return {"success": False, "error": f"Error en conversión Excel: {str(e)}"}

    def _convert_large_excel_to_parquet(self, file_path: str, parquet_path: str, file_size_mb: float) -> Dict[str, Any]:
        """Estrategias para archivos Excel grandes"""
        
        print("\n🔄 Modo: EXCEL GRANDE")
        
        # ESTRATEGIA 1: DuckDB directo
        print("\nEstrategia 1: DuckDB read_xlsx")
        try:
            conversion_sql = f"""
            COPY (
                SELECT * FROM read_xlsx('{file_path.replace(chr(92), '/')}', all_varchar = true)
            ) TO '{parquet_path.replace(chr(92), '/')}' (FORMAT 'parquet', COMPRESSION 'snappy')
            """
            
            self.conn.execute(conversion_sql)
            
            if os.path.exists(parquet_path) and os.path.getsize(parquet_path) > 0:
                print("Estrategia 1: EXITOSA")
                return {"success": True, "method": "duckdb_excel"}
            else:
                raise ValueError("Parquet vacío")
                
        except Exception as e1:
            print(f"Estrategia 1 falló: {e1}")
            if os.path.exists(parquet_path):
                try:
                    os.remove(parquet_path)
                except Exception:
                    pass
        
        # ESTRATEGIA 2: Pandas con engine óptimo
        print("\nEstrategia 2: Pandas con openpyxl")
        view_name = None
        try:
            df_excel = pd.read_excel(
                file_path,
                engine='openpyxl',
                dtype=str,
                na_filter=False
            )
            
            print(f"✓ Excel leído: {len(df_excel)} filas")
            
            # Limpieza
            for col in df_excel.columns:
                df_excel[col] = df_excel[col].astype(str)
            df_excel = df_excel.fillna('').replace(['nan', '<NA>', 'None'], '')
            
            # Vista temporal con nombre único
            view_name = f'temp_excel_df_{int(time.time() * 1000)}'
            self._temp_views.append(view_name)
            
            self.conn.register(view_name, df_excel)
            del df_excel
            
            conversion_sql = f"""
            COPY (
                SELECT * FROM {view_name}
            ) TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'snappy')
            """
            
            self.conn.execute(conversion_sql)
            
            # Limpiar vista
            self.conn.execute(f"DROP VIEW IF EXISTS {view_name}")
            if view_name in self._temp_views:
                self._temp_views.remove(view_name)
            
            if os.path.exists(parquet_path) and os.path.getsize(parquet_path) > 0:
                print("Estrategia 2: EXITOSA")
                return {"success": True, "method": "pandas_excel"}
                
        except Exception as e2:
            print(f"Estrategia 2 falló: {e2}")
            if view_name:
                try:
                    self.conn.execute(f"DROP VIEW IF EXISTS {view_name}")
                    if view_name in self._temp_views:
                        self._temp_views.remove(view_name)
                except Exception:
                    pass
        
        error_msg = f"Todas las estrategias fallaron para Excel de {file_size_mb:.2f}MB"
        return {"success": False, "error": error_msg}

    def _convert_standard_excel_to_parquet(self, file_path: str, parquet_path: str) -> Dict[str, Any]:
        """Método estándar para archivos Excel normales"""
        print("\n📌 Conversión Excel estándar")
        view_name = None
        
        try:
            df_excel = pd.read_excel(
                file_path,
                engine='openpyxl',
                dtype=str,
                na_filter=False,
                keep_default_na=False
            )
            
            print(f"✓ Excel leído: {len(df_excel)} filas, {len(df_excel.columns)} columnas")
            
            # Limpieza
            df_excel = df_excel.astype(str)
            df_excel = df_excel.replace(['nan', '<NA>', 'None', 'NaT'], '')
            df_excel = df_excel.fillna('')
            
            # Vista temporal única
            view_name = f'temp_excel_df_{int(time.time() * 1000)}'
            self._temp_views.append(view_name)
            
            self.conn.register(view_name, df_excel)
            del df_excel
            
            conversion_sql = f"""
            COPY (
                SELECT * FROM {view_name}
            ) TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'snappy')
            """
            
            self.conn.execute(conversion_sql)
            
            # Limpiar vista
            self.conn.execute(f"DROP VIEW IF EXISTS {view_name}")
            if view_name in self._temp_views:
                self._temp_views.remove(view_name)
            
            if os.path.exists(parquet_path) and os.path.getsize(parquet_path) > 0:
                print("Conversión Excel estándar: EXITOSA")
                return {"success": True, "method": "standard_excel"}
            else:
                raise ValueError("Parquet vacío")
            
        except Exception as e:
            print(f"Conversión Excel estándar falló: {e}")
            if view_name:
                try:
                    self.conn.execute(f"DROP VIEW IF EXISTS {view_name}")
                    if view_name in self._temp_views:
                        self._temp_views.remove(view_name)
                except Exception:
                    pass
            return {"success": False, "error": f"Error en conversión estándar: {str(e)}"}

    def _finalize_conversion(self, parquet_path: str, file_hash: str, original_name: str, ext: str, result: Dict[str, Any], start_time: float, original_file_path: str) -> Dict[str, Any]:
        """Finaliza la conversión con validación"""
        
        print("\n🔍 Finalizando conversión...")
        
        # Verificar archivo Parquet
        if not os.path.exists(parquet_path):
            raise FileNotFoundError(f"Archivo Parquet no creado: {parquet_path}")
        
        parquet_size = os.path.getsize(parquet_path)
        if parquet_size == 0:
            raise ValueError("Archivo Parquet está vacío")
        
        print(f"✓ Parquet creado: {parquet_size / 1024 / 1024:.2f}MB")
        
        # Obtener estadísticas
        try:
            stats_sql = f"SELECT COUNT(*) as total_rows FROM read_parquet('{parquet_path}')"
            total_rows = self.conn.execute(stats_sql).fetchone()[0]
            print(f"✓ Total filas: {total_rows:,}")
        except Exception as e:
            raise ValueError(f"Parquet corrupto: {e}")
        
        # Obtener columnas
        try:
            columns_sql = f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')"
            columns_result = self.conn.execute(columns_sql).fetchall()
            columns = [str(row[0]) for row in columns_result]
            print(f"✓ Columnas: {len(columns)}")
        except Exception as e:
            raise ValueError(f"No se pudieron obtener columnas: {e}")
        
        conversion_time = time.time() - start_time
        
        # Tamaños
        if os.path.exists(original_file_path):
            original_size = os.path.getsize(original_file_path)
        else:
            original_size = parquet_size * 4
        
        compression_ratio = (1 - parquet_size / original_size) * 100
        
        # Guardar en cache
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
        
        print(f"Conversión finalizada en {conversion_time:.2f}s")
        print(f"   Compresión: {compression_ratio:.1f}%")
        print("="*60 + "\n")
        
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
