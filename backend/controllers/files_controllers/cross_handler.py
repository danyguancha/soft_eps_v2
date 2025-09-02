# controllers/cross_handler.py
from typing import Dict, Any, List
import pandas as pd
import time
import io
from fastapi.responses import StreamingResponse
from controllers.files_controllers.upload_handler import upload_handler_instance
from services.csv_service import CSVService
from services.excel_service import ExcelService
from services.cross_service import CrossService
from controllers.files_controllers.storage_manager import FileStorageManager
from services.column_analyzer import column_analyzer
import json

class CrossHandler:
    """Manejador optimizado para operaciones de cruce entre archivos."""
    
    def __init__(self, storage_manager: FileStorageManager):
        self.storage_manager = storage_manager
        self.matcher = column_analyzer
        self.file_services = {
            "csv": CSVService(),
            "xlsx": ExcelService(),
            "xls": ExcelService()
        }

    def suggest_compatible_columns(self, request) -> Dict[str, Any]:
        """Sugiere automáticamente columnas compatibles para cruce"""
        try:
            file1_info = self.storage_manager.get_file_info(request.file1_key)
            file2_info = self.storage_manager.get_file_info(request.file2_key)
            
            if not file1_info or not file2_info:
                raise ValueError("Uno o ambos archivos no fueron encontrados")
            
            # ✅ Usar muestras solo para análisis de compatibilidad
            df1 = self._get_sample_dataframe(file1_info, request.file1_key, request.file1_sheet)
            df2 = self._get_sample_dataframe(file2_info, request.file2_key, request.file2_sheet)
            
            suggestions = self.matcher.find_best_column_matches(df1, df2)
            
            return {
                "success": True,
                "suggestions": suggestions[:10],
                "total_combinations": len(df1.columns) * len(df2.columns),
                "files_info": {
                    "file1": {"columns": df1.columns.tolist(), "sample_rows": len(df1)},
                    "file2": {"columns": df2.columns.tolist(), "sample_rows": len(df2)}
                }
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}

    def perform_cross(self, request) -> Dict[str, Any]:
        """Cruce para archivos pequeños/medianos que devuelve JSON"""
        try:
            file1_info = self.storage_manager.get_file_info(request.file1_key)
            file2_info = self.storage_manager.get_file_info(request.file2_key)
            
            if not file1_info or not file2_info:
                raise ValueError("Uno o ambos archivos no fueron encontrados")
            
            # ✅ DETECTAR SI ES ARCHIVO GRANDE
            total_rows = (file1_info.get('total_rows', 0) + file2_info.get('total_rows', 0))
            total_cols = len(file1_info.get('columns', [])) + len(file2_info.get('columns', []))
            
            if total_rows > 100000 or total_cols > 200:
                return {
                    "success": False,
                    "error": "LARGE_FILE_DETECTED",
                    "message": f"Archivo grande detectado ({total_rows:,} filas, {total_cols} columnas). Use el endpoint de descarga.",
                    "total_rows": total_rows,
                    "total_columns": total_cols,
                    "recommended_action": "download"
                }
            
            print(f"🔄 Iniciando cruce optimizado...")
            print(f"📊 Archivo 1: {file1_info['original_name']} ({file1_info.get('total_rows', 0):,} filas)")
            print(f"📊 Archivo 2: {file2_info['original_name']} ({file2_info.get('total_rows', 0):,} filas)")
            
            # ✅ VALIDACIÓN RÁPIDA con muestras
            df1_sample = self._get_sample_dataframe(
                file1_info, request.file1_key, request.file1_sheet, sample_size=1000
            )
            df2_sample = self._get_sample_dataframe(
                file2_info, request.file2_key, request.file2_sheet, sample_size=1000
            )
            
            self._validate_cross_columns(df1_sample, df2_sample, request)
            
            validation = self._validate_key_columns_compatibility_intelligent(
                df1_sample, df2_sample, request
            )
            
            if not validation["compatible"]:
                suggestions_result = self.suggest_compatible_columns(request)
                alternative_suggestions = suggestions_result.get("suggestions", [])[:5]
                
                return {
                    "success": False,
                    "error": "Columnas no suficientemente compatibles para cruce",
                    "validation": validation,
                    "alternative_suggestions": alternative_suggestions,
                    "details": {
                        "compatibility_score": validation["combined_score"],
                        "pattern_score": validation["pattern_score"],
                        "overlap_score": validation["overlap_score"],
                        "recommendation": validation["recommendation"],
                        "left_column_analysis": {
                            "name": request.key_column_file1,
                            "type": validation["left_analysis"]["data_type"],
                            "sample_values": validation["left_analysis"]["sample_values"][:5]
                        },
                        "right_column_analysis": {
                            "name": request.key_column_file2,
                            "type": validation["right_analysis"]["data_type"],
                            "sample_values": validation["right_analysis"]["sample_values"][:5]
                        }
                    },
                    "message": f"Score de compatibilidad: {validation['combined_score']:.3f}. {validation['recommendation']} Revisa las sugerencias alternativas."
                }
            
            # ✅ DETERMINAR COLUMNAS NECESARIAS para optimizar carga
            columns_file1 = request.columns_to_include.get('file1_columns', []) if hasattr(request, 'columns_to_include') and request.columns_to_include else []
            columns_file2 = request.columns_to_include.get('file2_columns', []) if hasattr(request, 'columns_to_include') and request.columns_to_include else []
            
            # Asegurar que las columnas clave estén incluidas
            if columns_file1 and request.key_column_file1 not in columns_file1:
                columns_file1.append(request.key_column_file1)
            if columns_file2 and request.key_column_file2 not in columns_file2:
                columns_file2.append(request.key_column_file2)
            
            print(f"🔄 Cargando archivos completos de forma adaptativa...")
            
            # ✅ CARGA ADAPTATIVA COMPLETA con optimización de columnas
            df1 = upload_handler_instance.get_data_adaptive(
                request.file1_key, 
                request.file1_sheet,
                columns_needed=columns_file1 if columns_file1 else None
            )
            
            df2 = upload_handler_instance.get_data_adaptive(
                request.file2_key, 
                request.file2_sheet,
                columns_needed=columns_file2 if columns_file2 else None
            )
            
            print(f"📊 Archivos completos cargados:")
            print(f"   - DF1: {len(df1):,} filas x {len(df1.columns)} columnas")
            print(f"   - DF2: {len(df2):,} filas x {len(df2.columns)} columnas")
            
            # ✅ FILTRADO FINAL DE COLUMNAS si es necesario
            if columns_file1:
                missing_cols1 = [col for col in columns_file1 if col not in df1.columns]
                if missing_cols1:
                    raise ValueError(f"Columnas no encontradas en archivo 1: {missing_cols1}")
                df1_filtered = df1[columns_file1].copy()
            else:
                df1_filtered = df1.copy()
                
            if columns_file2:
                missing_cols2 = [col for col in columns_file2 if col not in df2.columns]
                if missing_cols2:
                    raise ValueError(f"Columnas no encontradas en archivo 2: {missing_cols2}")
                df2_filtered = df2[columns_file2].copy()
            else:
                df2_filtered = df2.copy()
            
            # Normalizar tipos de columnas clave
            type1 = df1_filtered[request.key_column_file1].dtype
            type2 = df2_filtered[request.key_column_file2].dtype
            
            print(f"🔧 Normalizando tipos: {type1} -> str, {type2} -> str")
            
            df1_filtered[request.key_column_file1] = df1_filtered[request.key_column_file1].astype(str).str.strip()
            df2_filtered[request.key_column_file2] = df2_filtered[request.key_column_file2].astype(str).str.strip()
            
            cross_type = getattr(request, 'cross_type', 'left')
            print(f"🔗 Tipo de cruce: {cross_type}")
            
            # ✅ REALIZAR CRUCE ULTRA-EFICIENTE - RESULTADO SIEMPRE COMPLETO
            result_df, stats = CrossService.cross_files_with_stats(
                df1_filtered, 
                df2_filtered, 
                request.key_column_file1, 
                request.key_column_file2,
                columns_to_add=columns_file2
            )
            
            print(f"🎉 Cruce completado:")
            print(f"   - Total registros: {stats['total_rows']:,}")
            print(f"   - Con coincidencias: {stats['matched_rows']:,}")
            print(f"   - Sin coincidencias: {stats['unmatched_rows']:,}")
            print(f"   - Porcentaje coincidencias: {stats['match_percentage']:.2f}%")
            
            # ✅ CONVERTIR TODO EL RESULTADO A JSON para el frontend
            print(f"📤 Convirtiendo {len(result_df):,} registros a JSON...")
            try:
                clean_data = json.loads(result_df.to_json(orient='records', force_ascii=False))
                print(f"✅ JSON generado: {len(clean_data):,} registros listos para el frontend")
            except Exception as e:
                print(f"⚠️ Fallback a conversión manual: {e}")
                clean_data = self._convert_numpy_types_to_native(result_df)
            
            return {
                "success": True,
                "total_rows": len(result_df),  # ✅ TODOS los registros
                "columns": list(result_df.columns),
                "data": clean_data,  # ✅ TODOS los datos para el frontend
                "statistics": {
                    "file1_matched": stats['matched_rows'],
                    "file2_matched": stats['matched_rows'],
                    "match_percentage_file1": stats['match_percentage'],
                    "match_percentage_file2": stats['match_percentage'],
                    "cross_type": cross_type,
                    "duplicates_removed": stats.get('duplicates_removed', 0),
                    "processing_method": stats.get('processing_method', 'optimized'),
                    "performance": "high_performance_mode"
                },
                "validation": {
                    "compatibility_score": validation["combined_score"],
                    "pattern_score": validation["pattern_score"],
                    "overlap_score": validation["overlap_score"],
                    "recommendation": validation["recommendation"],
                    "left_analysis": validation["left_analysis"]["data_type"],
                    "right_analysis": validation["right_analysis"]["data_type"]
                },
                "metadata": {
                    "original_file1_rows": len(df1),
                    "original_file2_rows": len(df2),
                    "filtered_file1_rows": len(df1_filtered),
                    "filtered_file2_rows": len(df2_filtered),
                    "file1_columns_used": list(df1_filtered.columns),
                    "file2_columns_used": list(df2_filtered.columns),
                    "types_normalized": True,
                    "processing_strategy": "adaptive_loading_full_result",
                    "memory_optimized": True,
                    "full_data_returned": True,  # ✅ Confirmación explícita
                    "original_types": {
                        "file1_key_type": str(type1),
                        "file2_key_type": str(type2)
                    }
                }
            }
            
        except ValueError as ve:
            raise ve
        except KeyError as ke:
            raise ValueError(f"Error de columna: {str(ke)}")
        except Exception as e:
            raise ValueError(f"Error interno del servidor: {str(e)}")

    def perform_cross_for_streaming(self, request) -> StreamingResponse:
        """Versión optimizada que devuelve StreamingResponse para archivos grandes"""
        try:
            start_time = time.time()
            
            file1_info = self.storage_manager.get_file_info(request.file1_key)
            file2_info = self.storage_manager.get_file_info(request.file2_key)
            
            if not file1_info or not file2_info:
                raise ValueError("Uno o ambos archivos no fueron encontrados")
            
            print(f"🔄 Iniciando cruce con streaming...")
            print(f"📊 Archivo 1: {file1_info['original_name']} ({file1_info.get('total_rows', 0):,} filas)")
            print(f"📊 Archivo 2: {file2_info['original_name']} ({file2_info.get('total_rows', 0):,} filas)")
            
            # ✅ VALIDACIÓN RÁPIDA con muestras
            df1_sample = self._get_sample_dataframe(
                file1_info, request.file1_key, request.file1_sheet, sample_size=1000
            )
            df2_sample = self._get_sample_dataframe(
                file2_info, request.file2_key, request.file2_sheet, sample_size=1000
            )
            
            self._validate_cross_columns(df1_sample, df2_sample, request)
            
            validation = self._validate_key_columns_compatibility_intelligent(
                df1_sample, df2_sample, request
            )
            
            if not validation["compatible"]:
                raise ValueError(f"Columnas no compatibles: {validation['recommendation']}")
            
            # ✅ DETERMINAR COLUMNAS NECESARIAS
            columns_file1 = request.columns_to_include.get('file1_columns', []) if hasattr(request, 'columns_to_include') and request.columns_to_include else []
            columns_file2 = request.columns_to_include.get('file2_columns', []) if hasattr(request, 'columns_to_include') and request.columns_to_include else []
            
            if columns_file1 and request.key_column_file1 not in columns_file1:
                columns_file1.append(request.key_column_file1)
            if columns_file2 and request.key_column_file2 not in columns_file2:
                columns_file2.append(request.key_column_file2)
            
            # ✅ CARGA OPTIMIZADA
            print(f"🔄 Cargando archivos completos...")
            load_start = time.time()
            
            df1 = upload_handler_instance.get_data_adaptive(
                request.file1_key, 
                request.file1_sheet,
                columns_needed=columns_file1 if columns_file1 else None
            )
            
            df2 = upload_handler_instance.get_data_adaptive(
                request.file2_key, 
                request.file2_sheet,
                columns_needed=columns_file2 if columns_file2 else None
            )
            
            load_time = time.time() - load_start
            print(f"⏱️ Tiempo de carga: {load_time:.2f}s")
            
            # Filtrar columnas finales
            if columns_file1:
                df1_filtered = df1[columns_file1].copy()
            else:
                df1_filtered = df1.copy()
                
            if columns_file2:
                df2_filtered = df2[columns_file2].copy()
            else:
                df2_filtered = df2.copy()
            
            print(f"📊 Procesando cruce: DF1 {len(df1_filtered):,} x DF2 {len(df2_filtered):,}")
            
            # ✅ GENERAR NOMBRE DE ARCHIVO
            timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            filename = f"cruce_{timestamp}.csv"
            
            # ✅ GENERAR STREAMING RESPONSE
            streaming_response = CrossService.cross_files_to_stream(
                df1_filtered, 
                df2_filtered, 
                request.key_column_file1, 
                request.key_column_file2,
                filename
            )
            
            total_time = time.time() - start_time
            print(f"⏱️ TIEMPO TOTAL: {total_time:.2f}s")
            
            return streaming_response
            
        except Exception as e:
            print(f"❌ Error en cruce streaming: {e}")
            raise ValueError(f"Error en cruce: {str(e)}")

    def _validate_key_columns_compatibility_intelligent(self, df1: pd.DataFrame, df2: pd.DataFrame, request) -> Dict:
        """Validación inteligente de compatibilidad entre columnas clave"""
        col1_name = request.key_column_file1
        col2_name = request.key_column_file2
        
        if col1_name not in df1.columns:
            raise ValueError(f"La columna '{col1_name}' no existe en el primer archivo")
        if col2_name not in df2.columns:
            raise ValueError(f"La columna '{col2_name}' no existe en el segundo archivo")
        
        series1 = df1[col1_name].dropna()
        series2 = df2[col2_name].dropna()
        
        if len(series1) == 0:
            raise ValueError(f"La columna clave '{col1_name}' está completamente vacía")
        if len(series2) == 0:
            raise ValueError(f"La columna clave '{col2_name}' está completamente vacía")
        
        analysis1 = self.matcher.analyze_column_patterns(df1[col1_name])
        analysis2 = self.matcher.analyze_column_patterns(df2[col2_name])
        
        pattern_score = self.matcher.calculate_compatibility_score(analysis1, analysis2)
        overlap_score, overlap_examples = self.matcher._calculate_value_overlap(df1[col1_name], df2[col2_name])
        
        if pattern_score >= 0.6:
            combined_score = pattern_score * 0.7 + overlap_score * 0.3
        else:
            combined_score = pattern_score * 0.4 + overlap_score * 0.6
        
        is_compatible = combined_score >= 0.25
        
        if (analysis1["data_type"] == "document_id" and analysis2["data_type"] == "document_id" and 
            pattern_score >= 0.8):
            is_compatible = True
            combined_score = max(combined_score, 0.4)
        
        return {
            "compatible": is_compatible,
            "combined_score": round(combined_score, 3),
            "pattern_score": round(pattern_score, 3),
            "overlap_score": round(overlap_score, 3),
            "overlap_examples": overlap_examples,
            "left_analysis": analysis1,
            "right_analysis": analysis2,
            "recommendation": self.matcher._generate_recommendation(pattern_score, overlap_score, combined_score)
        }

    def _get_sample_dataframe(self, file_info: Dict[str, Any], file_id: str, sheet_name: str = None, sample_size: int = 1000) -> pd.DataFrame:
        """Obtiene una muestra del DataFrame SOLO para análisis de compatibilidad"""
        target_sheet = sheet_name or file_info.get("default_sheet")
        
        try:
            df = upload_handler_instance.get_data_chunk(
                file_id=file_id,
                start_row=0,
                chunk_size=sample_size,  # ✅ Solo para análisis
                sheet_name=target_sheet
            )
            return df
        except Exception:
            return self._get_dataframe_from_file(file_info, file_id, sheet_name)

    def _validate_cross_columns(self, df1: pd.DataFrame, df2: pd.DataFrame, request) -> None:
        """Valida que las columnas de cruce existan en ambos DataFrames"""
        if request.key_column_file1 not in df1.columns:
            raise ValueError(f"La columna '{request.key_column_file1}' no existe en el primer archivo")
        if request.key_column_file2 not in df2.columns:
            raise ValueError(f"La columna '{request.key_column_file2}' no existe en el segundo archivo")

    def _convert_numpy_types_to_native(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Convierte tipos numpy a tipos nativos de Python para serialización JSON"""
        import numpy as np
        
        def convert_value(value):
            if pd.isna(value):
                return None
            elif isinstance(value, np.integer):
                return int(value)
            elif isinstance(value, np.floating):
                return float(value)
            elif isinstance(value, np.bool_):
                return bool(value)
            elif isinstance(value, np.bytes_):
                return value.decode('utf-8', errors='ignore')
            elif hasattr(np, 'str_') and isinstance(value, np.str_):
                return str(value)
            elif hasattr(np, 'unicode_') and isinstance(value, np.unicode_):
                return str(value)
            elif isinstance(value, (str, bytes)):
                if isinstance(value, bytes):
                    return value.decode('utf-8', errors='ignore')
                return str(value)
            else:
                return value
        
        records = []
        for _, row in df.iterrows():
            record = {}
            for col in df.columns:
                record[col] = convert_value(row[col])
            records.append(record)
        return records

    def _get_dataframe_from_file(self, file_info: Dict[str, Any], file_id: str, sheet_name: str = None) -> pd.DataFrame:
        """Fallback: Obtiene DataFrame completo directamente"""
        return upload_handler_instance.get_data_adaptive(file_id, sheet_name)

    def get_available_columns(self, file_id: str, sheet_name: str = None) -> Dict[str, Any]:
        """Obtiene las columnas disponibles para un archivo específico"""
        file_info = self.storage_manager.get_file_info(file_id)
        if not file_info:
            raise ValueError("Archivo no encontrado")
        
        target_sheet = sheet_name or file_info.get("default_sheet")
        
        if file_info["ext"] != "csv" and sheet_name and sheet_name != file_info.get("default_sheet"):
            service = self.file_services[file_info["ext"]]
            file_obj = service.load(file_info["path"])
            try:
                columns = service.get_columns(file_obj, sheet_name)
                return {
                    "file_id": file_id,
                    "sheet_name": sheet_name,
                    "columns": columns
                }
            except Exception as e:
                raise ValueError(f"Error al acceder a la hoja '{sheet_name}': {str(e)}")
        
        return {
            "file_id": file_id,
            "sheet_name": target_sheet,
            "columns": file_info["columns"]
        }

storage_manager = FileStorageManager()
cross_handler_instance = CrossHandler(storage_manager)
