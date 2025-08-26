# controllers/cross_handler.py
import traceback
from typing import Dict, Any, List
import uuid
import pandas as pd
from services.csv_service import CSVService
from services.excel_service import ExcelService
from services.cross_service import CrossService
from controllers.files_controllers.storage_manager import FileStorageManager
import logging

logger = logging.getLogger(__name__)

class CrossHandler:
    def __init__(self, storage_manager: FileStorageManager):
        self.storage_manager = storage_manager
        self.file_services = {
            "csv": CSVService(),
            "xlsx": ExcelService(),
            "xls": ExcelService()
        }
    
    def perform_cross(self, request) -> Dict[str, Any]:
        """Realiza cruce entre dos archivos"""
        import json
        import traceback
        import numpy as np
        
        try:
            logger.info(f"🔄 CrossHandler.perform_cross iniciado")
            logger.info(f"📊 Request: file1={request.file1_key}, file2={request.file2_key}")
            logger.info(f"🔑 Columnas clave: {request.key_column_file1} <-> {request.key_column_file2}")
            logger.info(f"🔀 Tipo de cruce: {getattr(request, 'cross_type', 'inner')}")
            
            # Validar que los archivos existan
            logger.info("🔍 Validando existencia de archivos...")
            file1_info = self.storage_manager.get_file_info(request.file1_key)
            file2_info = self.storage_manager.get_file_info(request.file2_key)
            
            if not file1_info or not file2_info:
                logger.error(f"❌ Archivos no encontrados: file1={bool(file1_info)}, file2={bool(file2_info)}")
                raise ValueError("Uno o ambos archivos no fueron encontrados")
            
            logger.info("✅ Archivos encontrados, cargando DataFrames...")
            
            # Obtener DataFrames completos
            df1 = self._get_dataframe_from_file(
                file1_info, 
                request.file1_key, 
                request.file1_sheet
            )
            
            df2 = self._get_dataframe_from_file(
                file2_info, 
                request.file2_key, 
                request.file2_sheet
            )
            
            logger.info(f"✅ DataFrames cargados:")
            logger.info(f"  - DF1: {len(df1)} filas, {len(df1.columns)} columnas: {list(df1.columns)}")
            logger.info(f"  - DF2: {len(df2)} filas, {len(df2.columns)} columnas: {list(df2.columns)}")
            
            # Validar que las columnas de cruce existan
            logger.info("🔍 Validando columnas de cruce...")
            self._validate_cross_columns(df1, df2, request)
            logger.info("✅ Columnas de cruce validadas")
            
            # ✅ NUEVA VALIDACIÓN: COMPATIBILIDAD DE COLUMNAS CLAVE
            logger.info("🔍 Validando compatibilidad de columnas clave...")
            self._validate_key_columns_compatibility(df1, df2, request)
            logger.info("✅ Columnas clave son compatibles")
            
            # ✅ FILTRAR COLUMNAS ANTES DEL MERGE
            logger.info("📋 Procesando selección de columnas...")
            
            if hasattr(request, 'columns_to_include') and request.columns_to_include:
                file1_columns = request.columns_to_include.get('file1_columns', [])
                file2_columns = request.columns_to_include.get('file2_columns', [])
                
                logger.info(f"📊 Columnas especificadas:")
                logger.info(f"  - Archivo 1 ({len(file1_columns)} cols): {file1_columns}")
                logger.info(f"  - Archivo 2 ({len(file2_columns)} cols): {file2_columns}")
                
                # ✅ ASEGURAR QUE LAS COLUMNAS CLAVE ESTÉN INCLUIDAS
                if file1_columns and request.key_column_file1 not in file1_columns:
                    file1_columns.append(request.key_column_file1)
                    logger.info(f"➕ Agregada columna clave del archivo 1: {request.key_column_file1}")
                
                if file2_columns and request.key_column_file2 not in file2_columns:
                    file2_columns.append(request.key_column_file2)
                    logger.info(f"➕ Agregada columna clave del archivo 2: {request.key_column_file2}")
                
                # ✅ VALIDAR QUE TODAS LAS COLUMNAS EXISTAN
                if file1_columns:
                    missing_cols1 = [col for col in file1_columns if col not in df1.columns]
                    if missing_cols1:
                        logger.error(f"❌ Columnas faltantes en archivo 1: {missing_cols1}")
                        raise ValueError(f"Columnas no encontradas en archivo 1: {missing_cols1}")
                    df1_filtered = df1[file1_columns].copy()
                else:
                    logger.info("⚠️ Usando todas las columnas del archivo 1")
                    df1_filtered = df1.copy()
                    
                if file2_columns:
                    missing_cols2 = [col for col in file2_columns if col not in df2.columns]
                    if missing_cols2:
                        logger.error(f"❌ Columnas faltantes en archivo 2: {missing_cols2}")
                        raise ValueError(f"Columnas no encontradas en archivo 2: {missing_cols2}")
                    df2_filtered = df2[file2_columns].copy()
                else:
                    # Si no se especifican columnas del archivo 2, usar solo la clave
                    logger.info("⚠️ No se especificaron columnas del archivo 2, usando solo columna clave")
                    df2_filtered = df2[[request.key_column_file2]].copy()
            else:
                # Fallback: usar todos los DataFrames
                logger.warning("⚠️ No se especificó columns_to_include, usando DataFrames completos")
                df1_filtered = df1.copy()
                df2_filtered = df2.copy()
            
            logger.info(f"📊 DataFrames filtrados:")
            logger.info(f"  - DF1 filtrado: {len(df1_filtered.columns)} columnas: {list(df1_filtered.columns)}")
            logger.info(f"  - DF2 filtrado: {len(df2_filtered.columns)} columnas: {list(df2_filtered.columns)}")
            
            # ✅ REALIZAR EL CRUCE USANDO LOS DATAFRAMES FILTRADOS
            cross_type = getattr(request, 'cross_type', 'inner')
            logger.info(f"🔄 Realizando merge tipo '{cross_type}'...")
            
            result_df = CrossService.cross_files(
                df1_filtered, 
                df2_filtered, 
                request.key_column_file1, 
                request.key_column_file2,
                how=cross_type
            )
            
            logger.info(f"✅ Merge completado:")
            logger.info(f"  - Resultado: {len(result_df)} filas, {len(result_df.columns)} columnas")
            logger.info(f"  - Columnas finales: {list(result_df.columns)}")
            
            # ✅ CONVERTIR A JSON CON ENCODING CORRECTO PARA CARACTERES ESPECIALES
            logger.info("🔄 Convirtiendo resultado a JSON...")
            
            # Usar pandas to_json con force_ascii=False para mantener caracteres especiales
            clean_data = json.loads(result_df.to_json(orient='records', force_ascii=False))
            
            # ✅ CALCULAR ESTADÍSTICAS CON LOS DATAFRAMES FILTRADOS
            logger.info("📊 Calculando estadísticas de coincidencias...")
            
            try:
                # Contar coincidencias en el archivo 1
                file1_matched = len(df1_filtered[
                    df1_filtered[request.key_column_file1].isin(result_df[request.key_column_file1])
                ])
                
                # Contar coincidencias en el archivo 2
                # Buscar la columna clave del archivo 2 en el resultado (puede tener sufijo)
                key2_result_column = request.key_column_file2
                if key2_result_column not in result_df.columns:
                    # Buscar con sufijo _file2
                    key2_result_column = f"{request.key_column_file2}_file2"
                
                if key2_result_column in result_df.columns:
                    file2_matched = len(df2_filtered[
                        df2_filtered[request.key_column_file2].isin(result_df[key2_result_column])
                    ])
                else:
                    logger.warning(f"⚠️ No se pudo encontrar columna clave del archivo 2 en resultado")
                    file2_matched = 0
                    
            except Exception as stats_error:
                logger.warning(f"⚠️ Error calculando estadísticas: {stats_error}")
                file1_matched = 0
                file2_matched = 0
            
            # ✅ PREPARAR RESPUESTA FINAL
            response = {
                "success": True,
                "total_rows": len(result_df),
                "columns": list(result_df.columns),
                "data": clean_data,
                "file1_matched": file1_matched,
                "file2_matched": file2_matched,
                "cross_type": cross_type,
                # ✅ INFORMACIÓN ADICIONAL PARA DEBUGGING
                "metadata": {
                    "original_file1_rows": len(df1),
                    "original_file2_rows": len(df2),
                    "filtered_file1_rows": len(df1_filtered),
                    "filtered_file2_rows": len(df2_filtered),
                    "file1_columns_used": list(df1_filtered.columns),
                    "file2_columns_used": list(df2_filtered.columns)
                }
            }
            
            logger.info(f"✅ Response preparado exitosamente:")
            logger.info(f"  - Total registros: {response['total_rows']}")
            logger.info(f"  - Total columnas: {len(response['columns'])}")
            logger.info(f"  - Coincidencias archivo 1: {response['file1_matched']}")
            logger.info(f"  - Coincidencias archivo 2: {response['file2_matched']}")
            
            return response
            
        except ValueError as ve:
            # Errores de validación conocidos
            logger.error(f"❌ Error de validación: {str(ve)}")
            raise ve
        except KeyError as ke:
            # Errores de columnas faltantes
            logger.error(f"❌ Error de columna: {str(ke)}")
            logger.error(f"❌ Traceback: {traceback.format_exc()}")
            raise ValueError(f"Error de columna: {str(ke)}")
        except Exception as e:
            # Otros errores inesperados
            logger.error(f"❌ Error inesperado en CrossHandler.perform_cross:")
            logger.error(f"❌ Tipo: {type(e).__name__}")
            logger.error(f"❌ Mensaje: {str(e)}")
            logger.error(f"❌ Traceback completo:")
            logger.error(traceback.format_exc())
            raise ValueError(f"Error interno del servidor: {str(e)}")

    # ✅ NUEVO MÉTODO: VALIDACIÓN DE COMPATIBILIDAD DE COLUMNAS CLAVE
    def _validate_key_columns_compatibility(self, df1: pd.DataFrame, df2: pd.DataFrame, request) -> None:
        """Valida que las columnas clave sean compatibles para el cruce"""
        import pandas as pd
        import numpy as np
        
        col1_name = request.key_column_file1
        col2_name = request.key_column_file2
        
        logger.info(f"🔍 Analizando compatibilidad entre '{col1_name}' y '{col2_name}'")
        
        # Obtener las series de las columnas clave
        series1 = df1[col1_name].dropna()
        series2 = df2[col2_name].dropna()
        
        logger.info(f"📊 Datos de las columnas clave:")
        logger.info(f"  - {col1_name}: {len(series1)} valores no nulos de {len(df1)} total")
        logger.info(f"  - {col2_name}: {len(series2)} valores no nulos de {len(df2)} total")
        
        # ✅ VALIDACIÓN 1: Verificar que haya datos suficientes
        if len(series1) == 0:
            raise ValueError(f"La columna clave '{col1_name}' del archivo principal está completamente vacía")
        
        if len(series2) == 0:
            raise ValueError(f"La columna clave '{col2_name}' del archivo de búsqueda está completamente vacía")
        
        # ✅ VALIDACIÓN 2: Verificar porcentaje de valores nulos
        null_percentage1 = (len(df1) - len(series1)) / len(df1) * 100
        null_percentage2 = (len(df2) - len(series2)) / len(df2) * 100
        
        if null_percentage1 > 50:
            logger.warning(f"⚠️ La columna '{col1_name}' tiene {null_percentage1:.1f}% de valores nulos")
        
        if null_percentage2 > 50:
            logger.warning(f"⚠️ La columna '{col2_name}' tiene {null_percentage2:.1f}% de valores nulos")
        
        # ✅ VALIDACIÓN 3: Verificar tipos de datos compatibles
        dtype1 = series1.dtype
        dtype2 = series2.dtype
        
        logger.info(f"🔤 Tipos de datos:")
        logger.info(f"  - {col1_name}: {dtype1}")
        logger.info(f"  - {col2_name}: {dtype2}")
        
        # Determinar categoría de tipos
        def get_type_category(dtype):
            if pd.api.types.is_numeric_dtype(dtype):
                return "numérico"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                return "fecha"
            elif pd.api.types.is_object_dtype(dtype):
                return "texto"
            else:
                return "otro"
        
        cat1 = get_type_category(dtype1)
        cat2 = get_type_category(dtype2)
        
        logger.info(f"📂 Categorías de datos:")
        logger.info(f"  - {col1_name}: {cat1}")
        logger.info(f"  - {col2_name}: {cat2}")
        
        # ✅ VALIDACIÓN 4: Verificar compatibilidad de tipos
        compatible_combinations = [
            ("numérico", "numérico"),
            ("texto", "texto"),
            ("fecha", "fecha"),
            ("numérico", "texto"),  # Números pueden compararse como texto
            ("texto", "numérico")   # Texto puede convertirse a número si es válido
        ]
        
        if (cat1, cat2) not in compatible_combinations:
            raise ValueError(
                f"Tipos de datos incompatibles: La columna '{col1_name}' es de tipo {cat1} "
                f"y la columna '{col2_name}' es de tipo {cat2}. "
                f"No es posible realizar el cruce entre estos tipos de datos."
            )
        
        # ✅ VALIDACIÓN 5: Verificar que haya valores en común
        logger.info("🔍 Buscando valores en común...")
        
        # Convertir a string para comparación universal
        values1_str = series1.astype(str).str.strip().str.lower()
        values2_str = series2.astype(str).str.strip().str.lower()
        
        # Obtener muestra de valores únicos para análisis
        sample1 = set(values1_str.unique()[:100])  # Primeros 100 valores únicos
        sample2 = set(values2_str.unique()[:100])
        
        common_values = sample1.intersection(sample2)
        
        logger.info(f"📊 Análisis de valores comunes:")
        logger.info(f"  - Valores únicos en {col1_name}: {len(sample1)} (muestra)")
        logger.info(f"  - Valores únicos en {col2_name}: {len(sample2)} (muestra)")
        logger.info(f"  - Valores en común: {len(common_values)}")
        
        if len(common_values) == 0:
            # Mostrar algunos valores de ejemplo para ayudar al usuario
            examples1 = list(sample1)[:5]
            examples2 = list(sample2)[:5]
            
            raise ValueError(
                f"❌ Las columnas clave no tienen valores en común. "
                f"Ejemplos de valores en '{col1_name}': {examples1}. "
                f"Ejemplos de valores en '{col2_name}': {examples2}. "
                f"Verifique que las columnas contengan el mismo tipo de información "
                f"(IDs, códigos, nombres, etc.) y que los formatos coincidan."
            )
        
        # ✅ VALIDACIÓN 6: Advertencia si hay pocos valores en común
        common_percentage = (len(common_values) / min(len(sample1), len(sample2))) * 100
        
        if common_percentage < 10:
            logger.warning(
                f"⚠️ Solo {common_percentage:.1f}% de los valores coinciden entre las columnas clave. "
                f"El cruce puede resultar en pocos registros."
            )
        elif common_percentage < 30:
            logger.info(
                f"ℹ️ {common_percentage:.1f}% de los valores coinciden entre las columnas clave. "
                f"Resultado esperado del cruce: moderado."
            )
        else:
            logger.info(
                f"✅ {common_percentage:.1f}% de los valores coinciden entre las columnas clave. "
                f"Buen potencial para el cruce."
            )
        
        # ✅ VALIDACIÓN 7: Verificar formatos específicos si son números
        if cat1 == "numérico" and cat2 == "numérico":
            # Verificar si hay diferencias significativas en rangos
            range1 = (series1.min(), series1.max())
            range2 = (series2.min(), series2.max())
            
            logger.info(f"📊 Rangos numéricos:")
            logger.info(f"  - {col1_name}: {range1[0]} a {range1[1]}")
            logger.info(f"  - {col2_name}: {range2[0]} a {range2[1]}")
            
            # Si los rangos son completamente diferentes, advertir
            if (range1[1] < range2[0]) or (range2[1] < range1[0]):
                logger.warning(
                    f"⚠️ Los rangos numéricos no se solapan. "
                    f"Esto puede indicar que las columnas contienen diferentes tipos de IDs."
                )
        
        logger.info("✅ Validación de compatibilidad completada exitosamente")


    def _convert_numpy_types_to_native(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Convierte tipos numpy a tipos nativos de Python para serialización JSON"""
        import numpy as np
        
        def convert_value(value):
            """Convierte un valor numpy a tipo nativo de Python - Compatible con NumPy 2.0"""
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
            # ✅ FIX PARA NUMPY 2.0 - Usar hasattr para detectar tipos string
            elif hasattr(np, 'str_') and isinstance(value, np.str_):
                return str(value)
            # ✅ FALLBACK para numpy < 2.0 (si np.unicode_ aún existe)
            elif hasattr(np, 'unicode_') and isinstance(value, np.unicode_):
                return str(value)
            # ✅ FALLBACK genérico para tipos string
            elif isinstance(value, (str, bytes)):
                if isinstance(value, bytes):
                    return value.decode('utf-8', errors='ignore')
                return str(value)
            else:
                return value
        
        # Convertir DataFrame a lista de diccionarios con tipos nativos
        records = []
        for _, row in df.iterrows():
            record = {}
            for col in df.columns:
                record[col] = convert_value(row[col])
            records.append(record)
        
        return records

    
    def _get_dataframe_from_file(self, file_info: Dict[str, Any], file_id: str, sheet_name: str = None) -> pd.DataFrame:
        """Obtiene DataFrame desde cache o carga desde archivo"""
        # Determinar hoja a usar
        target_sheet = sheet_name or file_info.get("default_sheet")
        cache_key = self.storage_manager.generate_cache_key(file_id, target_sheet)
        
        # Intentar obtener desde cache primero
        df = self.storage_manager.get_cached_dataframe(cache_key)
        
        if df is not None:
            return df.copy()
        
        # Si no está en cache, cargar desde archivo
        service = self.file_services[file_info["ext"]]
        file_obj = service.load(file_info["path"])
        
        if file_info["ext"] == "csv":
            df = service.get_data(file_obj)
        else:
            df = service.get_data(file_obj, sheet_name=target_sheet)
        
        # Cachear para uso futuro
        self.storage_manager.cache_dataframe(cache_key, df)
        
        return df.copy()
    
    def _validate_cross_columns(self, df1: pd.DataFrame, df2: pd.DataFrame, request) -> None:
        """Valida que las columnas de cruce existan en ambos DataFrames"""
        if request.key_column_file1 not in df1.columns:
            raise ValueError(f"La columna '{request.key_column_file1}' no existe en el primer archivo")
        
        if request.key_column_file2 not in df2.columns:
            raise ValueError(f"La columna '{request.key_column_file2}' no existe en el segundo archivo")
    
    def get_available_columns(self, file_id: str, sheet_name: str = None) -> Dict[str, Any]:
        """Obtiene las columnas disponibles para un archivo específico"""
        file_info = self.storage_manager.get_file_info(file_id)
        if not file_info:
            raise ValueError("Archivo no encontrado")
        
        target_sheet = sheet_name or file_info.get("default_sheet")
        
        # Si es un archivo Excel y se especifica una hoja diferente
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
    


    def preview_cross_operation(self, request, limit: int = 100) -> Dict[str, Any]:
        """Previsualiza el resultado del cruce con una muestra de datos"""
        try:
            # Ajusta este método según tu storage_manager
            available_files = self.storage_manager.list_all_files()
            for idx, file_info in enumerate(available_files):
                logger.info(f"  [{idx}] ID: {file_info.get('file_id', 'NO-ID')} | Name: {file_info.get('original_name', 'NO-NAME')} | Type: {type(file_info.get('file_id', ''))}")
        except Exception as e:
            logger.warning(f"⚠️ No se pudo listar archivos: {e}")
        
        try:
            file1_info = self.storage_manager.get_file_info(request.file1_key)
            if file1_info:
                logger.info(f"✅ Archivo 1 ENCONTRADO: {file1_info}")
            else:
                logger.error(f"❌ Archivo 1 NO ENCONTRADO")
                try:
                    if isinstance(request.file1_key, str):
                        uuid_key = uuid.UUID(request.file1_key)
                        alt_result = self.storage_manager.get_file_info(uuid_key)
                except Exception as uuid_err:
                    logger.warning(f"⚠️ No se pudo convertir a UUID: {uuid_err}")
                    
        except Exception as e:
            logger.error(f"❌ Excepción buscando archivo 1: {type(e).__name__}: {e}")
            file1_info = None
        try:
            file2_info = self.storage_manager.get_file_info(request.file2_key)
            if file2_info:
                logger.info(f"✅ Archivo 2 ENCONTRADO: {file2_info}")
            else:
                logger.error(f"❌ Archivo 2 NO ENCONTRADO")
        except Exception as e:
            logger.error(f"❌ Excepción buscando archivo 2: {type(e).__name__}: {e}")
            file2_info = None
        
        # Verificación final
        if not file1_info or not file2_info:
            error_msg = f"Archivos no encontrados - file1: {'✅' if file1_info else '❌'}, file2: {'✅' if file2_info else '❌'}"
            logger.error(f"❌ {error_msg}")
            raise ValueError("Uno o ambos archivos no fueron encontrados")
        
        df1 = self._get_dataframe_from_file(file1_info, request.file1_key, request.file1_sheet).head(limit)
        df2 = self._get_dataframe_from_file(file2_info, request.file2_key, request.file2_sheet).head(limit)
        
        # Validar columnas
        self._validate_cross_columns(df1, df2, request)
        
        # Realizar cruce de muestra
        preview_result = CrossService.cross_files(
            df1, 
            df2, 
            request.key_column_file1, 
            request.key_column_file2,
            how=getattr(request, 'cross_type', 'inner')
        )
        
        return {
            "preview": True,
            "sample_size": limit,
            "file1_rows_sampled": len(df1),
            "file2_rows_sampled": len(df2),
            "result_rows": len(preview_result) if isinstance(preview_result, pd.DataFrame) else len(preview_result.get('data', [])),
            "result_columns": list(preview_result.columns) if isinstance(preview_result, pd.DataFrame) else preview_result.get('columns', []),
            "sample_data": preview_result.head(10).to_dict('records') if isinstance(preview_result, pd.DataFrame) else preview_result.get('data', [])[:10]
        }
