# controllers/cross_controller.py - CORREGIR DEFINICIÓN DEL MÉTODO

from typing import Dict, Any
from models.schemas import FileCrossRequest
from services.duckdb_service.duckdb_service import duckdb_service

class CrossController:
    """Controlador para operaciones de cruce de archivos"""
    
    def __init__(self):
        """Inicializar controlador"""
        self.duckdb_service = duckdb_service

    def _validate_key_columns(self, request: FileCrossRequest):
        """Valida que las columnas clave estén presentes"""
        if not request.key_column_file1 or not request.key_column_file2:
            raise ValueError("Las columnas clave no pueden estar vacías")


    def _validate_and_log_columns_to_include(self, request: FileCrossRequest):
        """Valida y registra columnas a incluir"""
        if not request.columns_to_include:
            return
        
        file1_cols = request.columns_to_include.get('file1_columns', [])
        file2_cols = request.columns_to_include.get('file2_columns', [])
        
        if not file1_cols and not file2_cols:
            raise ValueError("Debe especificar al menos una columna para incluir")
        
        # Log de columnas específicas
        if file1_cols:
            print(f"      Archivo 1: {file1_cols[:3]}{'...' if len(file1_cols) > 3 else ''}")
        if file2_cols:
            print(f"      Archivo 2: {file2_cols[:3]}{'...' if len(file2_cols) > 3 else ''}")


    def _execute_cross_join(self, request: FileCrossRequest) -> dict:
        """Ejecuta el cruce de archivos"""
        result = self.duckdb_service.cross_files_ultra_fast(
            file1_id=request.file1_key,
            file2_id=request.file2_key,
            key_column_file1=request.key_column_file1,
            key_column_file2=request.key_column_file2,
            join_type=request.cross_type.upper(),
            columns_to_include=request.columns_to_include
        )
        
        if not result.get("success"):
            raise ValueError("El cruce no fue exitoso")
        
        print("Cruce exitoso")
        return result


    def _build_success_response(self, result: dict) -> dict:
        """Construye respuesta exitosa del cruce"""
        return {
            "success": True,
            "data": result["data"],
            "columns": result["columns"],
            "total_rows": result["total_rows"],
            "statistics": result.get("statistics", {}),
            "ultra_fast": True,
            "method": "robust_cross_join_validated"
        }


    def _get_table_columns_sql(self, file_key: str) -> str:
        """Construye SQL para obtener columnas de una tabla"""
        if file_key not in self.duckdb_service.loaded_tables:
            return ""
        
        info = self.duckdb_service.loaded_tables[file_key]
        
        if info.get("type") == "lazy":
            return f"DESCRIBE SELECT * FROM read_parquet('{info['parquet_path']}')"
        else:
            return f"DESCRIBE {info['table_name']}"


    def _debug_file_columns(self, request: FileCrossRequest):
        """Intenta obtener información de debug sobre las columnas de los archivos"""
        try:
            # Debug archivo 1
            cols1_sql = self._get_table_columns_sql(request.file1_key)
            if cols1_sql:
                cols1_result = self.duckdb_service.conn.execute(cols1_sql).fetchall()
                [row[0] for row in cols1_result]
            
            # Debug archivo 2
            cols2_sql = self._get_table_columns_sql(request.file2_key)
            if cols2_sql:
                cols2_result = self.duckdb_service.conn.execute(cols2_sql).fetchall()
                [row[0] for row in cols2_result]
                
        except Exception as debug_error:
            print(f"   Error en debug: {debug_error}")


    def _enhance_error_message(self, error: Exception) -> str:
        """Mejora el mensaje de error con información específica"""
        error_msg = str(error)
        
        if "syntax error" in error_msg.lower():
            return f"Error de sintaxis SQL en el cruce. Verifique que las columnas especificadas existen y tienen nombres válidos: {error_msg}"
        elif "not found" in error_msg.lower():
            return f"Columna no encontrada en los archivos: {error_msg}"
        
        return error_msg


    def perform_cross(self, request: FileCrossRequest) -> Dict[str, Any]:
        """Perform cross con logging detallado y manejo robusto de errores"""
        try:
            # PASO 1: Validaciones
            self._validate_key_columns(request)
            self._validate_and_log_columns_to_include(request)
            
            # PASO 2: Ejecutar cruce
            try:
                result = self._execute_cross_join(request)
                return self._build_success_response(result)
                
            except Exception as join_error:
                print(f"Error específico en cruce: {join_error}")
                
                # PASO 3: Intentar debug
                self._debug_file_columns(request)
                
                raise join_error
                
        except Exception as e:
            import traceback
            traceback.print_exc()
            
            # PASO 4: Mejorar mensaje de error
            enhanced_msg = self._enhance_error_message(e)
            raise ValueError(enhanced_msg)


    def _validate_file_loaded(self, file_id: str) -> dict:
        """Valida que el archivo existe y retorna su información"""
        if file_id not in self.duckdb_service.loaded_tables:
            raise ValueError(f"Archivo {file_id} no está cargado en DuckDB")
        
        table_info = self.duckdb_service.loaded_tables[file_id]
        
        if not table_info:
            raise ValueError(f"No se encontró información para el archivo {file_id}")
        
        return table_info


    def _build_columns_query(self, file_id: str, table_info: dict) -> tuple:
        """Construye query SQL para obtener columnas según tipo de tabla"""
        table_type = table_info.get("type", "table")
        
        if table_type == "lazy":
            parquet_path = table_info.get("parquet_path")
            if not parquet_path:
                raise ValueError(f"No se encontró ruta de Parquet para {file_id}")
            
            print(f"Consultando Parquet: {parquet_path}")
            return (f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')", table_type)
        
        else:
            table_name = table_info.get("table_name")
            if not table_name:
                raise ValueError(f"No se encontró nombre de tabla para {file_id}")
            
            print(f"Consultando tabla: {table_name}")
            return (f"DESCRIBE {table_name}", table_type)


    def _extract_column_names(self, columns_result: list, file_id: str) -> list:
        """Extrae y valida nombres de columnas del resultado"""
        if not columns_result:
            raise ValueError(f"No se pudieron obtener columnas para {file_id}")
        
        columns = []
        for row in columns_result:
            if row and len(row) > 0 and row[0]:
                columns.append(row[0])
        
        if not columns:
            raise ValueError(f"No se encontraron columnas válidas para {file_id}")
        
        return columns


    def _execute_columns_query(self, columns_sql: str, file_id: str) -> list:
        """Ejecuta query SQL para obtener columnas"""
        try:
            columns_result = self.duckdb_service.conn.execute(columns_sql).fetchall()
            return self._extract_column_names(columns_result, file_id)
            
        except Exception as sql_error:
            raise ValueError(f"Error en consulta SQL para {file_id}: {str(sql_error)}")


    def get_file_columns_for_cross(self, file_id: str, sheet_name: str = None) -> Dict[str, Any]:
        """CORREGIDO: Manejo robusto de None values con validaciones extraídas"""
        try:
            # PASO 1: Validar que el archivo existe
            table_info = self._validate_file_loaded(file_id)
            
            # PASO 2: Construir query según tipo de tabla
            columns_sql, table_type = self._build_columns_query(file_id, table_info)
            
            # PASO 3: Ejecutar consulta y extraer columnas
            columns = self._execute_columns_query(columns_sql, file_id)
            
            # PASO 4: Retornar respuesta exitosa
            return {
                "success": True,
                "file_id": file_id,
                "columns": columns,
                "total_columns": len(columns),
                "table_type": table_type,
                "method": f"describe_{table_type}"
            }
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            raise ValueError(f"Error obteniendo columnas para {file_id}: {str(e)}")



# INSTANCIA GLOBAL DEL CONTROLADOR
cross_controller = CrossController()
