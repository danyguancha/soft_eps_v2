# controllers/cross_controller.py - CORREGIR DEFINICIÓN DEL MÉTODO

from typing import Dict, Any
from models.schemas import FileCrossRequest
from services.duckdb_service.duckdb_service import duckdb_service

class CrossController:
    """Controlador para operaciones de cruce de archivos"""
    
    def __init__(self):
        """Inicializar controlador"""
        self.duckdb_service = duckdb_service

    def perform_cross(self, request: FileCrossRequest) -> Dict[str, Any]:
        """CORREGIDO: Perform cross con logging detallado"""
        try:            
            # VALIDACIONES
            if not request.key_column_file1 or not request.key_column_file2:
                raise ValueError("Las columnas clave no pueden estar vacías")
            
            if request.columns_to_include:
                file1_cols = request.columns_to_include.get('file1_columns', [])
                file2_cols = request.columns_to_include.get('file2_columns', [])
                
                if not file1_cols and not file2_cols:
                    raise ValueError("Debe especificar al menos una columna para incluir")
                
                # LOG DE COLUMNAS ESPECÍFICAS
                if file1_cols:
                    print(f"      Archivo 1: {file1_cols[:3]}{'...' if len(file1_cols) > 3 else ''}")
                if file2_cols:
                    print(f"      Archivo 2: {file2_cols[:3]}{'...' if len(file2_cols) > 3 else ''}")
            
            # EJECUTAR CRUCE CON MANEJO DE ERRORES
            try:
                result = self.duckdb_service.cross_files_ultra_fast(
                    file1_id=request.file1_key,
                    file2_id=request.file2_key,
                    key_column_file1=request.key_column_file1,
                    key_column_file2=request.key_column_file2,
                    join_type=request.cross_type.upper(),
                    columns_to_include=request.columns_to_include
                )
                
                if result.get("success"):
                    print(f"Cruce exitoso")                    
                    return {
                        "success": True,
                        "data": result["data"],
                        "columns": result["columns"],
                        "total_rows": result["total_rows"],
                        "statistics": result.get("statistics", {}),
                        "ultra_fast": True,
                        "method": "robust_cross_join_validated"
                    }
                else:
                    raise Exception("El cruce no fue exitoso")
                    
            except Exception as join_error:
                print(f"Error específico en cruce: {join_error}")                
                # Verificar que las columnas clave existen
                try:
                    # Obtener columnas de archivo 1
                    if request.file1_key in self.duckdb_service.loaded_tables:
                        info1 = self.duckdb_service.loaded_tables[request.file1_key]
                        if info1.get("type") == "lazy":
                            cols1_sql = f"DESCRIBE SELECT * FROM read_parquet('{info1['parquet_path']}')"
                        else:
                            cols1_sql = f"DESCRIBE {info1['table_name']}"
                        cols1_result = self.duckdb_service.conn.execute(cols1_sql).fetchall()
                        cols1 = [row[0] for row in cols1_result]
                    
                    # Obtener columnas de archivo 2
                    if request.file2_key in self.duckdb_service.loaded_tables:
                        info2 = self.duckdb_service.loaded_tables[request.file2_key]
                        if info2.get("type") == "lazy":
                            cols2_sql = f"DESCRIBE SELECT * FROM read_parquet('{info2['parquet_path']}')"
                        else:
                            cols2_sql = f"DESCRIBE {info2['table_name']}"
                        cols2_result = self.duckdb_service.conn.execute(cols2_sql).fetchall()
                        cols2 = [row[0] for row in cols2_result]
                        
                except Exception as debug_error:
                    print(f"   Error en debug: {debug_error}")
                
                raise join_error
                
        except Exception as e:
            import traceback
            traceback.print_exc()
            
            # MENSAJE DE ERROR ESPECÍFICO
            error_msg = str(e)
            if "syntax error" in error_msg.lower():
                error_msg = f"Error de sintaxis SQL en el cruce. Verifique que las columnas especificadas existen y tienen nombres válidos: {error_msg}"
            elif "not found" in error_msg.lower():
                error_msg = f"Columna no encontrada en los archivos: {error_msg}"
            
            raise Exception(error_msg)

    def get_file_columns_for_cross(self, file_id: str, sheet_name: str = None) -> Dict[str, Any]:
        """CORREGIDO: Manejo robusto de None values"""
        try:            
            # VALIDAR que el archivo existe en loaded_tables
            if file_id not in self.duckdb_service.loaded_tables:
                raise ValueError(f"Archivo {file_id} no está cargado en DuckDB")
            
            # OBTENER información directamente de loaded_tables
            table_info = self.duckdb_service.loaded_tables[file_id]
            
            if not table_info:
                raise ValueError(f"No se encontró información para el archivo {file_id}")
            
            # CONSTRUIR consulta según el tipo de tabla
            table_type = table_info.get("type", "table")
            
            if table_type == "lazy":
                parquet_path = table_info.get("parquet_path")
                if not parquet_path:
                    raise ValueError(f"No se encontró ruta de Parquet para {file_id}")
                
                columns_sql = f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')"
                print(f"Consultando Parquet: {parquet_path}")
                
            else:
                table_name = table_info.get("table_name")
                if not table_name:
                    raise ValueError(f"No se encontró nombre de tabla para {file_id}")
                
                columns_sql = f"DESCRIBE {table_name}"
                print(f"Consultando tabla: {table_name}")
            
            # EJECUTAR consulta con manejo robusto
            try:
                columns_result = self.duckdb_service.conn.execute(columns_sql).fetchall()
                
                # VALIDAR que el resultado no sea None o vacío
                if not columns_result:
                    raise ValueError(f"No se pudieron obtener columnas para {file_id}")
                
                # EXTRAER nombres de columnas con validación
                columns = []
                for row in columns_result:
                    if row and len(row) > 0 and row[0]:  # Validar que la fila y el primer elemento existan
                        columns.append(row[0])
                
                if not columns:
                    raise ValueError(f"No se encontraron columnas válidas para {file_id}")
                
                
                return {
                    "success": True,
                    "file_id": file_id,
                    "columns": columns,
                    "total_columns": len(columns),
                    "table_type": table_type,
                    "method": f"describe_{table_type}"
                }
                
            except Exception as sql_error:
                raise Exception(f"Error en consulta SQL para {file_id}: {str(sql_error)}")
                
        except Exception as e:
            import traceback
            traceback.print_exc()
            raise Exception(f"Error obteniendo columnas para {file_id}: {str(e)}")


# INSTANCIA GLOBAL DEL CONTROLADOR
cross_controller = CrossController()
