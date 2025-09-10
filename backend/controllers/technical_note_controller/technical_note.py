# controllers/technical_note_controller/technical_note.py
import os
from typing import Dict, Any, List, Optional
from fastapi import HTTPException
from services.duckdb_service import duckdb_service
from services.aux_duckdb_services.query_pagination import QueryPagination

class TechnicalNoteController:
    """Controlador ULTRA-RÁPIDO usando DuckDB existente para archivos técnicos"""
    
    def __init__(self, storage_manager):
        self.storage_manager = storage_manager
        self.static_files_dir = "technical_note"
        # ✅ USAR CACHE EXISTENTE - no crear uno separado
        self.loaded_technical_files = {}
    
    def _ensure_data_source_available(self, filename: str, file_key: str) -> str:
        """
        Usa métodos existentes de DuckDBService para asegurar fuente de datos
        """
        try:
            print(f"🔍 Verificando fuente de datos para: {filename}")
            
            # ✅ USAR MÉTODO EXISTENTE: ensure_parquet_exists_or_regenerate
            if QueryPagination().ensure_parquet_exists_or_regenerate(file_key, duckdb_service.loaded_tables):
                # Parquet existe y está válido
                table_info = duckdb_service.loaded_tables[file_key]
                parquet_path = table_info.get('parquet_path')
                
                print(f"✅ Usando Parquet existente: {parquet_path}")
                return f"read_parquet('{parquet_path}')"
            
            # ✅ USAR MÉTODO EXISTENTE: _load_file_on_demand_with_regeneration
            if QueryPagination()._load_file_on_demand_with_regeneration(file_key, duckdb_service.loaded_tables):
                table_info = duckdb_service.loaded_tables[file_key]
                parquet_path = table_info.get('parquet_path')
                
                print(f"✅ Cargado bajo demanda: {parquet_path}")
                return f"read_parquet('{parquet_path}')"
            
            # ✅ ÚLTIMA OPCIÓN: Convertir desde CSV usando método existente
            csv_path = self._find_csv_file(filename)
            if not csv_path:
                raise Exception(f"No se encontró archivo CSV para {filename}")
            
            print(f"🔄 Convirtiendo usando DuckDBService: {csv_path}")
            
            # ✅ USAR MÉTODO EXISTENTE: convert_file_to_parquet
            _, ext = os.path.splitext(filename)
            conversion_result = duckdb_service.convert_file_to_parquet(
                file_path=csv_path,
                file_id=file_key,
                original_name=filename,
                ext=ext.replace('.', '')
            )
            
            if conversion_result.get("success"):
                parquet_path = conversion_result.get("parquet_path")
                
                # ✅ USAR MÉTODO EXISTENTE: load_parquet_lazy
                table_name = duckdb_service.load_parquet_lazy(file_key, parquet_path)
                
                print(f"✅ Conversión exitosa con DuckDBService: {parquet_path}")
                return f"read_parquet('{parquet_path}')"
            
            # ✅ FALLBACK DIRECTO A CSV si todo lo demás falla
            print(f"⚠️ Fallback a CSV directo: {csv_path}")
            return f"read_csv('{csv_path}', AUTO_DETECT=true, header=true)"
            
        except Exception as e:
            print(f"❌ Error asegurando fuente de datos: {e}")
            raise Exception(f"No se pudo obtener fuente de datos para {filename}: {e}")
    
    def _find_csv_file(self, filename: str) -> Optional[str]:
        """Busca el archivo CSV en las rutas conocidas"""
        possible_paths = [
            os.path.join(self.static_files_dir, filename),
            os.path.join(".", filename),
            filename
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                print(f"📁 CSV encontrado: {path}")
                return os.path.abspath(path)
        
        print(f"❌ CSV no encontrado en rutas: {possible_paths}")
        return None

    def get_geographic_values(
        self, 
        filename: str, 
        geo_type: str,
        departamento: Optional[str] = None,
        municipio: Optional[str] = None
    ) -> Dict[str, Any]:
        """Obtiene valores únicos de columnas geográficas con filtros en cascada"""
        try:
            print(f"🗺️ Obteniendo {geo_type} para {filename}")
            print(f"🔍 Filtros recibidos: departamento='{departamento}', municipio='{municipio}'")
            
            file_key = f"technical_{filename.replace('.', '_').replace(' ', '_').replace('-', '_')}"
            
            # ✅ USAR MÉTODO EXISTENTE PARA ASEGURAR FUENTE DE DATOS
            try:
                data_source = self._ensure_data_source_available(filename, file_key)
                print(f"✅ Fuente de datos asegurada: {data_source}")
            except Exception as data_error:
                print(f"❌ No se pudo asegurar fuente de datos: {data_error}")
                return {
                    "success": False,
                    "error": f"No se puede leer el archivo: {data_error}",
                    "geo_type": geo_type,
                    "values": []
                }
            
            # ✅ VERIFICAR QUE PODEMOS LEER EL ARCHIVO
            try:
                test_query = f"SELECT COUNT(*) FROM {data_source}"
                test_result = duckdb_service.conn.execute(test_query).fetchone()
                print(f"✅ Archivo legible: {test_result[0]} filas totales")
            except Exception as test_error:
                print(f"❌ Error leyendo archivo: {test_error}")
                return {
                    "success": False,
                    "error": f"No se puede leer el archivo: {test_error}",
                    "geo_type": geo_type,
                    "values": []
                }
            
            # ✅ CONSTRUIR FILTROS PADRE CORRECTAMENTE
            parent_filter = {}
            
            if geo_type == 'municipios' and departamento and departamento.strip():
                parent_filter['departamento'] = departamento.strip()
                print(f"✅ Filtro para municipios: departamento = '{departamento}'")
            
            elif geo_type == 'ips':
                if municipio and municipio.strip():
                    parent_filter['municipio'] = municipio.strip()
                    print(f"✅ Filtro para IPS: municipio = '{municipio}'")
                if departamento and departamento.strip():
                    parent_filter['departamento'] = departamento.strip()
                    print(f"✅ Filtro adicional para IPS: departamento = '{departamento}'")
            
            from services.keyword_age_report import ColumnKeywordReportService
            service = ColumnKeywordReportService()
            
            # ✅ MAPEO CORREGIDO - MANTENER IPS COMO IPS
            geo_type_mapping = {
                'departamentos': 'departamento',
                'municipios': 'municipio',
                'ips': 'ips'  # ✅ MANTENER 'ips' como 'ips'
            }
            
            geo_type_for_service = geo_type_mapping.get(geo_type, geo_type)
            print(f"🔄 Mapeo: {geo_type} -> {geo_type_for_service}")
            
            geo_sql = service.get_unique_geographic_values_sql(
                data_source, geo_type_for_service, duckdb_service.escape_identifier, parent_filter
            )
            
            # Ejecutar consulta
            try:
                print(f"🔧 Ejecutando SQL: {geo_sql}")
                result = duckdb_service.conn.execute(geo_sql).fetchall()
                values = []
                
                for row in result:
                    if row[0] is not None:
                        value = str(row[0]).strip()
                        if value and value not in ['NULL', 'null', 'None', 'none', 'NaN', 'nan', '']:
                            values.append(value)
                
                # Eliminar duplicados y ordenar
                values = sorted(list(set(values)))
                
                print(f"✅ {geo_type} obtenidos: {len(values)} valores únicos")
                if values:
                    print(f"📋 Primeros 10 valores: {values[:10]}")
                else:
                    print("⚠️ No se obtuvieron valores - revisar filtros o datos")
                
                return {
                    "success": True,
                    "filename": filename,
                    "geo_type": geo_type,
                    "values": values,
                    "total_values": len(values),
                    "filters_applied": parent_filter,
                    "engine": "DuckDB_Service_Methods"
                }
                
            except Exception as sql_error:
                print(f"❌ Error SQL geográfico: {sql_error}")
                print(f"🔧 SQL que falló: {geo_sql}")
                return {
                    "success": False,
                    "error": f"Error ejecutando consulta geográfica: {sql_error}",
                    "geo_type": geo_type,
                    "values": [],
                    "sql_used": geo_sql
                }
                
        except Exception as e:
            print(f"❌ Error en get_geographic_values: {e}")
            import traceback
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=f"Error obteniendo valores geográficos: {str(e)}")
        
    def read_technical_file_data_paginated(
        self, 
        filename: str, 
        page: int = 1, 
        page_size: int = 1000,
        sheet_name: Optional[str] = None,
        filters: Optional[List[Dict[str, Any]]] = None,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None
    ) -> Dict[str, Any]:
        """PAGINACIÓN ULTRA-RÁPIDA usando DuckDB service existente"""
        try:
            file_path = os.path.join(self.static_files_dir, filename)
            
            if not os.path.exists(file_path):
                raise HTTPException(status_code=404, detail=f"Archivo no encontrado: {filename}")
            
            # ✅ USAR SISTEMA DE FILE_ID EXISTENTE
            file_key = f"technical_{filename.replace('.', '_').replace(' ', '_').replace('-', '_')}"
            
            # ✅ VERIFICAR SI YA ESTÁ CARGADO EN EL SISTEMA
            if file_key not in duckdb_service.loaded_tables:
                print(f"🔄 Cargando archivo técnico: {filename}")
                
                # Obtener extensión
                _, ext = os.path.splitext(filename)
                
                # ✅ USAR MÉTODO DE CONVERSIÓN EXISTENTE
                parquet_result = duckdb_service.convert_file_to_parquet(
                    file_path=file_path,
                    file_id=file_key,
                    original_name=filename,
                    ext=ext.replace('.', '')
                )
                
                if parquet_result["success"]:
                    # ✅ USAR MÉTODO DE CARGA EXISTENTE
                    table_name = duckdb_service.load_parquet_lazy(
                        file_key, 
                        parquet_result["parquet_path"]
                    )
                    
                    # Registrar en cache local (compatible con el sistema)
                    self.loaded_technical_files[file_key] = {
                        "table_name": table_name,
                        "columns": parquet_result["columns"],
                        "total_rows": parquet_result["total_rows"],
                        "parquet_path": parquet_result["parquet_path"]
                    }
                    
                    print(f"✅ Archivo técnico cargado: {filename} -> {table_name}")
                else:
                    raise HTTPException(status_code=500, detail="Error convirtiendo archivo técnico")
            else:
                # Ya está cargado, obtener info del sistema
                table_info = duckdb_service.loaded_tables[file_key]
                if file_key not in self.loaded_technical_files:
                    # Sincronizar con cache local
                    self.loaded_technical_files[file_key] = {
                        "table_name": table_info.get("table_name", f"table_{file_key}"),
                        "columns": [],  # Se obtendrán dinámicamente
                        "total_rows": 0,  # Se calculará dinámicamente
                        "parquet_path": table_info.get("parquet_path")
                    }
            
            # ✅ USAR MÉTODO DE QUERY EXISTENTE
            result = QueryPagination().query_data_ultra_fast(
                conn=duckdb_service.conn,
                file_id=file_key,
                filters=filters,
                search=search,
                sort_by=sort_by,
                sort_order=sort_order or "asc",
                page=page,
                page_size=page_size,
                selected_columns= None,
                loaded_tables=duckdb_service.loaded_tables
            )
            
            if not result.get("success", True):
                raise HTTPException(status_code=500, detail=result.get("error", "Error en consulta"))
            
            # Obtener info del archivo (usar sistema existente)
            file_stats = duckdb_service.get_file_stats(file_key)
            technical_info = self.loaded_technical_files[file_key]
            
            return {
                "success": True,
                "filename": filename,
                "display_name": self._generate_display_name(filename),
                "description": self._generate_description(filename),
                "data": result["data"],
                "columns": result["columns"],
                "pagination": result["pagination"],
                "filters_applied": filters or [],
                "search_applied": search,
                "sort_applied": {"column": sort_by, "order": sort_order} if sort_by else None,
                "file_info": {
                    "total_rows": result.get("total_rows", file_stats.get("total_rows", 0)),
                    "total_columns": len(result["columns"]),
                    "processing_method": "duckdb_ultra_fast",
                    "ultra_fast": True,
                    "engine": "DuckDB_Service_Methods"
                }
            }
            
        except HTTPException:
            raise
        except Exception as e:
            print(f"❌ Error en read_technical_file_data_paginated: {e}")
            import traceback
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

    def get_keyword_age_report(
        self,
        filename: str,
        keywords: Optional[List[str]] = None,
        min_count: int = 0,
        include_temporal: bool = True,
        departamento: Optional[str] = None,
        municipio: Optional[str] = None,
        ips: Optional[str] = None
    ) -> Dict[str, Any]:
        """Genera reporte usando métodos existentes de DuckDBService"""
        try:
            print(f"📊 Reporte usando DuckDBService existente para: {filename}")
            print(f"🗺️ Filtros: Dept={departamento}, Mun={municipio}, IPS={ips}")
            
            file_key = f"technical_{filename.replace('.', '_').replace(' ', '_').replace('-', '_')}"
            
            # ✅ USAR INFRAESTRUCTURA EXISTENTE DEL DUCKDBSERVICE
            try:
                data_source = self._ensure_data_source_available(filename, file_key)
                print(f"✅ Fuente de datos asegurada: {data_source}")
            except Exception as data_error:
                print(f"❌ No se pudo asegurar fuente de datos: {data_error}")
                raise HTTPException(
                    status_code=500, 
                    detail=f"No se pudo acceder a los datos de {filename}: {str(data_error)}"
                )
            
            # ✅ OBTENER COLUMNAS USANDO DUCKDB CONNECTION EXISTENTE
            try:
                describe_sql = f"DESCRIBE SELECT * FROM {data_source}"
                columns_result = duckdb_service.conn.execute(describe_sql).fetchall()
                columns = [row[0] for row in columns_result]
                print(f"📋 Columnas obtenidas: {len(columns)} columnas")
            except Exception as col_error:
                print(f"❌ Error obteniendo columnas: {col_error}")
                raise HTTPException(status_code=500, detail=f"Error analizando estructura de {filename}")
            
            # ✅ LÓGICA DE REPORTE CON FILTROS GEOGRÁFICOS
            from services.keyword_age_report import ColumnKeywordReportService, KeywordRule
            
            rules = None
            if keywords:
                rules = [KeywordRule(name=k, synonyms=(k.lower(),)) for k in keywords]
            
            service = ColumnKeywordReportService(keywords=rules)
            matches = service.match_columns(columns)
            
            if not matches:
                return {
                    "success": True,
                    "filename": filename,
                    "rules": {"keywords": keywords or [r.name for r in service.keywords]},
                    "geographic_filters": {
                        "departamento": departamento,
                        "municipio": municipio,
                        "ips": ips,
                        "filter_type": "duckdb_service_methods"
                    },
                    "items": [],
                    "totals_by_keyword": {},
                    "temporal_data": {},
                    "message": "No se encontraron columnas con las palabras clave especificadas",
                    "data_source_used": data_source
                }
            
            # ✅ EJECUTAR QUERIES USANDO CONEXIÓN EXISTENTE
            basic_sql = service.build_report_sql_with_filters(
                data_source, matches, duckdb_service.escape_identifier,
                departamento, municipio, ips
            )
            
            try:
                basic_result = duckdb_service.conn.execute(basic_sql).fetchall()
                print(f"✅ SQL básico ejecutado: {len(basic_result)} filas")
            except Exception as sql_error:
                print(f"❌ Error SQL básico: {sql_error}")
                raise HTTPException(status_code=500, detail=f"Error ejecutando consulta: {str(sql_error)}")
            
            # ✅ PROCESAR RESULTADOS
            items = []
            for row in basic_result:
                count = int(row[3]) if row[3] is not None else 0
                if count >= min_count:
                    items.append({
                        "column": str(row[0]) if row[0] is not None else "",
                        "keyword": str(row[1]) if row[1] is not None else "",
                        "age_range": str(row[2]) if row[2] is not None else "",
                        "count": count
                    })
            
            # ✅ ANÁLISIS TEMPORAL USANDO CONEXIÓN EXISTENTE
            temporal_data = {}
            if include_temporal and items:
                temporal_sql = service.build_temporal_report_sql_with_filters(
                    data_source, matches, duckdb_service.escape_identifier,
                    departamento, municipio, ips
                )
                
                try:
                    temporal_result = duckdb_service.conn.execute(temporal_sql).fetchall()
                    print(f"✅ SQL temporal ejecutado: {len(temporal_result)} filas")
                    
                    # Procesar datos temporales
                    for row in temporal_result:
                        column_name = str(row[0]) if row[0] is not None else ""
                        keyword = str(row[1]) if row[1] is not None else ""
                        age_range = str(row[2]) if row[2] is not None else ""
                        year = int(row[3]) if row[3] is not None and row[3] > 0 else None
                        month = int(row[4]) if row[4] is not None and row[4] > 0 else None
                        count = int(row[5]) if row[5] is not None else 0
                        
                        if year and year > 1900 and year < 2100 and month and 1 <= month <= 12:
                            column_key = f"{column_name}|{keyword}|{age_range}"
                            
                            if column_key not in temporal_data:
                                temporal_data[column_key] = {
                                    "column": column_name,
                                    "keyword": keyword, 
                                    "age_range": age_range,
                                    "years": {}
                                }
                            
                            year_str = str(year)
                            if year_str not in temporal_data[column_key]["years"]:
                                temporal_data[column_key]["years"][year_str] = {
                                    "year": year,
                                    "total": 0,
                                    "months": {}
                                }
                            
                            month_names = {
                                1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
                                5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto", 
                                9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
                            }
                            
                            month_name = month_names.get(month, f"Mes {month}")
                            temporal_data[column_key]["years"][year_str]["months"][month_name] = {
                                "month": month,
                                "month_name": month_name,
                                "count": count
                            }
                            temporal_data[column_key]["years"][year_str]["total"] += count
                    
                    print(f"📊 Análisis temporal completado: {len(temporal_data)} columnas")
                    
                except Exception as temporal_error:
                    print(f"⚠️ Error temporal (continuando): {temporal_error}")
                    temporal_data = {}
            
            # ✅ CALCULAR TOTALES
            totals_by_keyword = {}
            for item in items:
                keyword = item["keyword"]
                totals_by_keyword[keyword] = totals_by_keyword.get(keyword, 0) + item["count"]
            
            print(f"📊 Reporte completado: {len(items)} items, totales: {totals_by_keyword}")
            
            return {
                "success": True,
                "filename": filename,
                "rules": {"keywords": keywords or [r.name for r in service.keywords]},
                "geographic_filters": {
                    "departamento": departamento,
                    "municipio": municipio,
                    "ips": ips,
                    "filter_type": "duckdb_service_methods"
                },
                "items": items,
                "totals_by_keyword": totals_by_keyword,
                "temporal_data": temporal_data,
                "ultra_fast": True,
                "engine": "DuckDB_Service_Existing_Methods",
                "data_source_used": data_source
            }
            
        except HTTPException:
            raise
        except Exception as e:
            print(f"❌ Error completo en reporte: {e}")
            import traceback
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=f"Error generando reporte: {str(e)}")

    def get_technical_file_metadata(self, filename: str) -> Dict[str, Any]:
        """Obtiene metadatos usando infraestructura existente de DuckDBService"""
        try:
            print(f"📋 Obteniendo metadatos para: {filename}")
            
            file_key = f"technical_{filename.replace('.', '_').replace(' ', '_').replace('-', '_')}"
            
            # ✅ USAR MÉTODO EXISTENTE PARA ASEGURAR FUENTE DE DATOS
            data_source = self._ensure_data_source_available(filename, file_key)
            
            # ✅ USAR CONEXIÓN EXISTENTE PARA METADATOS
            describe_sql = f"DESCRIBE SELECT * FROM {data_source}"
            columns_result = duckdb_service.conn.execute(describe_sql).fetchall()
            
            # Obtener conteo total usando conexión existente
            count_sql = f"SELECT COUNT(*) FROM {data_source}"
            total_rows = duckdb_service.conn.execute(count_sql).fetchone()[0]
            
            columns = []
            for row in columns_result:
                columns.append({
                    "name": row[0],
                    "type": row[1],
                    "null": row[2] if len(row) > 2 else "Unknown"
                })
            
            print(f"✅ Metadatos obtenidos: {len(columns)} columnas, {total_rows:,} filas")
            
            return {
                "success": True,
                "filename": filename,
                "display_name": self._generate_display_name(filename),
                "description": self._generate_description(filename),
                "total_rows": total_rows,  # ✅ GARANTIZADO QUE EXISTE
                "total_columns": len(columns),
                "columns": [col["name"] for col in columns],
                "detailed_columns": columns,
                "file_size": 0,  # Se puede obtener del table_info si es necesario
                "extension": "csv",
                "encoding": "utf-8",
                "recommended_page_size": 1000,
                "data_source_used": data_source,
                "engine": "DuckDB_Service_Existing_Methods"
            }
            
        except Exception as e:
            print(f"❌ Error obteniendo metadatos: {e}")
            return {
                "success": False,
                "error": str(e),
                "filename": filename,
                "total_rows": 0,
                "total_columns": 0,
                "columns": []
            }

    def get_column_unique_values(
        self, 
        filename: str, 
        column_name: str,
        sheet_name: Optional[str] = None,
        limit: int = 1000
    ) -> Dict[str, Any]:
        """Valores únicos usando sistema DuckDB existente"""
        try:
            file_key = f"technical_{filename.replace('.', '_').replace(' ', '_').replace('-', '_')}"
            
            # ✅ ASEGURAR FUENTE DE DATOS
            data_source = self._ensure_data_source_available(filename, file_key)
            
            # ✅ USAR MÉTODO EXISTENTE SI ESTÁ DISPONIBLE, SI NO, CREAR QUERY DIRECTA
            try:
                unique_values = duckdb_service.get_unique_values_ultra_fast(
                    file_key, column_name, limit
                )
            except Exception:
                # Fallback a query directa si el método no existe
                column_escaped = duckdb_service.escape_identifier(column_name)
                query = f"""
                SELECT DISTINCT {column_escaped} 
                FROM {data_source} 
                WHERE {column_escaped} IS NOT NULL 
                ORDER BY {column_escaped} 
                LIMIT {limit}
                """
                
                result = duckdb_service.conn.execute(query).fetchall()
                unique_values = [row[0] for row in result if row[0] is not None]
            
            return {
                "filename": filename,
                "column_name": column_name,
                "unique_values": unique_values,
                "total_unique": len(unique_values),
                "limited": len(unique_values) >= limit,
                "limit_applied": limit,
                "ultra_fast": True,
                "engine": "DuckDB_Service_Existing"
            }
            
        except Exception as e:
            print(f"❌ Error en get_column_unique_values: {e}")
            raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

    def get_available_static_files(self) -> List[Dict[str, Any]]:
        """Lista archivos técnicos disponibles"""
        try:
            if not os.path.exists(self.static_files_dir):
                return []
            
            available_files = []
            supported_extensions = {'.csv', '.xlsx', '.xls'}
            
            for filename in os.listdir(self.static_files_dir):
                file_path = os.path.join(self.static_files_dir, filename)
                
                if not os.path.isfile(file_path):
                    continue
                
                _, ext = os.path.splitext(filename)
                if ext.lower() not in supported_extensions:
                    continue
                
                file_size = os.path.getsize(file_path)
                
                available_files.append({
                    "filename": filename,
                    "display_name": self._generate_display_name(filename),
                    "file_path": file_path,
                    "extension": ext.lower().replace('.', ''),
                    "file_size": file_size,
                    "description": self._generate_description(filename),
                    "ultra_fast_ready": True
                })
            
            return available_files
            
        except Exception as e:
            print(f"❌ Error listando archivos técnicos: {e}")
            return []

    def _generate_display_name(self, filename: str) -> str:
        """Genera nombre display amigable"""
        name_map = {
            "AdolescenciaNueva.csv": "Adolescencia",
            "AdultezNueva.csv": "Adultez", 
            "InfanciaNueva.csv": "Infancia",
            "JuventudNueva.csv": "Juventud",
            "PrimeraInfanciaNueva.csv": "Primera Infancia",
            "VejezNueva.csv": "Vejez"
        }
        
        return name_map.get(filename, filename.replace('.csv', '').replace('.xlsx', '').replace('.xls', ''))
    
    def _generate_description(self, filename: str) -> str:
        """Genera descripción del archivo"""
        desc_map = {
            "AdolescenciaNueva.csv": "Datos de población adolescente",
            "AdultezNueva.csv": "Datos de población adulta",
            "InfanciaNueva.csv": "Datos de población infantil", 
            "JuventudNueva.csv": "Datos de población joven",
            "PrimeraInfanciaNueva.csv": "Datos de primera infancia",
            "VejezNueva.csv": "Datos de población adulto mayor"
        }
        
        return desc_map.get(filename, f"Archivo técnico: {filename}")

def get_technical_note_controller():
    from controllers.files_controllers.storage_manager import storage_manager
    return TechnicalNoteController(storage_manager)

technical_note_controller = get_technical_note_controller()
