# services/technical_note_services/report_service.py
from typing import Dict, Any, List, Optional
from services.duckdb_service.duckdb_service import duckdb_service
from services.keyword_age_report import ColumnKeywordReportService, KeywordRule

class ReportService:
    """Servicio especializado para generación de reportes"""
    
    def __init__(self):
        self.column_service = ColumnKeywordReportService()
    
    def generate_keyword_age_report(
        self,
        data_source: str,
        filename: str,
        keywords: Optional[List[str]] = None,
        min_count: int = 0,
        include_temporal: bool = True,
        geographic_filters: Optional[Dict[str, Optional[str]]] = None
    ) -> Dict[str, Any]:
        """Genera reporte de palabras clave y edades - CORREGIDO"""
        try:
            print(f"📊 Generando reporte para: {filename}")
            
            geographic_filters = geographic_filters or {}
            departamento = geographic_filters.get('departamento')
            municipio = geographic_filters.get('municipio') 
            ips = geographic_filters.get('ips')
            
            print(f"🗺️ Filtros: Dept={departamento}, Mun={municipio}, IPS={ips}")
            
            # Obtener columnas y configurar reglas
            columns = self._get_table_columns(data_source)
            rules = self._setup_keyword_rules(keywords)
            service = ColumnKeywordReportService(keywords=rules)
            matches = service.match_columns(columns)
            
            if not matches:
                return self._build_empty_report(filename, keywords, geographic_filters)
            
            # Ejecutar consulta principal
            items = self._execute_main_query(
                service, data_source, matches, 
                departamento, municipio, ips, min_count
            )
            
            # ✅ INICIALIZAR temporal_data vacío
            combined_temporal_data = {}
            
            if include_temporal and items:
                # Análisis temporal tradicional (fechas)
                temporal_data = self._execute_temporal_analysis(
                    service, data_source, matches,
                    departamento, municipio, ips
                )
                print(f"📅 Datos temporales obtenidos: {len(temporal_data)} entradas")
                
                # Análisis de estados de vacunación  
                vaccination_states_data = self._execute_vaccination_states_analysis(
                    data_source, matches, departamento, municipio, ips
                )
                print(f"💉 Datos de estados obtenidos: {len(vaccination_states_data)} entradas")
                
                # ✅ COMBINAR CORRECTAMENTE - NO REEMPLAZAR
                combined_temporal_data.update(temporal_data)  # Primero fechas
                combined_temporal_data.update(vaccination_states_data)  # Luego estados
                
                print(f"🔄 Datos combinados: {len(combined_temporal_data)} entradas totales")
                print(f"🔍 Claves finales: {list(combined_temporal_data.keys())}")
                
                # ✅ DEBUG: Verificar qué contiene cada entrada
                for key, data in combined_temporal_data.items():
                    data_type = data.get('type', 'temporal')
                    print(f"  - {key}: tipo={data_type}")
            
            # Calcular totales
            totals_by_keyword = self._calculate_totals(items)
            
            return self._build_success_report(
                filename, keywords, geographic_filters, 
                items, totals_by_keyword, combined_temporal_data, data_source
            )
            
        except Exception as e:
            print(f"❌ Error generando reporte: {e}")
            import traceback
            traceback.print_exc()
            raise Exception(f"Error en generación de reporte: {e}")
    
    def _get_table_columns(self, data_source: str) -> List[str]:
        """Obtiene columnas de la tabla"""
        try:
            describe_sql = f"DESCRIBE SELECT * FROM {data_source}"
            columns_result = duckdb_service.conn.execute(describe_sql).fetchall()
            columns = [row[0] for row in columns_result]
            print(f"📋 Columnas obtenidas: {len(columns)} columnas")
            return columns
        except Exception as e:
            print(f"❌ Error obteniendo columnas: {e}")
            raise Exception("Error analizando estructura de datos")
    
    def _setup_keyword_rules(self, keywords: Optional[List[str]]) -> Optional[List[KeywordRule]]:
        """Configura reglas de palabras clave"""
        if not keywords:
            return None
        return [KeywordRule(name=k, synonyms=(k.lower(),)) for k in keywords]
    
    def _execute_main_query(
        self, service, data_source: str, matches, 
        departamento, municipio, ips, min_count: int
    ) -> List[Dict[str, Any]]:
        """Ejecuta la consulta principal del reporte"""
        basic_sql = service.build_report_sql_with_filters(
            data_source, matches, duckdb_service.escape_identifier,
            departamento, municipio, ips
        )
        
        try:
            basic_result = duckdb_service.conn.execute(basic_sql).fetchall()
            print(f"✅ SQL básico ejecutado: {len(basic_result)} filas")
            
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
            
            return items
            
        except Exception as e:
            print(f"❌ Error en consulta principal: {e}")
            raise Exception(f"Error ejecutando consulta: {e}")
    
    def _execute_temporal_analysis(
        self, service, data_source: str, matches,
        departamento, municipio, ips
    ) -> Dict[str, Any]:
        """Ejecuta análisis temporal"""
        temporal_data = {}
        
        try:
            temporal_sql = service.build_temporal_report_sql_with_filters(
                data_source, matches, duckdb_service.escape_identifier,
                departamento, municipio, ips
            )
            
            temporal_result = duckdb_service.conn.execute(temporal_sql).fetchall()
            print(f"✅ SQL temporal ejecutado: {len(temporal_result)} filas")
            
            temporal_data = self._process_temporal_results(temporal_result)
            print(f"📊 Análisis temporal completado: {len(temporal_data)} columnas")
            
        except Exception as e:
            print(f"⚠️ Error temporal (continuando): {e}")
        
        return temporal_data
    
    def _process_temporal_results(self, temporal_result) -> Dict[str, Any]:
        """Procesa resultados del análisis temporal"""
        temporal_data = {}
        
        month_names = {
            1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
            5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto", 
            9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
        }
        
        for row in temporal_result:
            column_name = str(row[0]) if row[0] is not None else ""
            keyword = str(row[1]) if row[1] is not None else ""
            age_range = str(row[2]) if row[2] is not None else ""
            year = int(row[3]) if row[3] is not None and row[3] > 0 else None
            month = int(row[4]) if row[4] is not None and row[4] > 0 else None
            count = int(row[5]) if row[5] is not None else 0
            
            if self._is_valid_temporal_data(year, month):
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
                
                month_name = month_names.get(month, f"Mes {month}")
                temporal_data[column_key]["years"][year_str]["months"][month_name] = {
                    "month": month,
                    "month_name": month_name,
                    "count": count
                }
                temporal_data[column_key]["years"][year_str]["total"] += count
        
        return temporal_data
    
    def _is_valid_temporal_data(self, year, month) -> bool:
        """Valida datos temporales"""
        return (year and year > 1900 and year < 2100 and 
                month and 1 <= month <= 12)
    
    def _calculate_totals(self, items: List[Dict[str, Any]]) -> Dict[str, int]:
        """Calcula totales por palabra clave"""
        totals_by_keyword = {}
        for item in items:
            keyword = item["keyword"]
            totals_by_keyword[keyword] = totals_by_keyword.get(keyword, 0) + item["count"]
        return totals_by_keyword
    
    def _build_empty_report(
        self, filename: str, keywords: Optional[List[str]], 
        geographic_filters: Dict[str, Optional[str]]
    ) -> Dict[str, Any]:
        """Construye reporte vacío cuando no hay matches"""
        return {
            "success": True,
            "filename": filename,
            "rules": {"keywords": keywords or []},
            "geographic_filters": {**geographic_filters, "filter_type": "duckdb_service_methods"},
            "items": [],
            "totals_by_keyword": {},
            "temporal_data": {},
            "message": "No se encontraron columnas con las palabras clave especificadas"
        }
    
    def _build_success_report(
        self, filename: str, keywords: Optional[List[str]], 
        geographic_filters: Dict[str, Optional[str]], 
        items: List[Dict[str, Any]], 
        totals_by_keyword: Dict[str, int], 
        temporal_data: Dict[str, Any], 
        data_source: str
    ) -> Dict[str, Any]:
        """Construye reporte exitoso"""
        return {
            "success": True,
            "filename": filename,
            "rules": {"keywords": keywords or []},
            "geographic_filters": {**geographic_filters, "filter_type": "duckdb_service_methods"},
            "items": items,
            "totals_by_keyword": totals_by_keyword,
            "temporal_data": temporal_data,
            "ultra_fast": True,
            "engine": "DuckDB_Service_Existing_Methods",
            "data_source_used": data_source
        }
    
    def _execute_vaccination_states_analysis(
        self, data_source: str, matches, 
        departamento, municipio, ips
    ) -> Dict[str, Any]:
        """Ejecuta análisis de estados para columnas de vacunación - CON DEBUG"""
        states_data = {}
        
        try:
            print(f"🔍 Analizando estados de vacunación...")
            
            # Identificar columnas de vacunación con estados
            vaccination_columns = []
            for match in matches:
                column_name = match['column']
                if any(keyword in column_name.lower() for keyword in ['vacunación', 'vacunacion']):
                    print(f"📋 Evaluando columna de vacunación: {column_name}")
                    
                    # Verificar si la columna contiene estados
                    if self._column_has_states(data_source, column_name):
                        vaccination_columns.append(match)
                        print(f"✅ Columna con estados: {column_name}")
                    else:
                        print(f"ℹ️ Columna sin estados: {column_name}")
            
            print(f"💉 Columnas de vacunación con estados: {len(vaccination_columns)}")
            
            if not vaccination_columns:
                print("⚠️ No se encontraron columnas de vacunación con estados")
                return states_data
            
            # Construir y ejecutar consulta para estados
            states_sql = self._build_vaccination_states_sql_simple(
                data_source, vaccination_columns, 
                duckdb_service.escape_identifier,
                departamento, municipio, ips
            )
            
            print(f"🔧 Ejecutando SQL de estados...")
            states_result = duckdb_service.conn.execute(states_sql).fetchall()
            print(f"✅ SQL estados ejecutado: {len(states_result)} filas")
            
            # Procesar resultados de estados
            states_data = self._process_vaccination_states_results(states_result)
            print(f"📊 Estados procesados: {len(states_data)} columnas")
            
            # ✅ DEBUG FINAL
            for key, data in states_data.items():
                print(f"🔍 Estado final: {key} -> tipo: {data.get('type')}, estados: {list(data.get('states', {}).keys())}")
                
        except Exception as e:
            print(f"❌ Error análisis estados vacunación: {e}")
            import traceback
            traceback.print_exc()
        
        return states_data

    def _column_has_states(self, data_source: str, column_name: str) -> bool:
        """Verifica si una columna tiene estados en lugar de fechas"""
        try:
            escaped_column = duckdb_service.escape_identifier(column_name)
            check_sql = f"""
            SELECT DISTINCT {escaped_column}
            FROM {data_source} 
            WHERE {escaped_column} IS NOT NULL 
            AND TRIM(LOWER({escaped_column})) IN ('completo', 'incompleto', 'complete', 'incomplete')
            LIMIT 1
            """
            
            result = duckdb_service.conn.execute(check_sql).fetchall()
            return len(result) > 0
            
        except Exception as e:
            print(f"Error verificando estados en columna {column_name}: {e}")
            return False

    def _build_vaccination_states_sql_simple(
        self, data_source: str, vaccination_columns, 
        escape_func, departamento, municipio, ips
    ) -> str:
        """Construye SQL simple para análisis de estados - CORREGIDO age_range"""
        
        queries = []
        
        for match in vaccination_columns:
            column_name = match['column']
            keyword = match['keyword']
            # ✅ USAR EL MISMO age_range QUE EL ITEM PRINCIPAL
            age_range = match['age_range']  # En lugar de hardcoded 'Todos los grupos'
            
            escaped_column = escape_func(column_name)
            
            # Construir filtros geográficos base
            where_conditions = []
            
            if departamento:
                escaped_dept = escape_func('departamento')
                where_conditions.append(f"{escaped_dept} = '{departamento}'")
            
            if municipio:
                escaped_mun = escape_func('municipio') 
                where_conditions.append(f"{escaped_mun} = '{municipio}'")
                
            if ips:
                escaped_ips = escape_func('ips')
                where_conditions.append(f"{escaped_ips} = '{ips}'")
            
            base_where = " AND ".join(where_conditions) if where_conditions else "1=1"
            
            # Query para "Completo"
            query_completo = f"""
            SELECT 
                '{column_name}' as column_name,
                '{keyword}' as keyword,
                '{age_range}' as age_range,
                'Completo' as estado,
                COUNT(*) as count
            FROM {data_source}
            WHERE {base_where}
            AND {escaped_column} IS NOT NULL
            AND (
                TRIM(LOWER({escaped_column})) = 'completo' OR 
                TRIM(LOWER({escaped_column})) = 'complete'
            )
            """
            
            # Query para "Incompleto"  
            query_incompleto = f"""
            SELECT 
                '{column_name}' as column_name,
                '{keyword}' as keyword,
                '{age_range}' as age_range,
                'Incompleto' as estado,
                COUNT(*) as count
            FROM {data_source}
            WHERE {base_where}
            AND {escaped_column} IS NOT NULL
            AND (
                TRIM(LOWER({escaped_column})) = 'incompleto' OR 
                TRIM(LOWER({escaped_column})) = 'incomplete'
            )
            """
            
            queries.extend([query_completo, query_incompleto])
            print(f"📝 Agregadas queries para: {column_name} con age_range: {age_range}")
        
        final_sql = " UNION ALL ".join(queries) + " ORDER BY column_name, estado"
        
        return final_sql

    def _process_vaccination_states_results(self, states_result) -> Dict[str, Any]:
        """Procesa resultados - CON DEBUG EXTENSO"""
        states_data = {}
        
        print(f"🔍 Procesando {len(states_result)} filas de estados")
        
        for i, row in enumerate(states_result):
            column_name = str(row[0]) if row[0] is not None else ""
            keyword = str(row[1]) if row[1] is not None else ""
            age_range = str(row[2]) if row[2] is not None else ""
            estado = str(row[3]) if row[3] is not None else ""
            count = int(row[4]) if row[4] is not None else 0
            
            print(f"📊 Fila {i+1}: {column_name} - {estado}: {count}")
            
            if estado in ['Completo', 'Incompleto'] and count > 0:
                # ✅ CLAVE SIMPLE (sin normalización por ahora)
                column_key = f"{column_name}|{keyword}|{age_range}"
                
                if column_key not in states_data:
                    states_data[column_key] = {
                        "column": column_name,
                        "keyword": keyword,
                        "age_range": age_range,
                        "type": "states",  # ✅ CRUCIAL: Este campo debe estar
                        "states": {}
                    }
                    print(f"✅ Nueva entrada: {column_key}")
                
                states_data[column_key]["states"][estado] = {
                    "state": estado,
                    "count": count
                }
                print(f"✅ Estado agregado: {estado} = {count}")
        
        print(f"📊 Resultado final: {len(states_data)} entradas")
        for key, data in states_data.items():
            estados = list(data["states"].keys())
            print(f"  ✅ {key}: tipo={data['type']}, estados={estados}")
        
        return states_data
