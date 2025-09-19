from typing import Dict, Any, List, Optional
from services.duckdb_service.duckdb_service import duckdb_service
from services.keyword_age_report import ColumnKeywordReportService, KeywordRule
from controllers.technical_note_controller.age_range_extractor import AgeRangeExtractor

class ReportService:
    """Servicio especializado para generación de reportes CON NUMERADOR/DENOMINADOR"""
    
    def __init__(self):
        self.column_service = ColumnKeywordReportService()
        self.age_extractor = AgeRangeExtractor()
    
    def generate_keyword_age_report(
        self,
        data_source: str,
        filename: str,
        keywords: Optional[List[str]] = None,
        min_count: int = 0,
        include_temporal: bool = True,
        geographic_filters: Optional[Dict[str, Optional[str]]] = None,
        corte_fecha: str = "2025-07-31"
    ) -> Dict[str, Any]:
        """
        🆕 Genera reporte CON NUMERADOR Y DENOMINADOR POR RANGO DE EDAD ESPECÍFICO
        """
        try:
            print(f"\n📊 ========== REPORTE NUMERADOR/DENOMINADOR ==========")
            print(f"📋 Archivo: {filename}")
            print(f"🗓️ Fecha corte: {corte_fecha}")
            print(f"🔍 Keywords: {keywords}")
            
            geographic_filters = geographic_filters or {}
            departamento = geographic_filters.get('departamento')
            municipio = geographic_filters.get('municipio') 
            ips = geographic_filters.get('ips')
            
            print(f"🗺️ Filtros: Dept={departamento}, Mun={municipio}, IPS={ips}")
            
            # ✅ PASO 1: OBTENER COLUMNAS Y BUSCAR COINCIDENCIAS
            columns = self._get_table_columns(data_source)
            rules = self._setup_keyword_rules(keywords)
            service = ColumnKeywordReportService(keywords=rules)
            matches = service.match_columns(columns)
            
            if not matches:
                return self._build_empty_report(filename, keywords, geographic_filters)
            
            print(f"✅ Columnas coincidentes encontradas: {len(matches)}")
            
            # ✅ PASO 2: GENERAR REPORTES CON NUMERADOR/DENOMINADOR POR RANGO ESPECÍFICO
            items_with_numerator_denominator = self._execute_numerator_denominator_analysis(
                data_source, matches, departamento, municipio, ips, min_count, corte_fecha
            )
            
            # ✅ PASO 3: ANÁLISIS TEMPORAL (OPCIONAL)
            combined_temporal_data = {}
            if include_temporal and items_with_numerator_denominator:
                print(f"\n📅 ========== ANÁLISIS TEMPORAL ==========")
                
                temporal_data = self._execute_temporal_analysis(
                    service, data_source, matches, departamento, municipio, ips
                )
                
                vaccination_states_data = self._execute_vaccination_states_analysis(
                    data_source, matches, departamento, municipio, ips
                )
                
                combined_temporal_data.update(temporal_data)
                combined_temporal_data.update(vaccination_states_data)
                
                print(f"📊 Datos temporales combinados: {len(combined_temporal_data)} entradas")
            
            # ✅ PASO 4: CALCULAR TOTALES Y ESTADÍSTICAS GLOBALES
            totals_by_keyword = self._calculate_totals_with_numerator_denominator(items_with_numerator_denominator)
            global_statistics = self._calculate_global_statistics(items_with_numerator_denominator)
            
            return self._build_success_report_with_numerator_denominator(
                filename, keywords, geographic_filters, 
                items_with_numerator_denominator, totals_by_keyword, 
                combined_temporal_data, data_source, global_statistics, corte_fecha
            )
            
        except Exception as e:
            print(f"❌ Error generando reporte numerador/denominador: {e}")
            import traceback
            traceback.print_exc()
            raise Exception(f"Error en generación de reporte: {e}")
    
    def _execute_numerator_denominator_analysis(
        self, data_source: str, matches: List[Dict], 
        departamento: Optional[str], municipio: Optional[str], ips: Optional[str],
        min_count: int, corte_fecha: str
    ) -> List[Dict[str, Any]]:
        """
        ✅ COMPLETAMENTE CORREGIDO: Calcula numerador y denominador por rango de edad específico
        """
        items_with_numerator_denominator = []
        
        # ✅ DETECTAR CAMPO DE DOCUMENTO CORRECTO
        try:
            document_field = self._get_document_field(data_source)
        except Exception as e:
            print(f"❌ Error detectando campo documento: {e}")
            return []
        
        # ✅ DETECTAR CAMPOS DE EDAD CORRECTOS  
        try:
            edad_meses_field = self._get_age_months_field_corrected(data_source, corte_fecha)
            edad_años_field = self._get_age_years_field_corrected(data_source, corte_fecha)
        except Exception as e:
            print(f"❌ Error detectando campos de edad: {e}")
            return []
        
        # ✅ CONSTRUIR FILTROS GEOGRÁFICOS CORREGIDOS
        geo_conditions = []
        if departamento and departamento != 'Todos': 
            geo_conditions.append(f'"Departamento" = \'{departamento}\'')
        if municipio and municipio != 'Todos': 
            geo_conditions.append(f'"Municipio" = \'{municipio}\'')  
        if ips and ips != 'Todos': 
            geo_conditions.append(f'"Nombre IPS" = \'{ips}\'')
        geo_filter = " AND ".join(geo_conditions) if geo_conditions else "1=1"
        
        print(f"\n🔧 ========== ANÁLISIS NUMERADOR/DENOMINADOR CORREGIDO ==========")
        print(f"📊 Procesando {len(matches)} columnas coincidentes...")
        print(f"🔗 Campo documento: {document_field}")
        print(f"📅 Campo edad meses: {edad_meses_field}")
        print(f"🗓️ Campo edad años: {edad_años_field}")
        print(f"🗺️ Filtros geográficos: {geo_filter}")
        
        for match in matches:
            try:
                column_name = match['column']
                keyword = match['keyword'] 
                original_age_range = match['age_range']
                
                print(f"\n🔄 PROCESANDO: {column_name}")
                print(f"   🏷️ Keyword: {keyword}")
                print(f"   📅 Age range original: {original_age_range}")
                
                # ✅ EXTRAER RANGO DE EDAD ESPECÍFICO
                age_range_obj = self.age_extractor.extract_age_range(column_name)
                
                if not age_range_obj:
                    print(f"   ⚠️ No se pudo extraer rango de edad específico del nombre")
                    # Usar análisis tradicional como fallback
                    traditional_count = self._get_traditional_count(data_source, column_name, geo_filter)
                    if traditional_count >= min_count:
                        items_with_numerator_denominator.append({
                            "column": column_name,
                            "keyword": keyword,
                            "age_range": original_age_range,
                            "count": traditional_count,
                            "numerador": traditional_count,
                            "denominador": traditional_count,
                            "cobertura_porcentaje": 100.0,
                            "sin_datos": 0,
                            "metodo": "FALLBACK_TRADICIONAL"
                        })
                    continue
                
                print(f"   ✅ RANGO EXTRAÍDO: {age_range_obj.get_description()}")
                print(f"   📏 Min: {age_range_obj.min_age}, Max: {age_range_obj.max_age}, Unit: {age_range_obj.unit}")
                
                # ✅ GENERAR FILTRO SQL USANDO EL MÉTODO DEL AGERANGE
                specific_age_filter = age_range_obj.get_age_filter_sql(edad_meses_field, edad_años_field)
                age_description = age_range_obj.get_description()
                
                print(f"   🔍 FILTRO SQL GENERADO: {specific_age_filter}")
                
                # ✅ CALCULAR DENOMINADOR CON CAMPOS CORRECTOS
                denominator_sql = f"""
                SELECT COUNT(DISTINCT {document_field}) as total_poblacion_rango
                FROM {data_source}
                WHERE 
                    ({specific_age_filter})
                    AND "Fecha Nacimiento" IS NOT NULL 
                    AND TRIM("Fecha Nacimiento") != ''
                    AND LENGTH(TRIM("Fecha Nacimiento")) >= 8
                    AND TRY_CAST(strptime("Fecha Nacimiento", '%d/%m/%Y') AS DATE) IS NOT NULL
                    AND strptime("Fecha Nacimiento", '%d/%m/%Y') <= DATE '{corte_fecha}'
                    AND {document_field} IS NOT NULL
                    AND TRIM({document_field}) != ''
                    AND {geo_filter}
                """
                
                print(f"   🔍 QUERY DENOMINADOR (primeros 150 chars): {denominator_sql[:150]}...")
                
                try:
                    denominator_result = duckdb_service.conn.execute(denominator_sql).fetchone()
                    total_denominator = int(denominator_result[0]) if denominator_result and denominator_result[0] else 0
                except Exception as e:
                    print(f"   ❌ Error ejecutando denominador: {e}")
                    continue
                
                print(f"   📊 DENOMINADOR: {total_denominator:,} personas en {age_description}")
                
                if total_denominator == 0:
                    print(f"   ⚠️ DENOMINADOR = 0 - Sin población en este rango")
                    continue
                
                # ✅ CALCULAR NUMERADOR CON CAMPOS CORRECTOS
                escaped_column = duckdb_service.escape_identifier(column_name)
                
                numerator_sql = f"""
                SELECT COUNT(DISTINCT {document_field}) as total_con_datos
                FROM {data_source}
                WHERE 
                    ({specific_age_filter})
                    AND {escaped_column} IS NOT NULL 
                    AND TRIM(CAST({escaped_column} AS VARCHAR)) != ''
                    AND TRIM(CAST({escaped_column} AS VARCHAR)) NOT IN ('NULL', 'null', 'None', 'none', 'NaN', 'nan', 'N/A', 'n/a', '-')
                    AND "Fecha Nacimiento" IS NOT NULL 
                    AND TRIM("Fecha Nacimiento") != ''
                    AND LENGTH(TRIM("Fecha Nacimiento")) >= 8
                    AND TRY_CAST(strptime("Fecha Nacimiento", '%d/%m/%Y') AS DATE) IS NOT NULL
                    AND strptime("Fecha Nacimiento", '%d/%m/%Y') <= DATE '{corte_fecha}'
                    AND {document_field} IS NOT NULL
                    AND TRIM({document_field}) != ''
                    AND {geo_filter}
                """
                
                print(f"   🔍 QUERY NUMERADOR (primeros 150 chars): {numerator_sql[:150]}...")
                
                try:
                    numerator_result = duckdb_service.conn.execute(numerator_sql).fetchone()
                    total_numerator = int(numerator_result[0]) if numerator_result and numerator_result[0] else 0
                except Exception as e:
                    print(f"   ❌ Error ejecutando numerador: {e}")
                    continue
                
                print(f"   ✅ NUMERADOR: {total_numerator:,} con datos")
                
                # ✅ VALIDAR CONSISTENCIA
                if total_numerator > total_denominator:
                    print(f"   🔧 Corrigiendo: numerador > denominador")
                    total_numerator = total_denominator
                
                # ✅ CALCULAR MÉTRICAS
                cobertura_porcentaje = (total_numerator / total_denominator) * 100 if total_denominator > 0 else 0.0
                sin_datos = total_denominator - total_numerator
                
                # ✅ APLICAR FILTRO min_count
                if total_numerator >= min_count:
                    items_with_numerator_denominator.append({
                        "column": column_name,
                        "keyword": keyword,
                        "age_range": age_description,
                        "count": total_numerator,
                        
                        # CAMPOS NUMERADOR/DENOMINADOR
                        "numerador": total_numerator,
                        "denominador": total_denominator,
                        "cobertura_porcentaje": round(cobertura_porcentaje, 2),
                        "sin_datos": sin_datos,
                        "metodo": f"NUMERADOR_DENOMINADOR_RANGO_{age_range_obj.unit.upper()}",
                        
                        # Campos adicionales
                        "age_range_extracted": {
                            "min_age": age_range_obj.min_age,
                            "max_age": age_range_obj.max_age,
                            "unit": age_range_obj.unit,
                            "sql_filter": specific_age_filter
                        },
                        "corte_fecha": corte_fecha
                    })
                    
                    # ✅ LOG DETALLADO DE RESULTADO
                    print(f"   ✅ RESULTADO FINAL:")
                    print(f"      📊 DENOMINADOR: {total_denominator:,} (población {age_description})")
                    print(f"      ✅ NUMERADOR: {total_numerator:,} (con datos)")
                    print(f"      ❌ SIN DATOS: {sin_datos:,} (sin datos)")
                    print(f"      📈 COBERTURA: {cobertura_porcentaje:.1f}%")
                    
                    # ✅ DEBUGGING: Verificar si el rango incluye múltiples edades
                    if age_range_obj.min_age != age_range_obj.max_age:
                        print(f"   🔍 VERIFICACIÓN RANGO MÚLTIPLE:")
                        self._debug_age_range_coverage(
                            data_source, age_range_obj, edad_meses_field, edad_años_field, 
                            geo_filter, corte_fecha, document_field
                        )
                else:
                    print(f"   ⚠️ FILTRADO: numerador ({total_numerator}) < min_count ({min_count})")
                
            except Exception as e:
                print(f"   ❌ ERROR procesando {match.get('column', 'columna desconocida')}: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        print(f"\n✅ ========== ANÁLISIS COMPLETADO ==========")
        print(f"📊 Items procesados exitosamente: {len(items_with_numerator_denominator)}")
        
        return items_with_numerator_denominator
    
    def _get_document_field(self, data_source: str) -> str:
        """
        Devuelve el nombre de la columna que identifica unívocamente a la persona.
        """
        describe_sql = f'DESCRIBE SELECT * FROM {data_source}'
        cols = [row[0] for row in duckdb_service.conn.execute(describe_sql).fetchall()]

        doc_candidates = [
            'Nro Identificación', 'Nro Identificacion',
            'Número Documento', 'Numero Documento',
            'Documento', 'documento', 'Identificación', 'Identificacion',
            'cedula', 'Cedula', 'ID', 'id'
        ]
        # 1️⃣ búsqueda exacta
        for field in doc_candidates:
            if field in cols:
                print(f'✅ Campo documento detectado: {field}')
                return f'"{field}"'

        # 2️⃣ búsqueda por similitud (contiene “doc”, “ident”, “ced”)
        for field in cols:
            if any(k in field.lower() for k in ['doc', 'ident', 'ced']):
                print(f'✅ Campo documento por similitud: {field}')
                return f'"{field}"'

        # 3️⃣ último recurso: primer campo de la tabla
        print(f'⚠️ Usando primer campo como ID: {cols[0]}')
        return f'"{cols[0]}"'
    
    def _get_age_months_field_corrected(self, data_source: str, corte_fecha: str) -> str:
        """
        ✅ CORREGIDO: Determina el campo correcto para edad en meses
        """
        try:
            describe_sql = f"DESCRIBE SELECT * FROM {data_source}"
            columns_result = duckdb_service.conn.execute(describe_sql).fetchall()
            column_names = [row[0] for row in columns_result]
            
            print(f"   🔍 Buscando campo edad meses en {len(column_names)} columnas...")
            
            # Buscar columnas existentes de edad en meses
            edad_candidates = ['edad_meses', 'meses_edad', 'edad_en_meses', 'EdadMeses', 'Edad_Meses',
                             'edad_meses_calculada', 'meses', 'Meses']
            
            for candidate in edad_candidates:
                if candidate in column_names:
                    print(f"   ✅ Campo edad meses detectado: {candidate}")
                    return f'"{candidate}"'
            
            # ✅ BUSCAR POR SIMILITUD PARCIAL
            for column in column_names:
                if 'meses' in column.lower() and 'edad' in column.lower():
                    print(f"   ✅ Campo edad meses por similitud: {column}")
                    return f'"{column}"'
            
            # Si no hay campo directo, verificar si se puede calcular desde fecha nacimiento
            fecha_candidates = ['Fecha Nacimiento', 'fecha_nacimiento', 'FechaNacimiento', 'nacimiento',
                              'fecha_nac', 'fec_nacimiento', 'birth_date', 'birthdate']
            
            fecha_field = None
            for candidate in fecha_candidates:
                if candidate in column_names:
                    fecha_field = candidate
                    break
            
            # Buscar por similitud
            if not fecha_field:
                for column in column_names:
                    if any(keyword in column.lower() for keyword in ['nacimiento', 'fecha', 'birth', 'nac']):
                        if 'fecha' in column.lower() or 'birth' in column.lower():
                            fecha_field = column
                            break
            
            if fecha_field:
                calc_field = f"date_diff('month', strptime(\"{fecha_field}\", '%d/%m/%Y'), DATE '{corte_fecha}')"
                print(f"   ✅ Calculando edad meses desde: {fecha_field}")
                return calc_field
            
            raise Exception("No se encontró campo de edad en meses ni fecha de nacimiento")
            
        except Exception as e:
            print(f"   ❌ Error detectando campo edad meses: {e}")
            raise
    
    def _get_age_years_field_corrected(self, data_source: str, corte_fecha: str) -> str:
        """
        ✅ CORREGIDO: Determina el campo correcto para edad en años
        """
        try:
            describe_sql = f"DESCRIBE SELECT * FROM {data_source}"
            columns_result = duckdb_service.conn.execute(describe_sql).fetchall()
            column_names = [row[0] for row in columns_result]
            
            print(f"   🔍 Buscando campo edad años en {len(column_names)} columnas...")
            
            # Buscar columnas existentes de edad en años
            edad_candidates = ['edad_años', 'años_edad', 'edad_en_años', 'EdadAños', 'Edad_Años', 
                             'edad', 'Edad', 'age', 'Age', 'años', 'Anos', 'years']
            
            for candidate in edad_candidates:
                if candidate in column_names:
                    # Verificar si necesita conversión
                    if candidate.lower() in ['edad', 'age']:
                        print(f"   ✅ Campo edad años detectado (con conversión): {candidate}")
                        return f'TRY_CAST("{candidate}" AS INTEGER)'
                    else:
                        print(f"   ✅ Campo edad años detectado: {candidate}")
                        return f'"{candidate}"'
            
            # ✅ BUSCAR POR SIMILITUD PARCIAL
            for column in column_names:
                if any(keyword in column.lower() for keyword in ['edad', 'age', 'años', 'year']):
                    if not any(exclude in column.lower() for exclude in ['meses', 'month', 'dias', 'day']):
                        print(f"   ✅ Campo edad años por similitud: {column}")
                        return f'TRY_CAST("{column}" AS INTEGER)'
            
            # Calcular desde meses
            try:
                edad_meses_field = self._get_age_months_field_corrected(data_source, corte_fecha)
                calc_field = f"FLOOR(({edad_meses_field}) / 12)"
                print(f"   ✅ Calculando edad años desde meses")
                return calc_field
            except:
                pass
            
            # Fallback final
            print(f"   ⚠️ Usando fallback para edad años")
            return 'TRY_CAST("edad" AS INTEGER)'
            
        except Exception as e:
            print(f"   ❌ Error detectando campo edad años: {e}")
            return 'TRY_CAST("edad" AS INTEGER)'
    
    def _debug_age_range_coverage(
        self, data_source: str, age_range_obj, edad_meses_field: str, 
        edad_años_field: str, geo_filter: str, corte_fecha: str, document_field: str
    ):
        """
        ✅ CORREGIDO: Debug con campo documento correcto
        """
        try:
            if age_range_obj.unit == 'months' and age_range_obj.min_age != age_range_obj.max_age:
                debug_sql = f"""
                SELECT 
                    {edad_meses_field} as edad_meses,
                    COUNT(DISTINCT {document_field}) as poblacion
                FROM {data_source}
                WHERE 
                    {edad_meses_field} BETWEEN {age_range_obj.min_age} AND {age_range_obj.max_age}
                    AND "Fecha Nacimiento" IS NOT NULL 
                    AND TRY_CAST(strptime("Fecha Nacimiento", '%d/%m/%Y') AS DATE) IS NOT NULL
                    AND strptime("Fecha Nacimiento", '%d/%m/%Y') <= DATE '{corte_fecha}'
                    AND {document_field} IS NOT NULL
                    AND {geo_filter}
                GROUP BY {edad_meses_field}
                ORDER BY edad_meses
                """
                
                debug_result = duckdb_service.conn.execute(debug_sql).fetchall()
                print(f"      🔍 DESGLOSE POR MES:")
                total_verification = 0
                for row in debug_result:
                    edad_mes = row[0]
                    poblacion = row[1]
                    total_verification += poblacion
                    print(f"         {edad_mes} meses: {poblacion:,} personas")
                print(f"      ✅ TOTAL VERIFICACIÓN: {total_verification:,}")
                
        except Exception as e:
            print(f"      ❌ Error en debug: {e}")
    
    def _get_traditional_count(self, data_source: str, column_name: str, geo_filter: str) -> int:
        """Obtiene conteo tradicional como fallback"""
        try:
            escaped_column = duckdb_service.escape_identifier(column_name)
            count_sql = f"""
            SELECT COUNT(*) FROM {data_source}
            WHERE {escaped_column} IS NOT NULL 
            AND TRIM(CAST({escaped_column} AS VARCHAR)) != ''
            AND {geo_filter}
            """
            result = duckdb_service.conn.execute(count_sql).fetchone()
            return int(result[0]) if result[0] else 0
        except:
            return 0
    
    def _calculate_totals_with_numerator_denominator(self, items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Calcula totales por palabra clave CON numerador/denominador"""
        totals_by_keyword = {}
        
        for item in items:
            keyword = item["keyword"]
            
            if keyword not in totals_by_keyword:
                totals_by_keyword[keyword] = {
                    "count": 0,
                    "numerador": 0,
                    "denominador": 0,
                    "actividades": 0,
                    "cobertura_promedio": 0.0
                }
            
            totals_by_keyword[keyword]["count"] += item.get("count", 0)
            totals_by_keyword[keyword]["numerador"] += item.get("numerador", 0)
            totals_by_keyword[keyword]["denominador"] += item.get("denominador", 0)
            totals_by_keyword[keyword]["actividades"] += 1
        
        # Calcular coberturas promedio
        for keyword, totals in totals_by_keyword.items():
            if totals["denominador"] > 0:
                totals["cobertura_promedio"] = round((totals["numerador"] / totals["denominador"]) * 100, 2)
        
        return totals_by_keyword
    
    def _calculate_global_statistics(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calcula estadísticas globales del reporte"""
        total_denominador_global = sum(item.get("denominador", 0) for item in items)
        total_numerador_global = sum(item.get("numerador", 0) for item in items)
        total_sin_datos_global = sum(item.get("sin_datos", 0) for item in items)
        
        cobertura_global = (total_numerador_global / total_denominador_global * 100) if total_denominador_global > 0 else 0.0
        
        # Estadísticas de cobertura
        coverages = [item.get("cobertura_porcentaje", 0.0) for item in items]
        actividades_100_pct = len([c for c in coverages if c >= 100.0])
        actividades_menos_50_pct = len([c for c in coverages if c < 50.0])
        
        return {
            "total_actividades": len(items),
            "total_denominador_global": total_denominador_global,
            "total_numerador_global": total_numerador_global,
            "total_sin_datos_global": total_sin_datos_global,
            "cobertura_global_porcentaje": round(cobertura_global, 2),
            "actividades_100_pct_cobertura": actividades_100_pct,
            "actividades_menos_50_pct_cobertura": actividades_menos_50_pct,
            "mejor_cobertura": max(coverages) if coverages else 0.0,
            "peor_cobertura": min(coverages) if coverages else 0.0,
            "cobertura_promedio": round(sum(coverages) / len(coverages), 2) if coverages else 0.0
        }
    
    # ✅ MÉTODOS EXISTENTES (sin cambios)
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
    
    def _build_empty_report(
        self, filename: str, keywords: Optional[List[str]], 
        geographic_filters: Dict[str, Optional[str]]
    ) -> Dict[str, Any]:
        """Construye reporte vacío cuando no hay matches"""
        return {
            "success": True,
            "filename": filename,
            "rules": {"keywords": keywords or []},
            "geographic_filters": {**geographic_filters, "filter_type": "numerador_denominador"},
            "items": [],
            "totals_by_keyword": {},
            "temporal_data": {},
            "global_statistics": {
                "total_denominador_global": 0,
                "total_numerador_global": 0,
                "cobertura_global_porcentaje": 0.0
            },
            "message": "No se encontraron columnas con las palabras clave especificadas",
            "metodo": "NUMERADOR_DENOMINADOR_VACIO"
        }
    
    def _build_success_report_with_numerator_denominator(
        self, filename: str, keywords: Optional[List[str]], 
        geographic_filters: Dict[str, Optional[str]], 
        items: List[Dict[str, Any]], 
        totals_by_keyword: Dict[str, Any], 
        temporal_data: Dict[str, Any], 
        data_source: str,
        global_statistics: Dict[str, Any],
        corte_fecha: str
    ) -> Dict[str, Any]:
        """Construye reporte exitoso CON numerador/denominador"""
        return {
            "success": True,
            "filename": filename,
            "corte_fecha": corte_fecha,
            "rules": {"keywords": keywords or []},
            "geographic_filters": {**geographic_filters, "filter_type": "numerador_denominador"},
            "items": items,
            "totals_by_keyword": totals_by_keyword,
            "temporal_data": temporal_data,
            "global_statistics": global_statistics,
            "ultra_fast": True,
            "engine": "DuckDB_NumeradorDenominador_Keywords_v1",
            "data_source_used": data_source,
            "metodo": "NUMERADOR_DENOMINADOR_POR_RANGO_ESPECIFICO_KEYWORDS",
            "version": "1.0.0",
            "caracteristicas": [
                "extraccion_automatica_rango_edad",
                "numerador_denominador_por_columna", 
                "calculo_cobertura_especifica",
                "compatibilidad_keywords_tradicional",
                "analisis_temporal_integrado",
                "verificacion_rangos_multiples",
                "deteccion_automatica_campos"
            ]
        }
    
    # ✅ MÉTODOS TEMPORALES EXISTENTES (sin cambios)
    def _execute_temporal_analysis(self, service, data_source: str, matches, departamento, municipio, ips) -> Dict[str, Any]:
        """Ejecuta análisis temporal"""
        temporal_data = {}
        try:
            temporal_sql = service.build_temporal_report_sql_with_filters(
                data_source, matches, duckdb_service.escape_identifier,
                departamento, municipio, ips
            )
            temporal_result = duckdb_service.conn.execute(temporal_sql).fetchall()
            temporal_data = self._process_temporal_results(temporal_result)
            print(f"📊 Análisis temporal: {len(temporal_data)} columnas")
        except Exception as e:
            print(f"⚠️ Error temporal (continuando): {e}")
        return temporal_data
    
    def _process_temporal_results(self, temporal_result) -> Dict[str, Any]:
        """Procesa resultados temporales"""
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
        return (year and year > 1900 and year < 2100 and month and 1 <= month <= 12)
    
    def _execute_vaccination_states_analysis(self, data_source: str, matches, departamento, municipio, ips) -> Dict[str, Any]:
        """Ejecuta análisis de estados de vacunación"""
        states_data = {}
        try:
            vaccination_columns = []
            for match in matches:
                column_name = match['column']
                if any(keyword in column_name.lower() for keyword in ['vacunación', 'vacunacion']):
                    if self._column_has_states(data_source, column_name):
                        vaccination_columns.append(match)
            
            if vaccination_columns:
                states_sql = self._build_vaccination_states_sql_simple(
                    data_source, vaccination_columns, 
                    duckdb_service.escape_identifier,
                    departamento, municipio, ips
                )
                states_result = duckdb_service.conn.execute(states_sql).fetchall()
                states_data = self._process_vaccination_states_results(states_result)
                print(f"💉 Estados de vacunación: {len(states_data)} entradas")
                
        except Exception as e:
            print(f"❌ Error análisis estados: {e}")
        
        return states_data
    
    def _column_has_states(self, data_source: str, column_name: str) -> bool:
        """Verifica si columna tiene estados"""
        try:
            escaped_column = duckdb_service.escape_identifier(column_name)
            check_sql = f"""
            SELECT DISTINCT {escaped_column} FROM {data_source} 
            WHERE {escaped_column} IS NOT NULL 
            AND TRIM(LOWER({escaped_column})) IN ('completo', 'incompleto', 'complete', 'incomplete')
            LIMIT 1
            """
            result = duckdb_service.conn.execute(check_sql).fetchall()
            return len(result) > 0
        except:
            return False
    
    def _build_vaccination_states_sql_simple(self, data_source: str, vaccination_columns, escape_func, departamento, municipio, ips) -> str:
        """Construye SQL para estados de vacunación"""
        queries = []
        for match in vaccination_columns:
            column_name = match['column']
            keyword = match['keyword']
            age_range = match['age_range']
            escaped_column = escape_func(column_name)
            
            where_conditions = []
            if departamento: where_conditions.append(f"{escape_func('departamento')} = '{departamento}'")
            if municipio: where_conditions.append(f"{escape_func('municipio')} = '{municipio}'")
            if ips: where_conditions.append(f"{escape_func('ips')} = '{ips}'")
            base_where = " AND ".join(where_conditions) if where_conditions else "1=1"
            
            for estado in ['Completo', 'Incompleto']:
                state_condition = "completo" if estado == "Completo" else "incompleto"
                query = f"""
                SELECT '{column_name}', '{keyword}', '{age_range}', '{estado}', COUNT(*)
                FROM {data_source}
                WHERE {base_where} AND {escaped_column} IS NOT NULL
                AND (TRIM(LOWER({escaped_column})) = '{state_condition}' 
                     OR TRIM(LOWER({escaped_column})) = '{state_condition.replace("completo", "complete").replace("incompleto", "incomplete")}')
                """
                queries.append(query)
        
        return " UNION ALL ".join(queries) + " ORDER BY 1, 4" if queries else "SELECT 'no_data', 'no_data', 'no_data', 'no_data', 0 WHERE 1=0"
    
    def _process_vaccination_states_results(self, states_result) -> Dict[str, Any]:
        """Procesa resultados de estados"""
        states_data = {}
        for row in states_result:
            column_name = str(row[0]) if row[0] is not None else ""
            keyword = str(row[1]) if row[1] is not None else ""
            age_range = str(row[2]) if row[2] is not None else ""
            estado = str(row[3]) if row[3] is not None else ""
            count = int(row[4]) if row[4] is not None else 0
            
            if estado in ['Completo', 'Incompleto'] and count > 0:
                column_key = f"{column_name}|{keyword}|{age_range}"
                if column_key not in states_data:
                    states_data[column_key] = {
                        "column": column_name,
                        "keyword": keyword,
                        "age_range": age_range,
                        "type": "states",
                        "states": {}
                    }
                states_data[column_key]["states"][estado] = {"state": estado, "count": count}
        
        return states_data
