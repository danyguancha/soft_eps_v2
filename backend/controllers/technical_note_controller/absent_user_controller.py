from typing import Any, Dict, List, Optional
import unicodedata
import re

from services.duckdb_service.duckdb_service import duckdb_service
from services.technical_note_services.data_source_service import DataSourceService

class AbsentUserController:

    def _normalize_keyword(self, keyword: str) -> str:
        """Normaliza palabra clave removiendo tildes y convirtiendo a minúsculas"""
        normalized = ''.join(c for c in unicodedata.normalize('NFD', keyword)
                            if unicodedata.category(c) != 'Mn').lower().strip()
        
        if normalized.endswith('ica'):
            normalized = normalized[:-3] + 'ia'
        
        return normalized

    def _discover_activity_columns(self, data_source: str, keywords: List[str]) -> Dict[str, List[str]]:
        """Descubre dinámicamente columnas de actividades basadas en palabras clave"""
        try:
            # ✅ EXTRAER RUTA LIMPIA DEL DATA_SOURCE
            if data_source.startswith("read_parquet('") and data_source.endswith("')"):
                # Extraer ruta de read_parquet('ruta')
                clean_path = data_source[14:-2]  # Quitar read_parquet(' y ')
            elif data_source.startswith("'") and data_source.endswith("'"):
                # Ya está limpia con comillas
                clean_path = data_source[1:-1]  # Quitar comillas
            else:
                # Asumir que es una ruta directa
                clean_path = data_source
            
            # ✅ SINTAXIS OFICIAL DE DUCKDB PARA DESCRIBIR ARCHIVOS
            columns_sql = f"DESCRIBE SELECT * FROM '{clean_path}'"
            
            print(f"🔧 SQL generado: {columns_sql}")
            
            columns_result = duckdb_service.conn.execute(columns_sql).fetchall()
            
            # ✅ EXTRAER NOMBRES DE COLUMNAS - row[0] contiene el nombre para DESCRIBE
            available_columns = [row[0] for row in columns_result]  
            
            print(f"🔍 DESCUBRIENDO COLUMNAS DINÁMICAMENTE ({len(available_columns)} total)")
            print(f"📋 Primeras 10 columnas: {available_columns[:10]}")
            
            discovered_mapping = {}
            
            for keyword in keywords:
                normalized_keyword = self._normalize_keyword(keyword)
                print(f"🔎 Buscando columnas para: '{keyword}' (normalizado: '{normalized_keyword}')")
                
                # Encontrar columnas relacionadas con la palabra clave
                related_columns = []
                
                for col in available_columns:
                    col_normalized = self._normalize_keyword(col)
                    
                    # Verificar si la columna contiene la palabra clave
                    if normalized_keyword in col_normalized:
                        # Filtrar solo columnas que parecen ser de actividades/consultas
                        if self._is_activity_column(col):
                            related_columns.append(f'"{col}"')
                            print(f"  ✅ Encontrada: {col}")
                
                if related_columns:
                    discovered_mapping[normalized_keyword] = related_columns
                    print(f"📋 {keyword}: {len(related_columns)} columnas encontradas")
                else:
                    print(f"⚠️ No se encontraron columnas para: {keyword}")
            
            return discovered_mapping
            
        except Exception as e:
            print(f"❌ Error descubriendo columnas: {e}")
            import traceback
            traceback.print_exc()
            return {}




    def _is_activity_column(self, column_name: str) -> bool:
        """Determina si una columna representa una actividad/consulta - VERSIÓN MEJORADA"""
        col_lower = column_name.lower()
        
        # Patrones que indican columnas de actividades
        activity_patterns = [
            r'consulta.*\d+.*mes',           # "Consulta por X 1 mes"
            r'consulta.*\d+.*año',           # "Consulta por X 2 años"
            r'fecha.*consulta',              # "Fecha más reciente de Consulta"
            r'control.*\d+',                 # "Control de X 12 meses"
            r'valoracion.*\d+',              # "Valoración X 6 meses"
            r'seguimiento.*\d+',             # "Seguimiento X 18 meses"
            r'tamizaje.*\d+',                # "Tamizaje X 24 meses"
            
            # ✅ PATRONES MEJORADOS PARA VACUNACIÓN
            r'esquema.*vacun',               # "Esquema de vacunación"
            r'vacunacion.*regular',          # "Vacunación Regular"
            r'vacunacion.*covid',            # "Vacunación COVID"
            r'vacunacion.*completo',         # "Vacunación Completo"
            r'fecha.*vacun',                 # "Fecha más reciente de Vacunación"
            r'dosis.*vacun',                 # "Dosis de Vacunación"
            r'aplicacion.*vacuna',           # "Aplicación de vacuna"
            r'esquema.*inmun',               # "Esquema de inmunización"
            
            # ✅ PATRONES GENÉRICOS PARA OTRAS ACTIVIDADES
            r'consulta(?!.*estado)',        # Cualquier consulta que no sea estado
            r'fecha.*reciente.*consulta',    # Fechas de consulta reciente
            r'atencion.*\d+',                # "Atención X meses"
            r'seguimiento(?!.*resultado)',   # Seguimientos que no sean resultados
        ]
        
        # Excluir patrones que no son actividades
        exclude_patterns = [
            r'estado(?!.*esquema)',          # Estados (excepto "Estado de esquema")
            r'diagnostico(?!\s+\w+\s+\d)',  # Diagnósticos (excepto con números)
            r'resultado(?!.*consulta)',      # Resultados (excepto de consulta)
            r'clasificacion(?!.*actividad)', # Clasificaciones
            r'tipo(?!.*consulta)',           # Tipos (excepto tipo de consulta)
            r'codigo(?!.*actividad)',        # Códigos
        ]
        
        # Verificar patrones de actividades
        for pattern in activity_patterns:
            if re.search(pattern, col_lower):
                # Verificar que no sea un patrón excluido
                is_excluded = any(re.search(exclude_pattern, col_lower) for exclude_pattern in exclude_patterns)
                if not is_excluded:
                    return True
        
        return False


    def _generate_activity_reports(
        self,
        data_source: str,
        activity_columns: List[str],
        age_filter: str,
        geo_filter: str,
        corte_fecha: str
    ) -> List[Dict[str, Any]]:
        """Genera reportes individuales por cada actividad"""
        activity_reports = []
        
        # ✅ EXTRAER RUTA LIMPIA PARA LAS QUERIES
        if data_source.startswith("read_parquet('") and data_source.endswith("')"):
            clean_path = data_source[14:-2]
            table_reference = f"'{clean_path}'"
        elif data_source.startswith("'") and data_source.endswith("'"):
            table_reference = data_source
        else:
            table_reference = f"'{data_source}'"
        
        for column in activity_columns:
            try:
                # Query para esta actividad específica
                activity_sql = f"""
                SELECT 
                    "Departamento" as departamento,
                    "Municipio" as municipio,
                    "Nombre IPS" as nombre_ips,
                    "Nro Identificación" as nro_identificacion,
                    "Primer Apellido" as primer_apellido,
                    "Segundo Apellido" as segundo_apellido,
                    "Primer Nombre" as primer_nombre,
                    "Segundo Nombre" as segundo_nombre,
                    "Fecha Nacimiento" as fecha_nacimiento,
                    TRY_CAST(edad AS INTEGER) as edad_anos,
                    date_diff('month', strptime("Fecha Nacimiento", '%d/%m/%Y'), DATE '{corte_fecha}') as edad_meses,
                    {column} as actividad_valor
                FROM {table_reference}
                WHERE 
                    -- Filtro de inasistencia para esta actividad
                    ({column} IS NULL OR TRIM({column}) = '')
                    
                    -- Filtros de edad
                    AND ({age_filter})
                    
                    -- Fecha de nacimiento válida
                    AND "Fecha Nacimiento" IS NOT NULL 
                    AND TRIM("Fecha Nacimiento") != ''
                    AND TRY_CAST(strptime("Fecha Nacimiento", '%d/%m/%Y') AS DATE) IS NOT NULL
                    
                    -- Filtros geográficos
                    AND {geo_filter}
                    
                ORDER BY "Departamento", "Municipio", "Nombre IPS", "Primer Apellido", "Primer Nombre"
                """
                
                activity_result = duckdb_service.conn.execute(activity_sql).fetchall()
                
                # Estadísticas para esta actividad
                stats_sql = f"""
                SELECT 
                    COUNT(*) as total_inasistentes,
                    COUNT(DISTINCT "Departamento") as departamentos_afectados,
                    COUNT(DISTINCT "Municipio") as municipios_afectados,
                    COUNT(DISTINCT "Nombre IPS") as ips_afectadas
                FROM {table_reference}
                WHERE 
                    ({column} IS NULL OR TRIM({column}) = '')
                    AND ({age_filter})
                    AND "Fecha Nacimiento" IS NOT NULL 
                    AND TRIM("Fecha Nacimiento") != ''
                    AND TRY_CAST(strptime("Fecha Nacimiento", '%d/%m/%Y') AS DATE) IS NOT NULL
                    AND {geo_filter}
                """
                
                stats_result = duckdb_service.conn.execute(stats_sql).fetchone()
                
                # Procesar resultados
                inasistentes_data = []
                for row in activity_result:
                    inasistentes_data.append({
                        "departamento": str(row[0]) if row[0] else "",
                        "municipio": str(row[1]) if row[1] else "",
                        "nombre_ips": str(row[2]) if row[2] else "",
                        "nro_identificacion": str(row[3]) if row[3] else "",
                        "primer_apellido": str(row[4]) if row[4] else "",
                        "segundo_apellido": str(row[5]) if row[5] else "",
                        "primer_nombre": str(row[6]) if row[6] else "",
                        "segundo_nombre": str(row[7]) if row[7] else "",
                        "fecha_nacimiento": str(row[8]) if row[8] else "",
                        "edad_anos": int(row[9]) if row[9] is not None else None,
                        "edad_meses": int(row[10]) if row[10] is not None else None,
                        "actividad_valor": str(row[11]) if row[11] else "VACÍO",
                        "columna_evaluada": column.replace('"', '')
                    })
                
                activity_reports.append({
                    "actividad": column.replace('"', ''),
                    "inasistentes": inasistentes_data,
                    "statistics": {
                        "total_inasistentes": int(stats_result[0]) if stats_result[0] else 0,
                        "departamentos_afectados": int(stats_result[1]) if stats_result[1] else 0,
                        "municipios_afectados": int(stats_result[2]) if stats_result[2] else 0,
                        "ips_afectadas": int(stats_result[3]) if stats_result[3] else 0
                    }
                })
                
                print(f"✅ Actividad procesada: {column.replace('\"', '')} - {len(inasistentes_data)} inasistentes")
                
            except Exception as e:
                print(f"❌ Error procesando actividad {column}: {e}")
                continue
        
        return activity_reports




    def get_inasistentes_report(
        self,
        filename: str,
        selected_months: List[int],
        selected_years: List[int] = None,
        selected_keywords: List[str] = None,  
        corte_fecha: str = "2025-07-31",
        departamento: Optional[str] = None,
        municipio: Optional[str] = None,
        ips: Optional[str] = None,
        path_technical_note = ''
    ) -> Dict[str, Any]:
        """Genera reporte de inasistentes dinámico con descubrimiento automático de actividades"""
        try:
            print(f"🏥 Generando reporte DINÁMICO de inasistentes para: {filename}")
            print(f"📅 Edades en meses seleccionadas: {selected_months}")
            print(f"🗓️ Edades en años seleccionadas: {selected_years or 'Ninguna'}")
            print(f"🔑 Palabras clave: {selected_keywords or 'Ninguna'}")
            print(f"🗺️ Filtros: Dept={departamento}, Mun={municipio}, IPS={ips}")

            file_key = f"technical_{filename.replace('.', '_').replace(' ', '_').replace('-', '_')}"
            data_source = DataSourceService(path_technical_note).ensure_data_source_available(filename, file_key)
            
            # ✅ DESCUBRIMIENTO DINÁMICO DE COLUMNAS
            if not selected_keywords:
                selected_keywords = ['medicina']  # Fallback por defecto
                
            discovered_columns = self._discover_activity_columns(data_source, selected_keywords)
            
            # Recopilar todas las columnas encontradas
            all_activity_columns = []
            for keyword, columns in discovered_columns.items():
                all_activity_columns.extend(columns)
            
            if not all_activity_columns:
                return {
                    "success": False,
                    "error": f"No se encontraron actividades para las palabras clave: {selected_keywords}",
                    "inasistentes_por_actividad": [],
                    "resumen_general": {}
                }
            
            print(f"🎯 TOTAL ACTIVIDADES ENCONTRADAS: {len(all_activity_columns)}")
            
            # ✅ CONSTRUIR FILTROS DE EDAD
            age_conditions = []
            
            if selected_months:
                months_list = ','.join(map(str, selected_months))
                age_conditions.append(f"""
                    date_diff('month', strptime("Fecha Nacimiento", '%d/%m/%Y'), DATE '{corte_fecha}') IN ({months_list})
                """)
            
            if selected_years:
                years_list = ','.join(map(str, selected_years))
                age_conditions.append(f"""
                    TRY_CAST(edad AS INTEGER) IN ({years_list})
                """)
            
            if not age_conditions:
                return {
                    "success": False,
                    "error": "Debe seleccionar al menos una edad en meses o años",
                    "inasistentes_por_actividad": []
                }
            
            age_filter = " OR ".join(age_conditions)
            
            # ✅ CONSTRUIR FILTROS GEOGRÁFICOS
            geo_conditions = []
            
            if departamento:
                geo_conditions.append(f'"Departamento" = \'{departamento}\'')
            
            if municipio:
                geo_conditions.append(f'"Municipio" = \'{municipio}\'')
                
            if ips:
                geo_conditions.append(f'"Nombre IPS" = \'{ips}\'')
            
            geo_filter = " AND ".join(geo_conditions) if geo_conditions else "1=1"
            
            # ✅ GENERAR REPORTES POR CADA ACTIVIDAD
            print(f"🔧 Generando reportes individuales por actividad...")
            activity_reports = self._generate_activity_reports(
                data_source, all_activity_columns, age_filter, geo_filter, corte_fecha
            )
            
            # ✅ CALCULAR RESUMEN GENERAL
            total_inasistentes = sum(report["statistics"]["total_inasistentes"] for report in activity_reports)
            
            # Obtener todos los departamentos, municipios, IPS únicos
            all_departamentos = set()
            all_municipios = set()
            all_ips = set()
            
            for report in activity_reports:
                for inasistente in report["inasistentes"]:
                    if inasistente["departamento"]:
                        all_departamentos.add(inasistente["departamento"])
                    if inasistente["municipio"]:
                        all_municipios.add(inasistente["municipio"])
                    if inasistente["nombre_ips"]:
                        all_ips.add(inasistente["nombre_ips"])
            
            resumen_general = {
                "total_actividades_evaluadas": len(all_activity_columns),
                "total_inasistentes_global": total_inasistentes,
                "departamentos_afectados": len(all_departamentos),
                "municipios_afectados": len(all_municipios),
                "ips_afectadas": len(all_ips),
                "actividades_con_inasistentes": len([r for r in activity_reports if r["statistics"]["total_inasistentes"] > 0]),
                "actividades_sin_inasistentes": len([r for r in activity_reports if r["statistics"]["total_inasistentes"] == 0])
            }
            
            print(f"✅ REPORTE DINÁMICO GENERADO:")
            print(f"   📊 {len(activity_reports)} actividades procesadas")
            print(f"   👥 {total_inasistentes} inasistentes totales")
            print(f"   🎯 {resumen_general['actividades_con_inasistentes']} actividades con inasistencias")
            
            return {
                "success": True,
                "filename": filename,
                "corte_fecha": corte_fecha,
                "metodo": "DESCUBRIMIENTO_DINAMICO",
                "filtros_aplicados": {
                    "selected_months": selected_months,
                    "selected_years": selected_years or [],
                    "selected_keywords": selected_keywords,
                    "departamento": departamento,
                    "municipio": municipio,
                    "ips": ips
                },
                "columnas_descubiertas": {
                    keyword: [col.replace('"', '') for col in columns] 
                    for keyword, columns in discovered_columns.items()
                },
                "inasistentes_por_actividad": activity_reports,
                "resumen_general": resumen_general,
                "engine": "DuckDB_Dynamic_Discovery"
            }
            
        except Exception as e:
            print(f"❌ Error generando reporte dinámico: {e}")
            import traceback
            traceback.print_exc()
            return {
                "success": False,
                "error": str(e),
                "inasistentes_por_actividad": []
            }

    # ✅ MÉTODO SIMPLIFICADO - Ya no es necesario mantener mapeos manuales
    def _get_keyword_column_mapping(self) -> Dict[str, List[str]]:
        """Mapeo legacy - ahora se usa descubrimiento dinámico"""
        return {
            'medicina': ['"Consulta por Medicina 1 mes"'],  # Solo como fallback
        }
