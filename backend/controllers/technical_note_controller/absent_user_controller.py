
import csv
from datetime import datetime
import io
from typing import Any, Dict, List, Optional
import unicodedata

from fastapi.responses import StreamingResponse
import pandas as pd

from services.duckdb_service.duckdb_service import duckdb_service
from services.technical_note_services.data_source_service import DataSourceService
from controllers.technical_note_controller.absent_user.reports_activity import ReportActivity
from controllers.technical_note_controller.absent_user.activity_column import ActivityColumn

class AbsentUserController:

    def _normalize_keyword(self, keyword: str) -> str:
        """Normaliza palabra clave removiendo tildes y convirtiendo a minúsculas"""
        normalized = ''.join(c for c in unicodedata.normalize('NFD', keyword)
                            if unicodedata.category(c) != 'Mn').lower().strip()
        
        if normalized.endswith('ica'):
            normalized = normalized[:-3] + 'ia'
        
        return normalized

    def _discover_activity_columns(self, data_source: str, keywords: List[str]) -> Dict[str, List[str]]:
        """Descubre dinámicamente columnas de actividades - VERSIÓN FINAL QUE NO FALLA"""
        try:
            # EXTRAER RUTA LIMPIA DEL DATA_SOURCE
            if data_source.startswith("read_parquet('") and data_source.endswith("')"):
                clean_path = data_source[14:-2]
            elif data_source.startswith("'") and data_source.endswith("'"):
                clean_path = data_source[1:-1]
            else:
                clean_path = data_source
            
            print(f"🔍 Intentando descubrir columnas en: {clean_path}")
            
            # VERIFICAR QUE EL ARCHIVO EXISTE
            import os
            if not os.path.exists(clean_path):
                print(f"❌ ARCHIVO NO EXISTE: {clean_path}")
                return self._get_fallback_mapping(keywords)
            
            file_size = os.path.getsize(clean_path)
            print(f"📁 Archivo encontrado: {file_size:,} bytes")
            
            available_columns = []
            
            # ENFOQUE 1: Usar parquet_schema con la columna correcta 'name'
            try:
                print("🔄 Intentando con parquet_schema...")
                # CORRECCIÓN: usar 'name' en lugar de 'column_name'
                schema_sql = f"SELECT name FROM parquet_schema('{clean_path}')"
                schema_result = duckdb_service.conn.execute(schema_sql).fetchall()
                available_columns = [row[0] for row in schema_result if row[0]]  # Filtrar nulls
                print(f"✅ parquet_schema exitoso: {len(available_columns)} columnas")
            except Exception as e:
                print(f"⚠️ parquet_schema falló: {e}")
            
            # ENFOQUE 2: Si parquet_schema falla, usar LIMIT 0 con sintaxis directa
            if not available_columns:
                try:
                    print("🔄 Intentando lectura directa con LIMIT 0...")
                    # Usar sintaxis directa del archivo, no read_parquet
                    limit_sql = f"SELECT * FROM '{clean_path}' LIMIT 0"
                    result = duckdb_service.conn.execute(limit_sql)
                    if result.description:
                        available_columns = [desc[0] for desc in result.description]
                        print(f"✅ LIMIT 0 directo exitoso: {len(available_columns)} columnas")
                except Exception as e:
                    print(f"⚠️ LIMIT 0 directo falló: {e}")
            
            # ENFOQUE 3: Si falla, probar con DESCRIBE usando sintaxis directa
            if not available_columns:
                try:
                    print("🔄 Intentando con DESCRIBE sintaxis directa...")
                    # Usar sintaxis directa, no read_parquet dentro de DESCRIBE
                    describe_sql = f"DESCRIBE SELECT * FROM '{clean_path}'"
                    describe_result = duckdb_service.conn.execute(describe_sql).fetchall()
                    available_columns = [row[0] for row in describe_result]
                    print(f"✅ DESCRIBE directo exitoso: {len(available_columns)} columnas")
                except Exception as e:
                    print(f"⚠️ DESCRIBE directo falló: {e}")
            
            # ENFOQUE 4: Como último recurso, usar pandas
            if not available_columns:
                try:
                    print("🔄 Último recurso: usando pandas...")
                    import pandas as pd
                    df_sample = pd.read_parquet(clean_path, nrows=1)
                    available_columns = df_sample.columns.tolist()
                    print(f"✅ Pandas exitoso: {len(available_columns)} columnas")
                except Exception as e:
                    print(f"⚠️ Pandas falló: {e}")
            
            # VALIDACIÓN FINAL - NUNCA RETORNAR VACÍO
            if not available_columns:
                print(f"❌ TODOS LOS MÉTODOS FALLARON - USANDO FALLBACK COMPLETO")
                return self._get_fallback_mapping(keywords)
            
            print(f"🔍 DESCUBRIMIENTO EXITOSO: {len(available_columns)} columnas")
            print(f"📋 Primeras 15 columnas encontradas: {available_columns[:15]}")
            
            # BUSCAR COLUMNAS MATCHING CON KEYWORDS
            discovered_mapping = {}
            
            for keyword in keywords:
                normalized_keyword = self._normalize_keyword(keyword)
                print(f"🔎 Buscando columnas para: '{keyword}' (normalizado: '{normalized_keyword}')")
                
                matching_columns = []
                activity_columns = []
                
                for col in available_columns:
                    col_normalized = self._normalize_keyword(col)
                    
                    # Verificar si la columna contiene la palabra clave
                    if normalized_keyword in col_normalized:
                        matching_columns.append(col)
                        
                        # Verificar si es una columna de actividad
                        try:
                            if ActivityColumn().is_activity_column(col):
                                activity_columns.append(f'"{col}"')
                                print(f"  ✅ Encontrada y válida: {col}")
                            else:
                                print(f"  ❓ Encontrada pero filtrada por ActivityColumn: {col}")
                        except Exception as e:
                            # Si ActivityColumn falla, incluir la columna de todos modos
                            activity_columns.append(f'"{col}"')
                            print(f"  ⚠️ Incluida (ActivityColumn falló): {col}")
                
                # LOGGING DETALLADO PARA DEBUGGING
                if matching_columns:
                    print(f"  📊 Total columnas que contienen '{normalized_keyword}': {len(matching_columns)}")
                    if not activity_columns:
                        print(f"  📝 Columnas encontradas pero filtradas:")
                        for i, col in enumerate(matching_columns[:5]):
                            print(f"    {i+1}. {col}")
                
                if activity_columns:
                    discovered_mapping[normalized_keyword] = activity_columns
                    print(f"📋 {keyword}: {len(activity_columns)} columnas de actividad asignadas")
                else:
                    print(f"⚠️ No se encontraron columnas válidas para: {keyword}")
            
            # SI NO SE ENCUENTRA NADA ÚTIL, USAR FALLBACK
            if not discovered_mapping:
                print(f"⚠️ Sin matches útiles - aplicando fallback robusto")
                return self._get_fallback_mapping(keywords)
            
            print(f"✅ Descubrimiento completado exitosamente")
            return discovered_mapping
            
        except Exception as e:
            print(f"❌ ERROR CRÍTICO en descubrimiento: {e}")
            import traceback
            traceback.print_exc()
            
            # FALLBACK GARANTIZADO - NUNCA FALLA
            print(f"🆘 Aplicando fallback de emergencia")
            return self._get_fallback_mapping(keywords)

    def _get_fallback_mapping(self, keywords: List[str]) -> Dict[str, List[str]]:
        """Mapeo de fallback robusto - NUNCA RETORNA VACÍO"""
        print(f"🔄 Aplicando mapeo de fallback para: {keywords}")
        
        # Mapeos completos y actualizados
        fallback_mappings = {
            'vacunacion': [
                '"Esquema de vacunación Regular"', 
                '"Esquema de Vacunación COVID"', 
                '"Fecha más reciente de Vacunación"',
                '"BCG (Tuberculosis)"',
                '"Hepatitis B recién nacido"',
                '"Pentavalente primera dosis (2 meses)"',
                '"Pentavalente segunda dosis (4 meses)"',
                '"Pentavalente tercera dosis (6 meses)"',
                '"Polio (VOP) primera dosis (2 meses)"',
                '"Neumococo primera dosis (2 meses)"',
                '"Rotavirus primera dosis (2 meses)"'
            ],
            'medicina': [
                '"Consulta por Medicina 1 mes"', 
                '"Consulta por Medicina General"',
                '"Consulta por Medicina 2 meses"',
                '"Consulta por Medicina 4 meses"',
                '"Consulta por Medicina 6 meses"',
                '"Consulta por Medicina 9 meses"',
                '"Consulta por Medicina 12 meses"',
                '"Consulta por Medicina 18 meses"'
            ],
            'nutricion': [
                '"Consulta por Nutrición"', 
                '"Seguimiento Nutricional"',
                '"Valoración del estado nutricional"',
                '"Consulta por Nutrición y Dietética"'
            ],
            'pediatria': [
                '"Consulta por Pediatría"', 
                '"Control de Crecimiento y Desarrollo"',
                '"Consulta especializada por Pediatría"'
            ],
            'enfermeria': [
                '"Consulta por Enfermería"',
                '"Procedimiento de enfermería"'
            ],
            'odontologia': [
                '"Consulta por Odontología"',
                '"Procedimiento de odontología preventiva"'
            ]
        }
        
        result_mapping = {}
        
        for keyword in keywords:
            normalized_keyword = self._normalize_keyword(keyword)
            found = False
            
            # Buscar coincidencias exactas o parciales
            for known_key, known_columns in fallback_mappings.items():
                # Múltiples criterios de matching
                conditions = [
                    normalized_keyword == known_key,
                    normalized_keyword in known_key,
                    known_key in normalized_keyword,
                    any(part in known_key for part in normalized_keyword.split()),
                    any(part in normalized_keyword for part in known_key.split())
                ]
                
                if any(conditions):
                    result_mapping[normalized_keyword] = known_columns
                    print(f"📋 Fallback para '{keyword}': {len(known_columns)} columnas ({known_key})")
                    found = True
                    break
            
            # Si no se encuentra match, usar vacunación como default más completo
            if not found:
                result_mapping[normalized_keyword] = fallback_mappings['vacunacion']
                print(f"📋 Fallback genérico para '{keyword}': vacunacion ({len(fallback_mappings['vacunacion'])} columnas)")
        
        print(f"✅ Fallback completado - {len(result_mapping)} keywords con mapeos")
        return result_mapping

    def _test_parquet_file_health(self, clean_path: str) -> Dict[str, Any]:
        """Prueba la salud del archivo Parquet con múltiples métodos"""
        health_report = {
            "file_exists": False,
            "file_size": 0,
            "readable_by_duckdb": False,
            "readable_by_pandas": False,
            "total_rows": 0,
            "error_details": []
        }
        
        try:
            import os
            import pandas as pd
            
            # Test 1: Archivo existe
            if os.path.exists(clean_path):
                health_report["file_exists"] = True
                health_report["file_size"] = os.path.getsize(clean_path)
            else:
                health_report["error_details"].append("Archivo no existe")
                return health_report
            
            # Test 2: Legible por DuckDB
            try:
                count_sql = f"SELECT COUNT(*) FROM '{clean_path}'"
                result = duckdb_service.conn.execute(count_sql).fetchone()
                health_report["readable_by_duckdb"] = True
                health_report["total_rows"] = result[0] if result else 0
            except Exception as e:
                health_report["error_details"].append(f"DuckDB no puede leer: {e}")
            
            # Test 3: Legible por Pandas
            try:
                df_test = pd.read_parquet(clean_path, nrows=1)
                health_report["readable_by_pandas"] = True
            except Exception as e:
                health_report["error_details"].append(f"Pandas no puede leer: {e}")
            
        except Exception as e:
            health_report["error_details"].append(f"Error general: {e}")
        
        return health_report

    
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
        """Genera reporte de inasistentes dinámico con descubrimiento automático de actividades - VERSIÓN ROBUSTA"""
        try:
            print(f"🏥 Iniciando reporte de inasistentes para: {filename}")
            
            file_key = f"technical_{filename.replace('.', '_').replace(' ', '_').replace('-', '_')}"
            data_source = DataSourceService(path_technical_note).ensure_data_source_available(filename, file_key)
            
            # DESCUBRIMIENTO DINÁMICO DE COLUMNAS CON FALLBACK ROBUSTO
            if not selected_keywords:
                selected_keywords = ['medicina']
                print(f"🔄 Usando keywords por defecto: {selected_keywords}")
            
            print(f"🔍 Iniciando descubrimiento para keywords: {selected_keywords}")
            discovered_columns = self._discover_activity_columns(data_source, selected_keywords)
            
            # VERIFICAR RESULTADOS DEL DESCUBRIMIENTO
            if not discovered_columns:
                error_msg = f"No se pudieron descubrir actividades para: {selected_keywords}"
                print(f"❌ {error_msg}")
                return {
                    "success": False,
                    "error": error_msg,
                    "inasistentes_por_actividad": [],
                    "resumen_general": {},
                    "debug_info": {
                        "keywords_solicitadas": selected_keywords,
                        "data_source": data_source,
                        "discovery_method": "FAILED"
                    }
                }
            
            # Recopilar todas las columnas encontradas
            all_activity_columns = []
            for keyword, columns in discovered_columns.items():
                all_activity_columns.extend(columns)
            
            if not all_activity_columns:
                error_msg = f"No se encontraron columnas de actividad para: {selected_keywords}"
                print(f"❌ {error_msg}")
                return {
                    "success": False,
                    "error": error_msg,
                    "inasistentes_por_actividad": [],
                    "resumen_general": {},
                    "debug_info": {
                        "discovered_mapping": discovered_columns,
                        "discovery_method": "EMPTY_RESULTS"
                    }
                }
            
            print(f"🎯 TOTAL ACTIVIDADES ENCONTRADAS: {len(all_activity_columns)}")
            
            # CONSTRUIR FILTROS DE EDAD
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
            
            # CONSTRUIR FILTROS GEOGRÁFICOS
            geo_conditions = []
            
            if departamento:
                geo_conditions.append(f'"Departamento" = \'{departamento}\'')
            
            if municipio:
                geo_conditions.append(f'"Municipio" = \'{municipio}\'')
                
            if ips:
                geo_conditions.append(f'"Nombre IPS" = \'{ips}\'')
            
            geo_filter = " AND ".join(geo_conditions) if geo_conditions else "1=1"
            
            # GENERAR REPORTES POR CADA ACTIVIDAD
            print(f"🔧 Generando reportes individuales por actividad...")
            activity_reports = ReportActivity().generate_activity_reports(
                data_source, all_activity_columns, age_filter, geo_filter, corte_fecha
            )
            
            # CALCULAR RESUMEN GENERAL
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
            
            print(f"✅ Reporte completado exitosamente: {total_inasistentes} inasistentes encontrados")
            
            return {
                "success": True,
                "filename": filename,
                "corte_fecha": corte_fecha,
                "metodo": "DESCUBRIMIENTO_DINAMICO_ROBUSTO",
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
                "engine": "DuckDB_Dynamic_Discovery_v2"
            }
            
        except Exception as e:
            print(f"❌ ERROR CRÍTICO generando reporte dinámico: {e}")
            import traceback
            traceback.print_exc()
            return {
                "success": False,
                "error": str(e),
                "inasistentes_por_actividad": [],
                "debug_info": {
                    "error_type": type(e).__name__,
                    "filename": filename,
                    "selected_keywords": selected_keywords
                }
            }

    # Resto de métodos existentes sin cambios...
    def _get_keyword_column_mapping(self) -> Dict[str, List[str]]:
        """Mapeo legacy - ahora se usa descubrimiento dinámico"""
        return {
            'medicina': ['"Consulta por Medicina 1 mes"'],
        }
    
    def export_inasistentes_to_csv(
        self,
        filename: str,
        selected_months: List[int],
        selected_years: List[int] = None,
        selected_keywords: List[str] = None,  
        corte_fecha: str = "2025-07-31",
        departamento: Optional[str] = None,
        municipio: Optional[str] = None,
        ips: Optional[str] = None,
        path_technical_note = '',
        encoding: str = "cp1252", 
        use_excel_sep_hint: bool = True, 
        sep: str = ";"              
    ) -> StreamingResponse:

        report_data = self.get_inasistentes_report(
            filename, selected_months, selected_years, selected_keywords,
            corte_fecha, departamento, municipio, ips, path_technical_note
        )
        if not report_data.get("success"):
            raise Exception(report_data.get("error", "Error generando reporte"))

        rows = []
        for activity_report in report_data["inasistentes_por_actividad"]:
            for r in activity_report["inasistentes"]:
                rows.append({
                    "Departamento": r["departamento"],
                    "Municipio": r["municipio"],
                    "Nombre IPS": r["nombre_ips"],
                    "Número Identificación": r["nro_identificacion"],
                    "Primer Apellido": r["primer_apellido"],
                    "Segundo Apellido": r["segundo_apellido"],
                    "Primer Nombre": r["primer_nombre"],
                    "Segundo Nombre": r["segundo_nombre"],
                    "Fecha Nacimiento": r["fecha_nacimiento"],
                    "Edad Años": r["edad_anos"],
                    "Edad Meses": r["edad_meses"],
                    "Actividad Faltante": r["columna_evaluada"],
                    "Estado Actividad": r["actividad_valor"],
                    "Grupo Actividad": activity_report["actividad"],
                    "Fecha Corte": report_data["corte_fecha"],
                })

        df = pd.DataFrame(rows, columns=[
            "Departamento","Municipio","Nombre IPS","Número Identificación",
            "Primer Apellido","Segundo Apellido","Primer Nombre","Segundo Nombre",
            "Fecha Nacimiento","Edad Años","Edad Meses",
            "Actividad Faltante","Estado Actividad","Grupo Actividad",
            "Fecha Corte"
        ])

        # Mapa de codificación
        enc_map = {"cp1252": "cp1252", "latin-1": "latin-1", "utf-8-sig": "utf-8-sig"}
        enc = enc_map.get(encoding.lower(), "cp1252")

        # Escribir a buffer binario con separador ";"
        buf = io.BytesIO()

        # 1) Escribir opcionalmente la línea "sep=;" para Excel
        if use_excel_sep_hint:
            # Escribir “sep=;” con la misma codificación
            prefix = ("sep=" + sep + "\n").encode(enc)
            buf.write(prefix)

        # 2) Escribir el CSV con pandas y separador ";"
        #    quoting opcional: csv.QUOTE_MINIMAL (por defecto) o csv.QUOTE_ALL si necesitas comillas siempre
        df.to_csv(
            buf,
            index=False,
            encoding=enc,
            sep=sep,                    
            quoting=csv.QUOTE_MINIMAL,  # 
            quotechar='"',
            lineterminator="\n"
        )
        buf.seek(0)

        # Nombre de archivo (sanitizado para filename, no para contenido)
        def safe_name(s: str) -> str:
            repl = (("ñ","n"),("Ñ","N"),("á","a"),("é","e"),("í","i"),("ó","o"),("ú","u"),
                    ("Á","A"),("É","E"),("Í","I"),("Ó","O"),("Ú","U"))
            for a,b in repl: s = s.replace(a,b)
            return s.replace(" ", "-")

        filters = []
        if selected_keywords: filters.append("palabras-" + "-".join(safe_name(k) for k in selected_keywords))
        if selected_months:   filters.append("meses-" + "-".join(map(str, selected_months)))
        if selected_years:    filters.append("años-" + "-".join(map(str, selected_years)))
        if departamento:      filters.append("dept-" + safe_name(departamento))
        suffix = "_" + "_".join(filters) if filters else ""
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        out_name = f"inasistentes_{filename.replace('.csv','')}{suffix}_{corte_fecha}.csv"

        def stream():
            buf.seek(0)
            chunk = buf.read(8192)
            while chunk:
                yield chunk
                chunk = buf.read(8192)

        charset = "windows-1252" if enc == "cp1252" else ("ISO-8859-1" if enc == "latin-1" else "utf-8")
        return StreamingResponse(
            stream(),
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": f'attachment; filename="{out_name}"',
                "Content-Type": f"text/csv; charset={charset}"
            }
        )
