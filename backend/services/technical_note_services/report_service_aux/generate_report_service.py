# services/technical_note_services/report_service_aux/generate_report_service.py
from typing import Dict, Any, List, Optional
import pandas as pd
from services.duckdb_service.duckdb_service import duckdb_service
from services.keyword_age_report import ColumnKeywordReportService, KeywordRule
from utils.config_loader import config_loader
from services.technical_note_services.date_range_calculator import date_range_calculator
from services.technical_note_services.nt_rpms_integration import NTRPMSIntegration
from services.technical_note_services.report_service_aux.semaforization import Semaforization


class GenerateReport:
    def __init__(self):
        self.config = config_loader
        self.semaforo = Semaforization()
        self.nt_rpms_integration = None
    
    def set_nt_rpms_path(self, parquet_path: str):
        """Configura integración con NT RPMS"""
        if parquet_path:
            self.nt_rpms_integration = NTRPMSIntegration(parquet_path)
    
    def generate_keyword_age_report_extended(
        self,
        age_extractor,
        data_source: str,
        filename: str,
        keywords: Optional[List[str]] = None,
        geographic_filters: Optional[Dict[str, Optional[str]]] = None,
        corte_fecha: str = None,
        nt_rpms_parquet_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Genera reporte extendido con análisis mensual, trimestral, semestral y anual
        
        Args:
            age_extractor: Extractor de rangos de edad
            data_source: Fuente de datos del usuario
            filename: Nombre del archivo
            keywords: Palabras clave para búsqueda
            geographic_filters: Filtros geográficos
            corte_fecha: Fecha de corte (YYYY-MM-DD)
            nt_rpms_parquet_path: Ruta al Parquet NT RPMS consolidado
        
        Returns:
            Reporte completo con análisis extendido
        """
        try:
            # Validar fecha de corte
            if not corte_fecha:
                raise ValueError("El parámetro 'corte_fecha' es obligatorio")
            
            # Validar data_source
            if not data_source or data_source is None or data_source == Ellipsis:
                raise ValueError("data_source inválido: debe ser cadena con nombre o expresión válida")
            
            print(f"\n{'='*80}")
            print("GENERANDO REPORTE EXTENDIDO CON ANÁLISIS MENSUAL/TRIMESTRAL/SEMESTRAL/ANUAL")
            print(f"{'='*80}")
            print(f"Filename: {filename}")  # 🔥 AGREGADO
            print(f"Fecha de corte: {corte_fecha}")
            
            # Configurar NT RPMS si se proporciona
            if nt_rpms_parquet_path:
                self.set_nt_rpms_path(nt_rpms_parquet_path)
                print(f"✓ NT RPMS configurado: {nt_rpms_parquet_path}")
            
            # Extraer filtros geográficos
            geographic_filters = geographic_filters or {}
            departamento = geographic_filters.get('departamento')
            municipio = geographic_filters.get('municipio')
            ips = geographic_filters.get('ips')
            
            # Validar filtros geográficos
            if not departamento and not municipio and not ips:
                print("⚠️ Sin filtros geográficos - valores en 0")
                return self._build_empty_report(filename, keywords, geographic_filters, corte_fecha)
            
            # Obtener meses hasta fecha de corte
            meses_reporte = date_range_calculator.get_months_until_cutoff(corte_fecha)
            print(f"✓ Meses a reportar: {len(meses_reporte)}")
            
            # Matching de columnas
            columns = self._get_table_columns(data_source)
            rules = [KeywordRule(name=k, synonyms=(k.lower(),)) for k in keywords] if keywords else None
            service = ColumnKeywordReportService(keywords=rules)
            matches = service.match_columns(columns)
            
            if not matches:
                print("✗ No se encontraron columnas coincidentes")
                return self._build_empty_report(filename, keywords, geographic_filters, corte_fecha)
            
            print(f"✓ {len(matches)} columnas encontradas")
            
            # Generar reporte por cada match
            report_rows = []
            
            for match in matches:
                column_name = match['column']
                keyword = match['keyword']
                
                print(f"\n--- Procesando: {column_name} ---")
                
                # Extraer rango de edad
                age_range_obj = age_extractor.extract_age_range(column_name)
                
                if not age_range_obj:
                    print(f"⚠️ No se pudo extraer rango de edad")
                    continue
                
                # 🔥 MODIFICADO: Buscar mapping con filename para carga dinámica
                mapping = self.config.find_mapping(column_name, filename=filename)
                
                # Procesar según tipo de edad
                if age_range_obj.unit == 'months':
                    row_data = self._process_month_age_range(
                        data_source, column_name, age_range_obj, meses_reporte,
                        departamento, municipio, ips, mapping, filename  # 🔥 AGREGADO filename
                    )
                elif age_range_obj.unit == 'years':
                    row_data = self._process_year_age_range(
                        data_source, column_name, age_range_obj, meses_reporte,
                        departamento, municipio, ips, mapping, filename  # 🔥 AGREGADO filename
                    )
                else:
                    continue
                
                if row_data:
                    report_rows.append(row_data)
            
            # Convertir a DataFrame
            if not report_rows:
                return self._build_empty_report(filename, keywords, geographic_filters, corte_fecha)
            
            df = pd.DataFrame(report_rows)
            
            # Calcular agregados trimestrales, semestrales y anuales
            df = self._calculate_aggregates(df, meses_reporte)
            
            # Construir resultado final
            return {
                "success": True,
                "filename": filename,
                "corte_fecha": corte_fecha,
                "keywords": keywords or [],
                "geographic_filters": geographic_filters,
                "meses_reportados": len(meses_reporte),
                "data": df.to_dict('records'),
                "columns": df.columns.tolist(),
                "total_rows": len(df),
                "metodo": "EXTENDED_MONTHLY_QUARTERLY_SEMESTER_ANNUAL_DYNAMIC",  # 🔥 MODIFICADO
                "engine": "DuckDB_JSON_Config_Dynamic_v8"  # 🔥 MODIFICADO
            }
            
        except Exception as e:
            print(f"✗ Error generando reporte extendido: {e}")
            import traceback
            traceback.print_exc()
            raise ValueError(f"Error en generación de reporte: {e}")
    
    def _process_month_age_range(
        self,
        data_source: str,
        column_name: str,
        age_range_obj,
        meses_reporte: List[tuple],
        departamento: str,
        municipio: str,
        ips: str,
        mapping: Dict[str, Any],
        filename: str  # 🔥 NUEVO PARÁMETRO
    ) -> Dict[str, Any]:
        """Procesa rango de edad en meses"""
        try:
            min_meses = age_range_obj.min_age
            max_meses = age_range_obj.max_age
            
            # Inicializar fila de resultado
            row = {
                "consulta_procedimiento": column_name,
                "rango_edad": age_range_obj.get_description(),
                "tipo_edad": "meses"
            }
            
            # Calcular población objetivo (primer mes del reporte)
            mes_ref, anio_ref, _ = meses_reporte[0]
            fecha_inicio, fecha_fin = date_range_calculator.calculate_birth_range_for_month_age(
                mes_ref, anio_ref, min_meses, max_meses
            )
            
            poblacion_objeto = self._count_population(
                data_source, fecha_inicio, fecha_fin, departamento, municipio, ips
            )
            
            row["poblacion_objeto"] = poblacion_objeto
            
            # Obtener datos de NT RPMS usando edad_NT_RPMS
            if self.nt_rpms_integration and mapping:
                consolidado_info = mapping.get("consolidado", {})
                consulta_ntrpms = consolidado_info.get("consulta_procedimiento")
                edad_NT_RPMS = consolidado_info.get("edad_NT_RPMS")
                
                print(f"🔍 Buscando en NT_RPMS:")
                print(f"   Consulta: {consulta_ntrpms}")
                print(f"   Edad NT_RPMS: {edad_NT_RPMS}")
                print(f"   Filename: {filename}")  # 🔥 AGREGADO
                
                # Pasar edad_NT_RPMS al buscar
                nt_data = self.nt_rpms_integration.find_matching_row(
                    consulta_ntrpms, 
                    edad_NT_RPMS,
                    departamento, 
                    municipio, 
                    ips
                )
                
                if nt_data:
                    print(f"✅ Datos encontrados en NT_RPMS")
                    metrics = self.nt_rpms_integration.calculate_extended_metrics(
                        poblacion_objeto, nt_data
                    )
                    row.update(metrics)
                    denominador_mensual = metrics["valor_mensual"]
                else:
                    print(f"⚠️ No se encontraron datos en NT_RPMS")
                    denominador_mensual = 0
                    row.update({
                        "poblacion_susceptible": 0,
                        "valor_mensual": 0,
                        "meta": 0,
                        "frecuencia_uso": 0,
                        "proyeccion_tiempo": 12
                    })
            else:
                denominador_mensual = 0
                row.update({
                    "poblacion_susceptible": 0,
                    "valor_mensual": 0,
                    "meta": 0,
                    "frecuencia_uso": 0,
                    "proyeccion_tiempo": 12
                })
            
            # Calcular numerador por cada mes
            for mes_num, anio_num, nombre_mes in meses_reporte:
                fecha_inicio, fecha_fin = date_range_calculator.calculate_birth_range_for_month_age(
                    mes_num, anio_num, min_meses, max_meses
                )
                
                numerador = self._count_with_activity(
                    data_source, column_name, fecha_inicio, fecha_fin,
                    departamento, municipio, ips, mes_num, anio_num
                )
                
                row[f"{nombre_mes}_numerador"] = numerador
                row[f"{nombre_mes}_denominador"] = denominador_mensual
                
                # Calcular porcentaje y semáforo
                if denominador_mensual > 0:
                    porcentaje = (numerador / denominador_mensual) * 100
                else:
                    porcentaje = 0
                
                semaforo = self.semaforo.calculate_semaforizacion(numerador, porcentaje)
                
                row[f"{nombre_mes}_porcentaje"] = round(porcentaje, 2)
                row[f"{nombre_mes}_semaforo"] = semaforo["estado"]
            
            return row
            
        except Exception as e:
            print(f"Error procesando meses: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _process_year_age_range(
        self,
        data_source: str,
        column_name: str,
        age_range_obj,
        meses_reporte: List[tuple],
        departamento: str,
        municipio: str,
        ips: str,
        mapping: Dict[str, Any],
        filename: str  # 🔥 NUEVO PARÁMETRO
    ) -> Dict[str, Any]:
        """Procesa rango de edad en años"""
        try:
            edad_anios = age_range_obj.min_age
            
            # Inicializar fila
            row = {
                "consulta_procedimiento": column_name,
                "rango_edad": age_range_obj.get_description(),
                "tipo_edad": "anios"
            }
            
            # Calcular población objetivo (suma de todos los meses con esa edad)
            poblacion_objetivo_total = 0
            
            for mes_num, anio_num, _ in meses_reporte:
                fecha_inicio, fecha_fin = date_range_calculator.calculate_birth_range_for_year_age(
                    mes_num, anio_num, edad_anios
                )
                poblacion = self._count_population(
                    data_source, fecha_inicio, fecha_fin, departamento, municipio, ips
                )
                poblacion_objetivo_total += poblacion
            
            row["poblacion_objeto"] = poblacion_objetivo_total
            
            # Obtener datos NT RPMS usando edad_NT_RPMS
            if self.nt_rpms_integration and mapping:
                consolidado_info = mapping.get("consolidado", {})
                consulta_ntrpms = consolidado_info.get("consulta_procedimiento")
                edad_NT_RPMS = consolidado_info.get("edad_NT_RPMS")
                
                print(f"🔍 Buscando en NT_RPMS:")
                print(f"   Consulta: {consulta_ntrpms}")
                print(f"   Edad NT_RPMS: {edad_NT_RPMS}")
                print(f"   Filename: {filename}")  # 🔥 AGREGADO
                
                # Pasar edad_NT_RPMS al buscar
                nt_data = self.nt_rpms_integration.find_matching_row(
                    consulta_ntrpms,
                    edad_NT_RPMS,
                    departamento,
                    municipio,
                    ips
                )
                
                if nt_data:
                    print(f"✅ Datos encontrados en NT_RPMS")
                    metrics = self.nt_rpms_integration.calculate_extended_metrics(
                        poblacion_objetivo_total, nt_data
                    )
                    row.update(metrics)
                    denominador_mensual = metrics["valor_mensual"]
                else:
                    print(f"⚠️ No se encontraron datos en NT_RPMS")
                    denominador_mensual = 0
                    row.update({
                        "poblacion_susceptible": 0,
                        "valor_mensual": 0,
                        "meta": 0,
                        "frecuencia_uso": 0,
                        "proyeccion_tiempo": 12
                    })
            else:
                denominador_mensual = 0
                row.update({
                    "poblacion_susceptible": 0,
                    "valor_mensual": 0,
                    "meta": 0,
                    "frecuencia_uso": 0,
                    "proyeccion_tiempo": 12
                })
            
            # Calcular por mes
            for mes_num, anio_num, nombre_mes in meses_reporte:
                fecha_inicio, fecha_fin = date_range_calculator.calculate_birth_range_for_year_age(
                    mes_num, anio_num, edad_anios
                )
                
                numerador = self._count_with_activity(
                    data_source, column_name, fecha_inicio, fecha_fin,
                    departamento, municipio, ips, mes_num, anio_num
                )
                
                row[f"{nombre_mes}_numerador"] = numerador
                row[f"{nombre_mes}_denominador"] = denominador_mensual
                
                if denominador_mensual > 0:
                    porcentaje = (numerador / denominador_mensual) * 100
                else:
                    porcentaje = 0
                
                semaforo = self.semaforo.calculate_semaforizacion(numerador, porcentaje)
                
                row[f"{nombre_mes}_porcentaje"] = round(porcentaje, 2)
                row[f"{nombre_mes}_semaforo"] = semaforo["estado"]
            
            return row
            
        except Exception as e:
            print(f"Error procesando años: {e}")
            import traceback
            traceback.print_exc()
            return None

        
    def _count_population(
        self,
        data_source: str,
        fecha_inicio: str,
        fecha_fin: str,
        departamento: str,
        municipio: str,
        ips: str
    ) -> int:
        """Cuenta población en rango de fechas de nacimiento"""
        try:
            geo_filters = []
            if departamento:
                geo_filters.append(f'"Departamento" = \'{departamento}\'')
            if municipio:
                geo_filters.append(f'"Municipio" = \'{municipio}\'')
            if ips:
                geo_filters.append(f'"Nombre IPS" = \'{ips}\'')
            
            geo_filter = " AND ".join(geo_filters) if geo_filters else "1=1"
            
            sql = f"""
            SELECT COUNT(DISTINCT "Nro Identificación")
            FROM {data_source}
            WHERE {date_range_calculator.generate_sql_date_filter(fecha_inicio, fecha_fin)}
            AND "Fecha Nacimiento" IS NOT NULL
            AND "Nro Identificación" IS NOT NULL
            AND {geo_filter}
            """
            
            result = duckdb_service.conn.execute(sql).fetchone()
            return int(result[0]) if result[0] else 0
            
        except Exception as e:
            print(f"Error contando población: {e}")
            return 0
    
    def _count_with_activity(
        self,
        data_source: str,
        column_name: str,
        fecha_inicio: str,
        fecha_fin: str,
        departamento: str,
        municipio: str,
        ips: str,
        mes: int,
        anio: int
    ) -> int:
        """Cuenta registros con actividad en período"""
        try:
            column_safe = f'"{column_name}"' if not column_name.startswith('"') else column_name
            
            geo_filters = []
            if departamento:
                geo_filters.append(f'"Departamento" = \'{departamento}\'')
            if municipio:
                geo_filters.append(f'"Municipio" = \'{municipio}\'')
            if ips:
                geo_filters.append(f'"Nombre IPS" = \'{ips}\'')
            
            geo_filter = " AND ".join(geo_filters) if geo_filters else "1=1"
            
            sql = f"""
            SELECT COUNT(DISTINCT "Nro Identificación")
            FROM {data_source}
            WHERE {date_range_calculator.generate_sql_date_filter(fecha_inicio, fecha_fin)}
            AND "Fecha Nacimiento" IS NOT NULL
            AND "Nro Identificación" IS NOT NULL
            AND {column_safe} IS NOT NULL
            AND TRIM(CAST({column_safe} AS VARCHAR)) != ''
            AND TRIM(CAST({column_safe} AS VARCHAR)) NOT IN ('NULL', 'null', 'None', 'none', 'NaN', 'nan', 'N/A', 'n/a', '-', 'No')
            AND {geo_filter}
            """
            
            result = duckdb_service.conn.execute(sql).fetchone()
            return int(result[0]) if result[0] else 0
            
        except Exception as e:
            print(f"Error contando con actividad: {e}")
            return 0
    
    def _calculate_aggregates(self, df: pd.DataFrame, meses_reporte: List[tuple]) -> pd.DataFrame:
        """Calcula agregados trimestrales, semestrales y anuales"""
        try:
            structure = self.config.get_report_structure()
            
            # Trimestres
            for trim_key, trim_config in structure["trimestres"].items():
                meses = trim_config["meses"]
                
                # Filtrar solo meses que están en el reporte
                meses_disponibles = [m for m in meses if any(nombre == m for _, _, nombre in meses_reporte)]
                
                if meses_disponibles:
                    # Sumar numeradores
                    num_cols = [f"{m}_numerador" for m in meses_disponibles if f"{m}_numerador" in df.columns]
                    if num_cols:
                        df[trim_config["col_numerador"]] = df[num_cols].sum(axis=1)
                    
                    # Sumar denominadores
                    den_cols = [f"{m}_denominador" for m in meses_disponibles if f"{m}_denominador" in df.columns]
                    if den_cols:
                        df[trim_config["col_denominador"]] = df[den_cols].sum(axis=1)
                    
                    # Calcular porcentaje
                    if trim_config["col_numerador"] in df.columns and trim_config["col_denominador"] in df.columns:
                        df[trim_config["col_porcentaje"]] = df.apply(
                            lambda row: round((row[trim_config["col_numerador"]] / row[trim_config["col_denominador"]] * 100), 2)
                            if row[trim_config["col_denominador"]] > 0 else 0,
                            axis=1
                        )
                        
                        # Semaforización
                        df[trim_config["col_semaforo"]] = df.apply(
                            lambda row: self.semaforo.calculate_semaforizacion(
                                row[trim_config["col_numerador"]],
                                row[trim_config["col_porcentaje"]]
                            )["estado"],
                            axis=1
                        )
            
            # Semestres
            for sem_key, sem_config in structure["semestres"].items():
                trimestres = sem_config["trimestres"]
                
                # Sumar numeradores de trimestres
                num_cols = [structure["trimestres"][t]["col_numerador"] for t in trimestres 
                           if structure["trimestres"][t]["col_numerador"] in df.columns]
                if num_cols:
                    df[sem_config["col_numerador"]] = df[num_cols].sum(axis=1)
                
                # Sumar denominadores
                den_cols = [structure["trimestres"][t]["col_denominador"] for t in trimestres 
                           if structure["trimestres"][t]["col_denominador"] in df.columns]
                if den_cols:
                    df[sem_config["col_denominador"]] = df[den_cols].sum(axis=1)
                
                # Calcular porcentaje y semáforo
                if sem_config["col_numerador"] in df.columns and sem_config["col_denominador"] in df.columns:
                    df[sem_config["col_porcentaje"]] = df.apply(
                        lambda row: round((row[sem_config["col_numerador"]] / row[sem_config["col_denominador"]] * 100), 2)
                        if row[sem_config["col_denominador"]] > 0 else 0,
                        axis=1
                    )
                    
                    df[sem_config["col_semaforo"]] = df.apply(
                        lambda row: self.semaforo.calculate_semaforizacion(
                            row[sem_config["col_numerador"]],
                            row[sem_config["col_porcentaje"]]
                        )["estado"],
                        axis=1
                    )
            
            # Anual
            anual_config = structure["anual"]
            semestres = anual_config["semestres"]
            
            # Sumar semestres
            num_cols = [structure["semestres"][s]["col_numerador"] for s in semestres 
                       if structure["semestres"][s]["col_numerador"] in df.columns]
            if num_cols:
                df[anual_config["col_numerador"]] = df[num_cols].sum(axis=1)
            
            den_cols = [structure["semestres"][s]["col_denominador"] for s in semestres 
                       if structure["semestres"][s]["col_denominador"] in df.columns]
            if den_cols:
                df[anual_config["col_denominador"]] = df[den_cols].sum(axis=1)
            
            # Calcular porcentaje y semáforo anual
            if anual_config["col_numerador"] in df.columns and anual_config["col_denominador"] in df.columns:
                df[anual_config["col_porcentaje"]] = df.apply(
                    lambda row: round((row[anual_config["col_numerador"]] / row[anual_config["col_denominador"]] * 100), 2)
                    if row[anual_config["col_denominador"]] > 0 else 0,
                    axis=1
                )
                
                df[anual_config["col_semaforo"]] = df.apply(
                    lambda row: self.semaforo.calculate_semaforizacion(
                        row[anual_config["col_numerador"]],
                        row[anual_config["col_porcentaje"]]
                    )["estado"],
                    axis=1
                )
            
            return df
            
        except Exception as e:
            print(f"Error calculando agregados: {e}")
            return df
    
    def _get_table_columns(self, data_source: str) -> List[str]:
        """Obtiene columnas de la tabla"""
        try:
            describe_sql = f"DESCRIBE SELECT * FROM {data_source}"
            columns_result = duckdb_service.conn.execute(describe_sql).fetchall()
            return [row[0] for row in columns_result]
        except Exception as e:
            print(f"Error obteniendo columnas: {e}")
            raise ValueError("Error analizando estructura de datos")
    
    def _build_empty_report(
        self,
        filename: str,
        keywords: List[str],
        geographic_filters: Dict[str, Optional[str]],
        corte_fecha: str
    ) -> Dict[str, Any]:
        """Construye reporte vacío"""
        return {
            "success": True,
            "filename": filename,
            "corte_fecha": corte_fecha,
            "keywords": keywords or [],
            "geographic_filters": geographic_filters,
            "data": [],
            "columns": [],
            "total_rows": 0,
            "message": "Sin filtros geográficos o sin datos coincidentes",
            "metodo": "EMPTY_REPORT"
        }
