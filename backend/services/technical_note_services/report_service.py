# services/technical_note_services/report_service.py

import json
import os
from typing import Dict, Any, List, Optional
from datetime import datetime
from services.duckdb_service.duckdb_service import duckdb_service
from services.technical_note_services.nt_rpms_integration import NTRPMSIntegration
from services.technical_note_services.report_service_aux.corrected_months import CorrectedMonths
from services.technical_note_services.report_service_aux.corrected_years import CorrectedYear
from services.technical_note_services.report_service_aux.identity_document import IdentityDocument
from utils.text_normalizer import normalize_text


class ReportService:
    
    def __init__(self):
        self.corrected_months = CorrectedMonths()
        self.corrected_years = CorrectedYear()
        self.identity_document = IdentityDocument()
        
        # Cargar rangos de fechas desde JSON
        self.birth_date_ranges_den = self._load_birth_date_ranges_den()
        
        # Cargar rangos compuestos
        self.rangos_compuestos = self._load_rangos_compuestos()
    
    def _load_birth_date_ranges_den(self) -> Dict[str, Dict[int, tuple]]:
        """Carga rangos de fechas de nacimiento desde archivo JSON"""
        try:
            config_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'config')
            json_path = os.path.join(config_dir, 'birth_date_ranges_den.json')
            
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            birth_date_ranges_den = {}
            for edad_key, meses_dict in data.items():
                birth_date_ranges_den[edad_key] = {
                    int(mes): tuple(fechas) 
                    for mes, fechas in meses_dict.items()
                }
            
            print(f"✓ Rangos de fechas cargados: {len(birth_date_ranges_den)} grupos de edad")
            return birth_date_ranges_den
        
        except FileNotFoundError:
            print(f"⚠️ No se encontró birth_date_ranges_den.json, usando dict vacío")
            return {}
        
        except Exception as e:
            print(f"⚠️ Error cargando birth_date_ranges_den.json: {e}")
            return {}
    
    def _load_rangos_compuestos(self) -> Dict[str, Any]:
        """Carga rangos compuestos desde age_ranges.json"""
        try:
            config_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'config')
            json_path = os.path.join(config_dir, 'age_ranges.json')
            
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Convertir lista a diccionario por label (normalizado)
            rangos_dict = {}
            for rango in data.get('rangos_compuestos', []):
                label_norm = normalize_text(rango['label'])
                rangos_dict[label_norm] = rango
                # También guardar con label original
                rangos_dict[rango['label']] = rango
            
            print(f"✓ Rangos compuestos cargados: {len(data.get('rangos_compuestos', []))} rangos")
            return rangos_dict
        
        except Exception as e:
            print(f"⚠️ Error cargando rangos compuestos: {e}")
            return {}
    
    @property
    def conn(self):
        return duckdb_service.conn
    
    def generate_keyword_age_report(
        self,
        data_source: str,
        filename: str,
        keywords: Optional[List[str]] = None,
        geographic_filters: Dict[str, Any] = None,
        corte_fecha: str = None,
        nt_rpms_data_source: Optional[str] = None,
        column_mappings: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        🔥 VERSIÓN DEPURADA: Genera reporte con normalización, matching correcto y sin duplicados.
        """
        try:
            print(f"\n{'='*80}")
            print("GENERANDO REPORTE CON DENOMINADOR MENSUAL CONSTANTE (SIN DUPLICADOS)")
            print(f"{'='*80}")
            print(f"Fecha de corte: {corte_fecha}")
            
            corte_dt = datetime.strptime(corte_fecha, '%Y-%m-%d')
            mes_limite = corte_dt.month
            anio_corte = corte_dt.year
            
            print(f"📅 Mes límite: {mes_limite} (hasta {corte_dt.strftime('%B %Y')})")
            
            nt_rpms_integration = None
            if nt_rpms_data_source:
                nt_rpms_integration = NTRPMSIntegration(nt_rpms_data_source)
                print(f"✅ NT_RPMS integration inicializada")
            
            depto = geographic_filters.get('departamento') if geographic_filters else None
            muni = geographic_filters.get('municipio') if geographic_filters else None
            ips_name = geographic_filters.get('ips') if geographic_filters else None
            
            print(f"📍 Filtros geográficos:")
            print(f"   Departamento: {depto}")
            print(f"   Municipio: {muni}")
            print(f"   IPS: {ips_name}")
            
            if not depto and not muni and not ips_name:
                print("⚠️ Sin filtros geográficos - retornando reporte vacío")
                return self._empty_report(filename, corte_fecha, keywords, geographic_filters)
            
            where_conditions = []
            if depto:
                where_conditions.append(f'"Departamento" = \'{depto}\'')
            if muni:
                where_conditions.append(f'"Municipio" = \'{muni}\'')
            if ips_name:
                where_conditions.append(f'"Nombre IPS" = \'{ips_name}\'')
            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"

            describe_query = f"DESCRIBE SELECT * FROM {data_source}"
            columns_result = self.conn.execute(describe_query).fetchall()
            all_columns = [row[0] for row in columns_result]
            print(f"📋 Dataset tiene {len(all_columns)} columnas totales")

            # === NUEVA SECCIÓN PARA EVITAR DUPLICADOS ===
            matching_columns_set = set()
            if column_mappings and 'mappings' in column_mappings:
                print(f"📝 Analizando mappings...")
                relevant_mappings = []
                for mapping in column_mappings.get('mappings', []):
                    columna_usuario = mapping.get('columna_usuario', '')
                    mapping_keyword = mapping.get('keyword', '')
                    if not columna_usuario or not mapping_keyword:
                        continue
                    if keywords:
                        keywords_norm = [normalize_text(kw) for kw in keywords]
                        mapping_keyword_norm = normalize_text(mapping_keyword)
                        keyword_match = False
                        matched_kw = None
                        for kw_norm in keywords_norm:
                            if kw_norm in mapping_keyword_norm or mapping_keyword_norm in kw_norm:
                                keyword_match = True
                                matched_kw = kw_norm
                                break
                        if keyword_match:
                            relevant_mappings.append(mapping)
                            if len(relevant_mappings) <= 3:
                                print(f"   ✓ Mapping incluido: '{columna_usuario}' (keyword: '{matched_kw}')")
                    else:
                        relevant_mappings.append(mapping)
                print(f"   → {len(relevant_mappings)} mappings relevantes para keywords {keywords}")

                mapped_columns_from_relevants = set()
                for mapping in relevant_mappings:
                    columna_usuario = mapping.get('columna_usuario', '')
                    if columna_usuario:
                        mapped_columns_from_relevants.add(columna_usuario)
                print(f"   → {len(mapped_columns_from_relevants)} columnas únicas en mappings")

                for col_dataset in all_columns:
                    col_dataset_norm = normalize_text(col_dataset)
                    for mapped_col in mapped_columns_from_relevants:
                        mapped_col_norm = normalize_text(mapped_col)
                        if col_dataset_norm == mapped_col_norm:
                            matching_columns_set.add(col_dataset)
                            print(f"   ✅ MATCH: Dataset '{col_dataset}' ←→ Mapping '{mapped_col}'")
                            break

            matching_columns = list(matching_columns_set)

            if not matching_columns:
                print(f"\n❌ No se encontraron columnas del mapping en el dataset")
                print(f"   Keywords solicitadas: {keywords}")
                print(f"\n   📋 Primeras 20 columnas del dataset:")
                for i, col in enumerate(all_columns[:20], 1):
                    col_norm = normalize_text(col)
                    print(f"      [{i:2}] '{col}' → '{col_norm}'")
                return self._empty_report(filename, corte_fecha, keywords, geographic_filters)

            print(f"\n✓ {len(matching_columns)} columnas encontradas desde mappings")
            
            report_items = []
            meses_nombres = ["enero", "febrero", "marzo", "abril", "mayo", "junio",
                            "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]
            for col in matching_columns:
                print(f"\n🔄 Procesando: {col}")
                all_mappings = self._find_all_mappings_for_column(col, column_mappings)
                if not all_mappings:
                    print(f"   ⚠️ No se encontraron mappings")
                    continue
                print(f"   📋 {len(all_mappings)} mappings encontrados")
                valid_mappings = self._filter_mappings_by_population_availability(
                    all_mappings, data_source, where_clause, corte_fecha
                )
                if not valid_mappings:
                    print(f"   ⚠️ Ningún mapping tiene población disponible")
                    continue
                print(f"   ✅ {len(valid_mappings)} mappings con población")
                for idx, mapping in enumerate(valid_mappings, 1):
                    print(f"\n   [{idx}/{len(valid_mappings)}] Procesando mapping...")
                    item = self._process_single_mapping(
                        mapping=mapping,
                        column_name=col,
                        data_source=data_source,
                        where_clause=where_clause,
                        corte_fecha=corte_fecha,
                        anio_corte=anio_corte,
                        mes_limite=mes_limite,
                        meses_nombres=meses_nombres,
                        nt_rpms_integration=nt_rpms_integration,
                        depto=depto,
                        muni=muni,
                        ips_name=ips_name
                    )
                    if item:
                        report_items.append(item)
                        print(f"      ✓ Item generado")

            print(f"\n{'='*80}")
            print(f"✅ Reporte generado: {len(report_items)} items")
            print(f"{'='*80}\n")
            return {
                'success': True,
                'filename': filename,
                'corte_fecha': corte_fecha,
                'keywords': keywords or [],
                'geographic_filters': geographic_filters or {},
                'data': report_items,
                'total_rows': len(report_items),
                'meses_reportados': mes_limite,
                'metodo': 'multiple_mappings_normalized_sin_duplicados'
            }        
        except Exception as e:
            print(f"\n✗ ERROR: {e}")
            import traceback
            traceback.print_exc()
            raise



    
    def _process_single_mapping(
        self,
        mapping: Dict[str, Any],
        column_name: str,
        data_source: str,
        where_clause: str,
        corte_fecha: str,
        anio_corte: int,
        mes_limite: int,
        meses_nombres: List[str],
        nt_rpms_integration: Optional[NTRPMSIntegration],
        depto: str,
        muni: str,
        ips_name: str
    ) -> Optional[Dict[str, Any]]:
        """Procesa un mapping individual y genera un item del reporte"""
        
        try:
            consolidado_info = mapping.get('consolidado', {})
            consulta_proc = consolidado_info.get('consulta_procedimiento', '')
            edad_aplicable = consolidado_info.get('edad_aplicable', '')
            edad_NT_RPMS = consolidado_info.get('edad_NT_RPMS', '')
            
            print(f"      Consulta: {consulta_proc}")
            print(f"      Edad aplicable: {edad_aplicable}")
            print(f"      Edad NT_RPMS: {edad_NT_RPMS}")
            
            # 🔥 BUSCAR DATOS RPMS CON NORMALIZACIÓN
            rpms_data = None
            if nt_rpms_integration:
                rpms_data = nt_rpms_integration.find_matching_row(
                    consulta_procedimiento=consulta_proc,
                    edad_nt_rpms=edad_NT_RPMS,
                    departamento=depto,
                    municipio=muni,
                    nombre_ips=ips_name
                )
            
            # Calcular población
            poblaciones_mensuales = self._get_population_by_predefined_dates(
                data_source=data_source,
                where_clause=where_clause,
                edad_key=edad_aplicable,
                corte_fecha=corte_fecha
            )
            
            # Población objeto anual
            if -1 in poblaciones_mensuales:
                poblacion_obj_anual = poblaciones_mensuales[-1]
                print(f"      📊 Población objeto (años - conteo único): {poblacion_obj_anual}")
            else:
                poblacion_obj_anual = sum(
                    poblaciones_mensuales.get(i, 0) 
                    for i in range(1, mes_limite + 1)
                )
                print(f"      📊 Población objeto (meses - suma): {poblacion_obj_anual}")
            
            if poblacion_obj_anual == 0:
                print(f"      ⊘ Población = 0, omitiendo item")
                return None
            
            # Calcular valores globales
            if rpms_data:
                frecuencia_indicada = rpms_data.get('frecuencia_indicada', 0)
                proyeccion_tiempo = rpms_data.get('proyeccion_tiempo', 12)
                meta = rpms_data.get('meta', 0)
                
                poblacion_susceptible = poblacion_obj_anual * meta * frecuencia_indicada
                valor_mensual = poblacion_susceptible / proyeccion_tiempo if proyeccion_tiempo > 0 else 0
                denominador_mensual = round(valor_mensual)
                
                print(f"      📊 RPMS encontrado:")
                print(f"         Meta: {meta}, Freq: {frecuencia_indicada}, Proy: {proyeccion_tiempo}")
                print(f"         Pob susceptible: {poblacion_susceptible:.2f}")
                print(f"         Denominador mensual: {denominador_mensual}")
            else:
                print(f"      ⚠️ RPMS NO encontrado - valores en 0")
                frecuencia_indicada = 0
                proyeccion_tiempo = 12
                meta = 0
                poblacion_susceptible = 0
                denominador_mensual = 0
            
            # Calcular numerador por mes
            numeradores_mensuales = self._get_numerador_by_month_with_age_filter(
                data_source=data_source,
                column_name=column_name,
                where_clause=where_clause,
                edad_aplicable=edad_aplicable,
                anio_corte=anio_corte,
                mes_limite=mes_limite,
                corte_fecha=corte_fecha
            )
            
            # Construir datos mensuales
            mensual_data = {}
            
            for mes_num in range(1, 13):
                mes_nombre = meses_nombres[mes_num - 1]
                
                if mes_num <= mes_limite:
                    if -1 in poblaciones_mensuales:
                        poblacion_objeto_mes = poblacion_obj_anual
                    else:
                        poblacion_objeto_mes = poblaciones_mensuales.get(mes_num, 0)
                    
                    numerador = numeradores_mensuales.get(mes_num, 0)
                    denominador = denominador_mensual
                    
                    mensual_data[mes_nombre] = {
                        "poblacion_objeto": poblacion_objeto_mes,
                        "numerador": numerador,
                        "denominador": denominador
                    }
                else:
                    mensual_data[mes_nombre] = {
                        "poblacion_objeto": 0,
                        "numerador": 0,
                        "denominador": 0
                    }
            
            # Calcular consolidados
            trimestres = self._calculate_trimestres(mensual_data, meses_nombres, mes_limite)
            semestres = self._calculate_semestres(trimestres)
            anual = self._calculate_anual(semestres)
            
            # Construir item
            item = {
                "consulta_procedimiento": consulta_proc or column_name,
                "rango_edad": edad_aplicable,
                "poblacion_objeto": poblacion_obj_anual,
                "frecuencia_indicada": frecuencia_indicada,
                "poblacion_susceptible": round(poblacion_susceptible, 0),
                "meta": meta,
                "proyeccion_tiempo": proyeccion_tiempo,
                "valor_mensual": denominador_mensual,
                **{mes: mensual_data[mes] for mes in meses_nombres},
                **trimestres,
                **semestres,
                "anual": anual
            }
            
            return item
            
        except Exception as e:
            print(f"      ✗ Error procesando mapping: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _find_all_mappings_for_column(
        self, 
        column_name: str, 
        column_mappings: Optional[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """🔥 MEJORADO: Encuentra TODOS los mappings + DEBUG"""
        if not column_mappings:
            return []
        
        all_mappings = []
        col_norm = normalize_text(column_name)
        
        print(f"      🔍 Buscando mappings para columna: '{column_name}'")
        print(f"         Normalizada: '{col_norm}'")
        
        for idx, mapping in enumerate(column_mappings.get("mappings", [])):
            mapping_col = mapping.get("columna_usuario", "")
            mapping_col_norm = normalize_text(mapping_col)
            
            # Comparación normalizada
            if col_norm == mapping_col_norm:
                consolidado = mapping.get('consolidado', {})
                edad_aplicable = consolidado.get('edad_aplicable', '')
                consulta_proc = consolidado.get('consulta_procedimiento', '')
                
                all_mappings.append(mapping)
                print(f"         [{len(all_mappings)}] Match #{idx}: Edad '{edad_aplicable}', Consulta '{consulta_proc[:50]}...'")
        
        print(f"      → Total mappings encontrados: {len(all_mappings)}")
        
        # 🔥 DEDUPLICACIÓN: Agrupar por edad + consulta
        unique_mappings = []
        seen_keys = set()
        
        for mapping in all_mappings:
            consolidado = mapping.get('consolidado', {})
            edad_aplicable = consolidado.get('edad_aplicable', '')
            consulta_proc = consolidado.get('consulta_procedimiento', '')
            
            # Crear clave única normalizada
            key = (normalize_text(edad_aplicable), normalize_text(consulta_proc))
            
            if key not in seen_keys:
                unique_mappings.append(mapping)
                seen_keys.add(key)
            else:
                print(f"      ⚠️ Duplicado omitido: Edad '{edad_aplicable}'")
        
        print(f"      ✅ Mappings únicos después de deduplicar: {len(unique_mappings)}")
        
        return unique_mappings


    
    def _filter_mappings_by_population_availability(
        self,
        all_mappings: List[Dict[str, Any]],
        data_source: str,
        where_clause: str,
        corte_fecha: str
    ) -> List[Dict[str, Any]]:
        """Filtra mappings verificando si hay población disponible"""
        print(f"      🔍 Filtrando {len(all_mappings)} mappings por población...")
        
        valid_mappings = []
        
        for mapping in all_mappings:
            consolidado_info = mapping.get('consolidado', {})
            edad_aplicable = consolidado_info.get('edad_aplicable', '')
            
            if not edad_aplicable:
                continue
            
            try:
                poblaciones = self._get_population_by_predefined_dates(
                    data_source=data_source,
                    where_clause=where_clause,
                    edad_key=edad_aplicable,
                    corte_fecha=corte_fecha
                )
                
                # Calcular total
                if -1 in poblaciones:
                    total_poblacion = poblaciones[-1]
                else:
                    total_poblacion = sum(poblaciones.values())
                
                if total_poblacion > 0:
                    print(f"         ✅ {edad_aplicable}: {total_poblacion} personas")
                    valid_mappings.append(mapping)
                else:
                    print(f"         ⊘ {edad_aplicable}: 0 personas (omitido)")
            
            except Exception as e:
                print(f"         ⚠️ Error verificando {edad_aplicable}: {e}")
                continue
        
        return valid_mappings
    
    def _get_numerador_by_month_with_age_filter(
        self,
        data_source: str,
        column_name: str,
        where_clause: str,
        edad_aplicable: str,
        anio_corte: int,
        mes_limite: int,
        corte_fecha: str
    ) -> Dict[int, int]:
        """Calcula numerador filtrando por edad_aplicable"""
        print(f"      🔢 Calculando numerador (con filtro de edad)")
        
        col_escaped = f'"{column_name}"'
        
        # Detectar formato de fecha
        format_detection_query = f"""
        SELECT CAST({col_escaped} AS VARCHAR) as fecha_str
        FROM {data_source}
        WHERE {where_clause}
          AND {col_escaped} IS NOT NULL
          AND CAST({col_escaped} AS VARCHAR) != ''
        LIMIT 1
        """
        
        try:
            sample = self.conn.execute(format_detection_query).fetchone()
            if sample:
                fecha_sample = sample[0]
                formato_detectado = 'yyyy-mm-dd' if '-' in fecha_sample else 'dd/mm/yyyy'
                print(f"         📅 Formato: {formato_detectado}")
            else:
                formato_detectado = 'yyyy-mm-dd'
        except:
            formato_detectado = 'yyyy-mm-dd'
        
        # Construir filtro de edad
        edad_filter = self._build_age_filter_for_numerador(
            data_source, edad_aplicable, corte_fecha
        )
        
        numeradores = {}
        
        for mes_num in range(1, 13):
            if mes_num > mes_limite:
                numeradores[mes_num] = 0
                continue
            
            if formato_detectado == 'yyyy-mm-dd':
                query = f"""
                SELECT COUNT(DISTINCT "Nro Identificación")
                FROM {data_source}
                WHERE {where_clause}
                  AND {col_escaped} IS NOT NULL
                  AND CAST({col_escaped} AS VARCHAR) != ''
                  AND CAST({col_escaped} AS VARCHAR) LIKE '____-__-__'
                  AND EXTRACT(MONTH FROM CAST(CAST({col_escaped} AS VARCHAR) AS DATE)) = {mes_num}
                  AND EXTRACT(YEAR FROM CAST(CAST({col_escaped} AS VARCHAR) AS DATE)) = {anio_corte}
                  AND {edad_filter}
                """
            else:
                query = f"""
                SELECT COUNT(DISTINCT "Nro Identificación")
                FROM {data_source}
                WHERE {where_clause}
                  AND {col_escaped} IS NOT NULL
                  AND CAST({col_escaped} AS VARCHAR) != ''
                  AND EXTRACT(MONTH FROM strptime(CAST({col_escaped} AS VARCHAR), '%d/%m/%Y')) = {mes_num}
                  AND EXTRACT(YEAR FROM strptime(CAST({col_escaped} AS VARCHAR), '%d/%m/%Y')) = {anio_corte}
                  AND {edad_filter}
                """
            
            try:
                result = self.conn.execute(query).fetchone()
                count = int(result[0]) if result else 0
                numeradores[mes_num] = count
                
                if count > 0:
                    print(f"         ✅ Mes {mes_num}: {count} atenciones")
            except Exception as e:
                print(f"         ⚠️ Error mes {mes_num}: {str(e)[:80]}")
                numeradores[mes_num] = 0
        
        total = sum(numeradores.values())
        print(f"         ✓ Total numerador: {total}")
        
        return numeradores
    
    def _build_age_filter_for_numerador(
        self, 
        data_source: str, 
        edad_aplicable: str,
        corte_fecha: str
    ) -> str:
        """Construye filtro SQL para edad en numerador"""
        
        # 🔥 NORMALIZAR edad_aplicable para búsqueda
        edad_norm = normalize_text(edad_aplicable)
        
        # Verificar si es rango compuesto (normalizado)
        if edad_norm in self.rangos_compuestos:
            rango_info = self.rangos_compuestos[edad_norm]
            print(f"         🎯 Rango compuesto: {edad_aplicable}")
            
            conditions = []
            
            # Filtro por meses
            if 'incluye_meses' in rango_info and rango_info['incluye_meses']:
                meses_list = rango_info['incluye_meses']
                edad_meses_expr = self.corrected_months.get_age_months_field_corrected(
                    data_source, corte_fecha
                )
                meses_str = ','.join(map(str, meses_list))
                conditions.append(f"({edad_meses_expr}) IN ({meses_str})")
            
            # Filtro por años
            if 'incluye_anios' in rango_info and rango_info['incluye_anios']:
                anios_list = rango_info['incluye_anios']
                edad_field = self.corrected_years.get_age_years_field_corrected(data_source)
                if edad_field:
                    anios_str = ','.join(map(str, anios_list))
                    conditions.append(f"CAST({edad_field} AS INTEGER) IN ({anios_str})")
            
            if conditions:
                return "(" + " OR ".join(conditions) + ")"
            else:
                return "1=1"
        
        # Si no es compuesto, usar lógica simple
        edad_lower = edad_aplicable.lower().strip()
        
        if 'mes' in edad_lower.replace('ñ', 'n'):
            import re
            match = re.search(r'(\d+)', edad_lower)
            if match:
                meses_valor = int(match.group(1))
                edad_meses_expr = self.corrected_months.get_age_months_field_corrected(
                    data_source, corte_fecha
                )
                return f"({edad_meses_expr}) = {meses_valor}"
        
        elif 'ano' in edad_lower or 'año' in edad_lower:
            import re
            match = re.search(r'(\d+)', edad_lower)
            if match:
                anios_valor = int(match.group(1))
                edad_field = self.corrected_years.get_age_years_field_corrected(data_source)
                if edad_field:
                    return f"CAST({edad_field} AS INTEGER) = {anios_valor}"
        
        return "1=1"
    
    def _normalize_age_key(self, edad_str: str) -> str:
        """🔥 MEJORADO: Normaliza clave con normalización de texto"""
        if not edad_str:
            return ""
        
        edad_norm = normalize_text(edad_str)
        
        for key in self.birth_date_ranges_den.keys():
            key_norm = normalize_text(key)
            if edad_norm == key_norm:
                return key
        
        return ""
    
    def _get_population_by_predefined_dates(
        self,
        data_source: str,
        where_clause: str,
        edad_key: str,
        corte_fecha: str = None
    ) -> Dict[int, int]:
        """
        🔥 MEJORADO: Soporta rangos compuestos con normalización
        """
        if not edad_key:
            print(f"   ⚠️ edad_key está vacío")
            return {i: 0 for i in range(1, 13)}
        
        # 🔥 NORMALIZAR Y VERIFICAR SI ES RANGO COMPUESTO
        edad_norm = normalize_text(edad_key)
        
        if edad_norm in self.rangos_compuestos:
            print(f"   🎯 Rango compuesto detectado: {edad_key}")
            return self._get_population_for_composite_range(
                data_source, where_clause, edad_norm, corte_fecha
            )
        
        # Detectar si es años
        edad_lower = edad_key.lower().strip()
        edad_sin_tildes = edad_lower.replace('ñ', 'n')
        
        print(f"   🔤 Edad recibida: '{edad_key}'")
        
        if 'ano' in edad_sin_tildes or 'año' in edad_lower:
            print(f"   🔍 Detectada palabra 'año' → ES AÑOS")
            import re
            match = re.search(r'(\d+)', edad_sin_tildes)
            if match:
                anios_valor = int(match.group(1))
                print(f"   🎯 ✅ DETECTADO: {anios_valor} años")
                return self._get_population_for_years(
                    data_source=data_source,
                    where_clause=where_clause,
                    anios=anios_valor,
                    corte_fecha=corte_fecha
                )
        
        # ES MESES
        print(f"   🎯 No contiene 'año' → ES MESES")
        
        edad_key_norm = self._normalize_age_key(edad_key)
        
        if not edad_key_norm:
            print(f"      ⚠️ No se pudo normalizar: '{edad_key}'")
            return {i: 0 for i in range(1, 13)}
        
        if edad_key_norm not in self.birth_date_ranges_den:
            print(f"      ⚠️ '{edad_key_norm}' no está en birth_date_ranges_den")
            return {i: 0 for i in range(1, 13)}
        
        print(f"      ✓ Usando rango: '{edad_key_norm}'")
        
        poblaciones = {}
        date_ranges = self.birth_date_ranges_den[edad_key_norm]
        
        for mes_num in range(1, 13):
            if mes_num not in date_ranges:
                poblaciones[mes_num] = 0
                continue
            
            fecha_inicio, fecha_fin = date_ranges[mes_num]
            
            query = f"""
            SELECT COUNT(DISTINCT "Nro Identificación")
            FROM {data_source}
            WHERE {where_clause}
              AND "Fecha Nacimiento" IS NOT NULL
              AND strptime("Fecha Nacimiento", '%d/%m/%Y') >= strptime('{fecha_inicio}', '%d/%m/%Y')
              AND strptime("Fecha Nacimiento", '%d/%m/%Y') <= strptime('{fecha_fin}', '%d/%m/%Y')
            """
            
            result = self.conn.execute(query).fetchone()
            poblaciones[mes_num] = int(result[0]) if result else 0
        
        total = sum(poblaciones.values())
        print(f"      ✓ Total población (meses): {total}")
        
        return poblaciones
    
    def _get_population_for_composite_range(
        self,
        data_source: str,
        where_clause: str,
        edad_key_norm: str,
        corte_fecha: str
    ) -> Dict[int, int]:
        """Calcula población para rangos compuestos (meses + años)"""
        
        rango_info = self.rangos_compuestos[edad_key_norm]
        
        print(f"      🔍 Procesando rango compuesto:")
        print(f"         Meses: {rango_info.get('incluye_meses', [])[:5] if rango_info.get('incluye_meses') else 'Ninguno'}...")
        print(f"         Años: {rango_info.get('incluye_anios', [])}")
        
        poblacion_total = 0
        
        # Sumar población de meses
        if 'incluye_meses' in rango_info and rango_info['incluye_meses']:
            meses_list = rango_info['incluye_meses']
            meses_str = ','.join(map(str, meses_list))
            
            edad_meses_expr = self.corrected_months.get_age_months_field_corrected(
                data_source, corte_fecha
            )
            
            query = f"""
            SELECT COUNT(DISTINCT "Nro Identificación")
            FROM {data_source}
            WHERE {where_clause}
              AND "Fecha Nacimiento" IS NOT NULL
              AND ({edad_meses_expr}) IN ({meses_str})
            """
            
            result = self.conn.execute(query).fetchone()
            pob_meses = int(result[0]) if result else 0
            poblacion_total += pob_meses
            
            print(f"         Población (meses): {pob_meses}")
        
        # Sumar población de años
        if 'incluye_anios' in rango_info and rango_info['incluye_anios']:
            anios_list = rango_info['incluye_anios']
            anios_str = ','.join(map(str, anios_list))
            
            edad_field = self.corrected_years.get_age_years_field_corrected(data_source)
            
            if edad_field:
                query = f"""
                SELECT COUNT(DISTINCT "Nro Identificación")
                FROM {data_source}
                WHERE {where_clause}
                  AND "Fecha Nacimiento" IS NOT NULL
                  AND CAST({edad_field} AS INTEGER) IN ({anios_str})
                """
                
                result = self.conn.execute(query).fetchone()
                pob_anios = int(result[0]) if result else 0
                poblacion_total += pob_anios
                
                print(f"         Población (años): {pob_anios}")
        
        print(f"      ✅ Población total (compuesto): {poblacion_total}")
        
        return {-1: poblacion_total}
    
    def _get_population_for_years(
        self,
        data_source: str,
        where_clause: str,
        anios: int,
        corte_fecha: str
    ) -> Dict[int, int]:
        """Calcula población para AÑOS usando columna Edad"""
        
        print(f"      🔢 BUSCANDO POBLACIÓN DE {anios} AÑOS")
        
        edad_field = self.corrected_years.get_age_years_field_corrected(data_source)
        
        if not edad_field:
            print(f"      ❌ No se pudo obtener columna de edad")
            return {-1: 0}
        
        document_field = self.identity_document.get_document_field(data_source)
        
        query = f"""
        SELECT COUNT(DISTINCT {document_field})
        FROM {data_source}
        WHERE {where_clause}
          AND "Fecha Nacimiento" IS NOT NULL
          AND {edad_field} IS NOT NULL
          AND CAST({edad_field} AS INTEGER) = {anios}
        """
        
        try:
            result = self.conn.execute(query).fetchone()
            count = int(result[0]) if result else 0
            
            print(f"      ✅ POBLACIÓN {anios} AÑOS: {count}")
            
            return {-1: count}
        
        except Exception as e:
            print(f"      ❌ Error: {e}")
            return {-1: 0}
    
    def _calculate_trimestres(self, mensual_data, meses_nombres, mes_limite):
        """Calcula trimestres"""
        trimestres = {}
        trimestre_meses = {
            "T1": meses_nombres[0:3],
            "T2": meses_nombres[3:6],
            "T3": meses_nombres[6:9],
            "T4": meses_nombres[9:12]
        }
        
        for trim_key, meses in trimestre_meses.items():
            meses_validos = [m for m in meses if meses_nombres.index(m) + 1 <= mes_limite]
            
            if meses_validos:
                pob = sum(mensual_data[m]["poblacion_objeto"] for m in meses_validos)
                num = sum(mensual_data[m]["numerador"] for m in meses_validos)
                den = sum(mensual_data[m]["denominador"] for m in meses_validos)
                cob = (num / den * 100) if den > 0 else 0
                semaf, color = self._calcular_semaforizacion(cob)
            else:
                pob = num = den = cob = 0
                semaf, color = "Sin datos", "#6c757d"
            
            trimestres[trim_key] = {
                "poblacion_objeto": pob,
                "numerador": num,
                "denominador": den,
                "cobertura": round(cob, 2),
                "semaforizacion": semaf,
                "color": color
            }
        
        return trimestres
    
    def _calculate_semestres(self, trimestres):
        """Calcula semestres"""
        semestres = {}
        
        for sem_key, trims in [("S1", ["T1", "T2"]), ("S2", ["T3", "T4"])]:
            pob = sum(trimestres[t]["poblacion_objeto"] for t in trims)
            num = sum(trimestres[t]["numerador"] for t in trims)
            den = sum(trimestres[t]["denominador"] for t in trims)
            cob = (num / den * 100) if den > 0 else 0
            semaf, color = self._calcular_semaforizacion(cob)
            
            semestres[sem_key] = {
                "poblacion_objeto": pob,
                "numerador": num,
                "denominador": den,
                "cobertura": round(cob, 2),
                "semaforizacion": semaf,
                "color": color
            }
        
        return semestres
    
    def _calculate_anual(self, semestres):
        """Calcula anual"""
        pob = sum(semestres[s]["poblacion_objeto"] for s in ["S1", "S2"])
        num = sum(semestres[s]["numerador"] for s in ["S1", "S2"])
        den = sum(semestres[s]["denominador"] for s in ["S1", "S2"])
        cob = (num / den * 100) if den > 0 else 0
        semaf, color = self._calcular_semaforizacion(cob)
        
        return {
            "poblacion_objeto": pob,
            "numerador": num,
            "denominador": den,
            "cobertura": round(cob, 2),
            "semaforizacion": semaf,
            "color": color
        }
    
    def _calcular_semaforizacion(self, cobertura: float) -> tuple:
        """Calcula semaforización"""
        if cobertura >= 95:
            return ('Óptimo', '#52c41a')
        elif cobertura >= 80:
            return ('Aceptable', '#faad14')
        elif cobertura >= 60:
            return ('Deficiente', '#fa8c16')
        else:
            return ('Muy Deficiente', '#ff4d4f')
    
    def _empty_report(self, filename, corte_fecha, keywords, geographic_filters):
        """Construye reporte vacío"""
        return {
            'success': True,
            'filename': filename,
            'corte_fecha': corte_fecha,
            'keywords': keywords or [],
            'geographic_filters': geographic_filters or {},
            'data': [],
            'total_rows': 0,
            'message': 'No se encontraron columnas o sin filtros geográficos'
        }
