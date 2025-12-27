# services/technical_note_services/nt_rpms_integration.py

from typing import Optional, Dict, Any, List, Tuple
from services.duckdb_service.duckdb_service import duckdb_service
from utils.text_normalizer import normalize_text


class NTRPMSIntegration:
    """Integración con datos consolidados de NT RPMS con validación de servicios habilitados y matching por similitud"""
    
    def __init__(self, data_source: str):
        """
        Inicializa integración con NT RPMS
        
        Args:
            data_source: Fuente de datos (tabla, vista o read_parquet(...))
        """
        self.data_source = data_source
        self.table_name = None
        self.similarity_threshold = 0.70  # 70% de similitud mínima
        
        print(f"✓ NT_RPMS Integration inicializando...")
        print(f"   data_source recibido: {data_source[:80]}...")
        
        # Detectar si es read_parquet() y crear vista temporal
        if 'read_parquet' in data_source.lower():
            print(f"   🔍 Detectado formato read_parquet(), creando vista temporal...")
            import hashlib
            import random
            temp_suffix = hashlib.md5(str(random.random()).encode()).hexdigest()[:8]
            self.table_name = f"nt_rpms_temp_{temp_suffix}"
            
            try:
                create_view_sql = f"CREATE OR REPLACE TEMP VIEW {self.table_name} AS SELECT * FROM {data_source}"
                duckdb_service.conn.execute(create_view_sql)
                print(f"   ✅ Vista temporal creada: {self.table_name}")
            except Exception as e:
                print(f"   ⚠️ Error creando vista temporal: {e}")
                self.table_name = data_source
        else:
            self.table_name = data_source
        
        # Verificar que la tabla tiene datos
        try:
            count_query = f"SELECT COUNT(*) FROM {self.table_name}"
            total_rows = duckdb_service.conn.execute(count_query).fetchone()[0]
            print(f"   ✅ Tabla NT_RPMS: {total_rows:,} filas")
        except Exception as e:
            print(f"   ⚠️ Error verificando NT_RPMS: {e}")
        
        # Detectar columna de habilitación
        self.habilitado_column = self._detect_habilitado_column()
    
    def _detect_habilitado_column(self) -> Optional[str]:
        """
        Detecta columna que indica si el servicio está habilitado.
        
        Prioriza "servicios_habilitados" y busca alternativas.
        
        Returns:
            Nombre de la columna o None si no existe
        """
        try:
            describe_query = f"DESCRIBE SELECT * FROM {self.table_name}"
            columns_result = duckdb_service.conn.execute(describe_query).fetchall()
            cols = [row[0] for row in columns_result]
            cols_norm = {normalize_text(c): c for c in cols}
            
            # Candidatos ordenados por prioridad
            candidates = [
                'servicios_habilitados',
                'Servicios_Habilitados',
                'SERVICIOS_HABILITADOS',
                'Servicios Habilitados',
                'Habilitado',
                'HABILITADO',
                'habilitado',
                'Estado',
                'ESTADO',
                'Activo',
                'ACTIVO',
                'Enabled',
                'ENABLED'
            ]
            
            for cand in candidates:
                if normalize_text(cand) in cols_norm:
                    column_name = cols_norm[normalize_text(cand)]
                    print(f"   ✓ Columna de habilitación detectada: '{column_name}'")
                    return column_name
            
            print(f"   ⚠️ No se detectó columna de habilitación (se asumirá todo habilitado)")
            return None
        
        except Exception as e:
            print(f"   ⚠️ Error detectando columna habilitado: {e}")
            return None
    
    def find_matching_row(
        self,
        consulta_procedimiento: str,
        edad_nt_rpms: str,
        departamento: str = None,
        municipio: str = None,
        nombre_ips: str = None
    ) -> Optional[Dict[str, Any]]:
        """
        Busca fila que coincida con consulta/procedimiento y edad usando:
        1. Coincidencia exacta (prioridad)
        2. Coincidencia por similitud si no hay exacta (70% mínimo)
        
        IMPORTANTE: 
        - Si se especifican filtros geográficos y NO se encuentra match (ni exacto ni similar) → DESHABILITADO
        - Si no se especifican filtros geográficos → busca sin ellos
        - Verifica si el servicio está habilitado (columna "servicios_habilitados" = 1)
        
        Args:
            consulta_procedimiento: Nombre de consulta/procedimiento
            edad_nt_rpms: Edad en formato NT_RPMS (ej: "12, 14 y 16 Años")
            departamento: Filtro por departamento (opcional)
            municipio: Filtro por municipio (opcional)
            nombre_ips: Filtro por nombre de IPS (opcional)
        
        Returns:
            - Dict con datos RPMS si está habilitado
            - Dict con {'habilitado': False, ...} si está deshabilitado
            - None si no se encuentra
        """
        try:
            print(f"\n      🔍 BUSCANDO EN NT_RPMS (CON MATCHING POR SIMILITUD):")
            print(f"         Consulta: '{consulta_procedimiento[:60]}...'")
            print(f"         Edad NT_RPMS: '{edad_nt_rpms}'")
            print(f"         Filtros geo: Depto={departamento}, Muni={municipio}, IPS={nombre_ips}")
            
            # Normalizar todos los textos para comparación
            consulta_norm = normalize_text(consulta_procedimiento)
            edad_norm = normalize_text(edad_nt_rpms)
            
            # Normalizar filtros geográficos
            depto_norm = normalize_text(departamento) if departamento else None
            muni_norm = normalize_text(municipio) if municipio else None
            ips_norm = normalize_text(nombre_ips) if nombre_ips else None
            
            print(f"      📝 Textos normalizados:")
            print(f"         Consulta: '{consulta_norm[:60]}...'")
            print(f"         Edad: '{edad_norm}'")
            if depto_norm:
                print(f"         Depto: '{depto_norm}'")
            if muni_norm:
                print(f"         Muni: '{muni_norm}'")
            if ips_norm:
                print(f"         IPS: '{ips_norm}'")
            
            # Determinar si se especificaron filtros geográficos
            has_geo_filters = bool(depto_norm or muni_norm or ips_norm)
            
            # PASO 1: Intentar coincidencia EXACTA con filtros geográficos
            if has_geo_filters:
                print(f"      🎯 Intentando coincidencia EXACTA con filtros geográficos...")
                result = self._query_rpms_exact_match(
                    consulta_norm, edad_norm, depto_norm, muni_norm, ips_norm
                )
                
                if result:
                    print(f"      ✅ Match EXACTO encontrado")
                    return self._process_rpms_result(
                        result, consulta_procedimiento, edad_nt_rpms, 
                        departamento, municipio, nombre_ips, True, 100.0
                    )
                
                # PASO 2: Intentar coincidencia POR SIMILITUD con filtros geográficos
                print(f"      🔍 No hay match exacto, intentando por SIMILITUD (≥{self.similarity_threshold*100:.0f}%)...")
                best_match, similarity_score = self._query_rpms_similarity_match(
                    consulta_norm, edad_norm, depto_norm, muni_norm, ips_norm
                )
                
                if best_match and similarity_score >= self.similarity_threshold:
                    print(f"      ✅ Match por SIMILITUD encontrado ({similarity_score*100:.1f}% coincidencia)")
                    return self._process_rpms_result(
                        best_match, consulta_procedimiento, edad_nt_rpms,
                        departamento, municipio, nombre_ips, True, similarity_score
                    )
                
                # NO SE ENCONTRÓ NI EXACTO NI POR SIMILITUD → DESHABILITADO
                print(f"      ❌ No se encontró servicio para la geografía especificada")
                print(f"      🚫 SERVICIO DESHABILITADO para esta ubicación geográfica")
                return {
                    'habilitado': False,
                    'consulta_procedimiento': consulta_procedimiento,
                    'edad_nt_rpms': edad_nt_rpms,
                    'departamento': departamento,
                    'municipio': municipio,
                    'nombre_ips': nombre_ips,
                    'cups': '',
                    'periodo': 'ANUAL',
                    'meta': 0,
                    'frecuencia_indicada': 0,
                    'proyeccion_tiempo': 0
                }
            
            # PASO 3: Si NO se especificaron filtros geográficos, buscar sin ellos
            print(f"      🔍 Buscando SIN filtros geográficos...")
            result_sin_geo = self._query_rpms_exact_match(consulta_norm, edad_norm, None, None, None)
            
            if result_sin_geo:
                return self._process_rpms_result(
                    result_sin_geo, consulta_procedimiento, edad_nt_rpms,
                    departamento, municipio, nombre_ips, False, 100.0
                )
            
            print(f"      ❌ No se encontró ningún match")
            return None
            
        except Exception as e:
            print(f"      ⚠️ Error buscando en NT_RPMS: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _query_rpms_exact_match(
        self,
        consulta_norm: str,
        edad_norm: str,
        depto_norm: Optional[str],
        muni_norm: Optional[str],
        ips_norm: Optional[str]
    ) -> Optional[tuple]:
        """Query con coincidencia EXACTA"""
        conditions = []
        
        # Filtro de consulta (normalizado)
        conditions.append(self._build_normalized_condition('consultas_procedimientos', consulta_norm))
        
        # Filtro de edad (normalizado)
        conditions.append(self._build_normalized_condition('frecuencia_edad', edad_norm))
        
        # Filtros geográficos (si se especificaron)
        if depto_norm:
            conditions.append(self._build_normalized_condition('departamento', depto_norm))
        
        if muni_norm:
            conditions.append(self._build_normalized_condition('municipio', muni_norm))
        
        if ips_norm:
            conditions.append(self._build_normalized_condition('nombre_ips', ips_norm))
        
        # Construir SELECT con columna de habilitación si existe
        select_fields = self._get_select_fields()
        select_clause = ', '.join(select_fields)
        
        # Construir query completa
        query = f"""
        SELECT {select_clause}
        FROM {self.table_name}
        WHERE {" AND ".join(conditions)}
        LIMIT 1
        """
        
        try:
            return duckdb_service.conn.execute(query).fetchone()
        except Exception as e:
            print(f"      ⚠️ Error en query exacta: {e}")
            return None
    
    def _query_rpms_similarity_match(
        self,
        consulta_norm: str,
        edad_norm: str,
        depto_norm: Optional[str],
        muni_norm: Optional[str],
        ips_norm: Optional[str]
    ) -> Tuple[Optional[tuple], float]:
        """
        Query con coincidencia por SIMILITUD.
        Retorna el mejor match y su score de similitud.
        """
        # Filtros obligatorios (consulta y edad deben coincidir exactamente)
        base_conditions = [
            self._build_normalized_condition('consultas_procedimientos', consulta_norm),
            self._build_normalized_condition('frecuencia_edad', edad_norm)
        ]
        
        # Construir query que traiga TODOS los candidatos que coincidan en consulta y edad
        # pero sin filtrar por geografía (para calcular similitud)
        select_fields = self._get_select_fields()
        select_clause = ', '.join(select_fields)
        
        query = f"""
        SELECT {select_clause}
        FROM {self.table_name}
        WHERE {" AND ".join(base_conditions)}
        """
        
        try:
            candidates = duckdb_service.conn.execute(query).fetchall()
            
            if not candidates:
                return None, 0.0
            
            print(f"      📋 Encontrados {len(candidates)} candidatos para matching por similitud")
            
            # Calcular similitud para cada candidato
            best_match = None
            best_score = 0.0
            
            for candidate in candidates:
                # Extraer valores geográficos del candidato
                candidate_depto = normalize_text(str(candidate[8])) if candidate[8] else ""
                candidate_muni = normalize_text(str(candidate[9])) if candidate[9] else ""
                candidate_ips = normalize_text(str(candidate[7])) if candidate[7] else ""
                
                # Calcular score de similitud
                score = self._calculate_geo_similarity(
                    depto_norm, muni_norm, ips_norm,
                    candidate_depto, candidate_muni, candidate_ips
                )
                
                print(f"         Candidato: Depto='{candidate_depto[:20]}', Muni='{candidate_muni[:20]}', IPS='{candidate_ips[:30]}' → Similitud: {score*100:.1f}%")
                
                if score > best_score:
                    best_score = score
                    best_match = candidate
            
            if best_match:
                print(f"      🎯 Mejor match: {best_score*100:.1f}% de similitud")
            
            return best_match, best_score
            
        except Exception as e:
            print(f"      ⚠️ Error en query por similitud: {e}")
            return None, 0.0
    
    def _calculate_geo_similarity(
        self,
        depto_input: Optional[str],
        muni_input: Optional[str],
        ips_input: Optional[str],
        depto_candidate: str,
        muni_candidate: str,
        ips_candidate: str
    ) -> float:
        """
        Calcula score de similitud geográfica (0.0 a 1.0).
        
        Estrategia:
        1. Departamento y Municipio deben coincidir exactamente (si se especificaron)
        2. IPS usa coincidencia por tokens compartidos (palabras clave)
        """
        scores = []
        
        # DEPARTAMENTO: Debe coincidir exactamente si se especificó
        if depto_input:
            if depto_input == depto_candidate:
                scores.append(1.0)
            else:
                return 0.0  # Si departamento no coincide, score = 0
        
        # MUNICIPIO: Debe coincidir exactamente si se especificó
        if muni_input:
            if muni_input == muni_candidate:
                scores.append(1.0)
            else:
                return 0.0  # Si municipio no coincide, score = 0
        
        # IPS: Coincidencia por tokens compartidos
        if ips_input:
            ips_score = self._calculate_text_similarity(ips_input, ips_candidate)
            scores.append(ips_score)
        
        # Score promedio
        return sum(scores) / len(scores) if scores else 0.0
    
    def _calculate_text_similarity(self, text1: str, text2: str) -> float:
        """
        Calcula similitud entre dos textos basándose en tokens compartidos.
        
        Ejemplo:
        - text1: "E.S.E. HOSPITAL SAN RAFAEL"
        - text2: "E. S. E. HOSPITAL SAN RAFAEL DE LETICIA"
        - Tokens1: {E, S, E, HOSPITAL, SAN, RAFAEL} = 5 únicos
        - Tokens2: {E, S, E, HOSPITAL, SAN, RAFAEL, DE, LETICIA} = 7 únicos
        - Compartidos: {E, S, HOSPITAL, SAN, RAFAEL} = 5
        - Score: 5 / max(5, 7) = 5/7 = 0.714 (71.4%)
        """
        # Tokenizar: separar por espacios y eliminar puntos/caracteres especiales
        tokens1 = set(self._tokenize(text1))
        tokens2 = set(self._tokenize(text2))
        
        # Tokens compartidos
        common_tokens = tokens1 & tokens2
        
        # Score: tokens compartidos / total de tokens únicos en el más largo
        if not tokens1 or not tokens2:
            return 0.0
        
        max_tokens = max(len(tokens1), len(tokens2))
        score = len(common_tokens) / max_tokens
        
        return score
    
    def _tokenize(self, text: str) -> List[str]:
        """
        Tokeniza texto: elimina puntos, comas, guiones y divide por espacios.
        Filtra tokens muy cortos (< 2 caracteres).
        """
        import re
        # Remover puntos, comas, guiones, paréntesis
        cleaned = re.sub(r'[.,\-():/]', ' ', text)
        # Dividir por espacios
        tokens = cleaned.split()
        # Filtrar tokens cortos y convertir a mayúsculas
        return [t.upper() for t in tokens if len(t) >= 2]
    
    def _get_select_fields(self) -> List[str]:
        """Construye lista de campos SELECT incluyendo columna de habilitación"""
        select_fields = [
            'consultas_procedimientos',
            'frecuencia_edad',
            'meta',
            'frecuencia_indicada',
            'cups',
            'periodo',
            'proyeccion_tiempo',
            'nombre_ips',
            'departamento',
            'municipio'
        ]
        
        if self.habilitado_column:
            select_fields.append(f'"{self.habilitado_column}"')
        
        return select_fields
    
    def _build_normalized_condition(self, column_name: str, normalized_value: str) -> str:
        """Construye condición SQL con normalización de texto"""
        return f"""
            UPPER(
                TRIM(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        regexp_replace(
                                            CAST({column_name} AS VARCHAR),
                                            '[áàäâ]', 'A', 'g'
                                        ),
                                        '[éèëê]', 'E', 'g'
                                    ),
                                    '[íìïî]', 'I', 'g'
                                ),
                                '[óòöô]', 'O', 'g'
                            ),
                            '[úùüû]', 'U', 'g'
                        ),
                        '[ñÑ]', 'N', 'g'
                    )
                )
            ) = '{normalized_value}'
        """
    
    def _process_rpms_result(
        self,
        result: tuple,
        consulta_procedimiento: str,
        edad_nt_rpms: str,
        departamento: Optional[str],
        municipio: Optional[str],
        nombre_ips: Optional[str],
        with_geo: bool,
        similarity_score: float
    ) -> Dict[str, Any]:
        """
        Procesa resultado de query y verifica si el servicio está habilitado
        
        Returns:
            Dict con datos RPMS o dict especial si está deshabilitado
        """
        # Mapear resultado base
        result_dict = {
            'consultas_procedimientos': result[0],
            'frecuencia_edad': result[1],
            'meta': float(result[2]) if result[2] else 0.0,
            'frecuencia_indicada': float(result[3]) if result[3] else 0.0,
            'cups': result[4],
            'periodo': result[5],
            'proyeccion_tiempo': int(result[6]) if result[6] else 12,
            'nombre_ips': result[7],
            'departamento': result[8],
            'municipio': result[9],
            'similarity_score': similarity_score  # Agregar score
        }
        
        # VERIFICAR SI ESTÁ HABILITADO
        habilitado = 1  # Default
        
        if self.habilitado_column and len(result) > 10:
            habilitado_val = result[10]
            # Normalizar a 0 o 1
            habilitado = 1 if habilitado_val in [1, '1', 'SI', 'si', 'Sí', True] else 0
            result_dict['habilitado'] = habilitado
        else:
            result_dict['habilitado'] = 1
        
        # SI ESTÁ DESHABILITADO, DEVOLVER DICT ESPECIAL
        if habilitado == 0:
            print(f"      ⚠️ SERVICIO DESHABILITADO (servicios_habilitados = 0)")
            print(f"      💡 Numerador y denominador serán 0, semaforización NA")
            return {
                'habilitado': False,
                'consulta_procedimiento': consulta_procedimiento,
                'edad_nt_rpms': edad_nt_rpms,
                'departamento': departamento,
                'municipio': municipio,
                'nombre_ips': nombre_ips,
                'similarity_score': similarity_score,
                **result_dict
            }
        
        # Servicio habilitado - devolver datos normales
        if with_geo:
            if similarity_score >= 1.0:
                print(f"      ✅ Match EXACTO CON filtros geográficos")
            else:
                print(f"      ✅ Match por SIMILITUD ({similarity_score*100:.1f}%) CON filtros geográficos")
                print(f"         IPS en RPMS: '{result[7]}'")
                print(f"         IPS buscada: '{nombre_ips}'")
        else:
            print(f"      ✅ Match encontrado SIN filtros geográficos")
        
        print(f"      ✅ Servicio HABILITADO (servicios_habilitados = 1)")
        
        return result_dict
    
    def calculate_extended_metrics(
        self,
        poblacion_objeto: int,
        nt_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Calcula métricas extendidas basadas en datos de NT_RPMS
        
        Args:
            poblacion_objeto: Población objetivo (conteo de personas)
            nt_data: Datos de NT_RPMS (meta, frecuencia, proyección)
        
        Returns:
            Dict con métricas calculadas
        """
        try:
            # Si el servicio está deshabilitado, devolver todo en 0
            if not nt_data.get('habilitado', True):
                return {
                    'poblacion_susceptible': 0,
                    'valor_mensual': 0,
                    'meta': 0.0,
                    'frecuencia_uso': 0.0,
                    'proyeccion_tiempo': 0
                }
            
            meta = nt_data.get('meta', 0.0)
            frecuencia = nt_data.get('frecuencia_indicada', 1.0)
            proyeccion = nt_data.get('proyeccion_tiempo', 12)
            
            # Población susceptible = población * meta
            poblacion_susceptible = poblacion_objeto * meta
            
            # Denominador mensual = (población susceptible * frecuencia) / proyección
            if proyeccion > 0:
                denominador_mensual = (poblacion_susceptible * frecuencia) / proyeccion
            else:
                denominador_mensual = 0
            
            return {
                'poblacion_susceptible': round(poblacion_susceptible, 2),
                'valor_mensual': round(denominador_mensual, 0),
                'meta': meta,
                'frecuencia_uso': nt_data.get('frecuencia_uso', 0.0),
                'proyeccion_tiempo': proyeccion
            }
            
        except Exception as e:
            print(f"Error calculando métricas: {e}")
            return {
                'poblacion_susceptible': 0,
                'valor_mensual': 0,
                'meta': 0.0,
                'frecuencia_uso': 0.0,
                'proyeccion_tiempo': 12
            }
    
    def __del__(self):
        """Limpia vista temporal al destruir objeto"""
        if self.table_name and 'temp' in self.table_name:
            try:
                drop_query = f"DROP VIEW IF EXISTS {self.table_name}"
                duckdb_service.conn.execute(drop_query)
            except:
                pass
