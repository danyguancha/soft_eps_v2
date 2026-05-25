# services/technical_note_services/nt_rpms_integration.py

from typing import Optional, Dict, Any, List, Tuple
from services.duckdb_service.duckdb_service import duckdb_service
from utils.text_normalizer import normalize_text


# ── Helper global: parsea float con coma O punto decimal y maneja porcentajes ─
def _parse_float(value, default: float = 0.0) -> float:
    """
    Convierte a float tolerando:
      - Formato español con coma decimal : '119,4666' → 119.4666
      - Punto decimal estándar           : '107.33'   → 107.33
      - Porcentaje Excel (display value) : '70%'      → 0.70
                                           '25,94%'   → 0.2594
      - None / vacío / NULL              : → default (0.0)
    """
    if value in (None, '', 'NULL', 'null'):
        return default
    try:
        s = str(value).strip().replace(',', '.')
        # ── Porcentaje: quitar '%' y dividir por 100 ─────────────────
        if s.endswith('%'):
            return float(s[:-1]) / 100.0
        return float(s)
    except (ValueError, TypeError):
        return default


def _parse_int(value, default: int = 0) -> int:
    """Convierte a int tolerando coma decimal y porcentaje (trunca decimal)."""
    return int(_parse_float(value, float(default)))


class NTRPMSIntegration:
    """Integración con datos consolidados de NT RPMS con validación de servicios habilitados y matching por similitud"""

    def __init__(self, data_source: str):
        self.data_source = data_source
        self.table_name = None
        self.similarity_threshold = 0.70

        print(f"✓ NT_RPMS Integration inicializando...")
        print(f"   data_source recibido: {data_source[:80]}...")

        if 'read_parquet' in data_source.lower():
            print(f"   🔍 Detectado formato read_parquet(), creando vista temporal...")
            import hashlib, random
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

        try:
            count_query = f"SELECT COUNT(*) FROM {self.table_name}"
            total_rows = duckdb_service.conn.execute(count_query).fetchone()[0]
            print(f"   ✅ Tabla NT_RPMS: {total_rows:,} filas")
        except Exception as e:
            print(f"   ⚠️ Error verificando NT_RPMS: {e}")

        self.habilitado_column = self._detect_habilitado_column()
        self.historico_column  = self._detect_historico_column()
        self.atenciones_column = self._detect_atenciones_column()

    # ============================================================
    # DETECCIÓN DE COLUMNAS
    # ============================================================

    def _detect_habilitado_column(self) -> Optional[str]:
        try:
            cols = [row[0] for row in duckdb_service.conn.execute(
                f"DESCRIBE SELECT * FROM {self.table_name}").fetchall()]
            cols_norm = {normalize_text(c): c for c in cols}

            candidates = [
                'servicios_habilitados', 'Servicios_Habilitados', 'SERVICIOS_HABILITADOS',
                'Servicios Habilitados', 'Habilitado', 'HABILITADO', 'habilitado',
                'Estado', 'ESTADO', 'Activo', 'ACTIVO', 'Enabled', 'ENABLED'
            ]
            for cand in candidates:
                if normalize_text(cand) in cols_norm:
                    col = cols_norm[normalize_text(cand)]
                    print(f"   ✓ Columna de habilitación detectada: '{col}'")
                    return col

            print(f"   ⚠️ No se detectó columna de habilitación (se asumirá todo habilitado)")
            return None
        except Exception as e:
            print(f"   ⚠️ Error detectando columna habilitado: {e}")
            return None

    def _detect_historico_column(self) -> Optional[str]:
        try:
            cols = [row[0] for row in duckdb_service.conn.execute(
                f"DESCRIBE SELECT * FROM {self.table_name}").fetchall()]
            cols_norm = {normalize_text(c): c for c in cols}

            candidates = [
                'intervenciones_realizadas', 'Intervenciones_Realizadas',
                'Intervenciones Realizadas', 'INTERVENCIONES_REALIZADAS',
                'historico', 'Histórico', 'Historico', 'HISTORICO',
                'intervenciones', 'Intervenciones', 'realizadas', 'Realizadas'
            ]
            for cand in candidates:
                if normalize_text(cand) in cols_norm:
                    col = cols_norm[normalize_text(cand)]
                    print(f"   ✓ Columna de histórico detectada: '{col}'")
                    return col

            for col in cols:
                col_norm = normalize_text(col)
                if 'intervencion' in col_norm or 'historico' in col_norm or 'realizada' in col_norm:
                    print(f"   ✓ Columna de histórico detectada (parcial): '{col}'")
                    return col

            print(f"   ⚠️ No se detectó columna de histórico (intervenciones_realizadas = 0)")
            return None
        except Exception as e:
            print(f"   ⚠️ Error detectando columna histórico: {e}")
            return None

    def _detect_atenciones_column(self) -> Optional[str]:
        try:
            cols = [row[0] for row in duckdb_service.conn.execute(
                f"DESCRIBE SELECT * FROM {self.table_name}").fetchall()]
            cols_norm = {normalize_text(c): c for c in cols}

            candidates = [
                'atenciones_realizar_anual',
                'Atenciones_Realizar_Anual',
                'Atenciones Realizar Anual',
                'ATENCIONES_REALIZAR_ANUAL',
                'atenciones_a_realizar',
                'Atenciones a Realizar',
                'Atenciones_a_Realizar',
                'atenciones_anual',
                'Atenciones Anual',
                'total_atenciones',
                'Total Atenciones',
                'Total_Atenciones',
            ]
            for cand in candidates:
                if normalize_text(cand) in cols_norm:
                    col = cols_norm[normalize_text(cand)]
                    print(f"   ✓ Columna atenciones_realizar_anual detectada: '{col}'")
                    return col

            for col in cols:
                col_norm = normalize_text(col)
                if 'atencion' in col_norm and ('realizar' in col_norm or 'anual' in col_norm):
                    print(f"   ✓ Columna atenciones_realizar_anual detectada (parcial): '{col}'")
                    return col

            print(f"   ⚠️ No se detectó columna atenciones_realizar_anual (usará 0)")
            return None
        except Exception as e:
            print(f"   ⚠️ Error detectando columna atenciones: {e}")
            return None

    # ============================================================
    # BÚSQUEDA PRINCIPAL
    # ============================================================

    def find_matching_row(
        self,
        consulta_procedimiento: str,
        edad_nt_rpms: str,
        departamento: str = None,
        municipio: str = None,
        nombre_ips: str = None
    ) -> Optional[Dict[str, Any]]:
        try:
            print(f"\n      🔍 BUSCANDO EN NT_RPMS (CON MATCHING POR SIMILITUD):")
            print(f"         Consulta: '{consulta_procedimiento[:60]}...'")
            print(f"         Edad NT_RPMS: '{edad_nt_rpms}'")
            print(f"         Filtros geo: Depto={departamento}, Muni={municipio}, IPS={nombre_ips}")

            consulta_norm = normalize_text(consulta_procedimiento)
            edad_norm     = normalize_text(edad_nt_rpms)
            depto_norm    = normalize_text(departamento) if departamento else None
            muni_norm     = normalize_text(municipio)    if municipio    else None
            ips_norm      = normalize_text(nombre_ips)   if nombre_ips   else None

            print(f"      📝 Textos normalizados:")
            print(f"         Consulta: '{consulta_norm[:60]}...'")
            print(f"         Edad: '{edad_norm}'")
            if depto_norm: print(f"         Depto: '{depto_norm}'")
            if muni_norm:  print(f"         Muni: '{muni_norm}'")
            if ips_norm:   print(f"         IPS: '{ips_norm}'")

            has_geo_filters = bool(depto_norm or muni_norm or ips_norm)

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

                print(f"      🔍 No hay match exacto, intentando por SIMILITUD (≥{self.similarity_threshold*100:.0f}%)...")
                best_match, sim_score = self._query_rpms_similarity_match(
                    consulta_norm, edad_norm, depto_norm, muni_norm, ips_norm
                )
                if best_match and sim_score >= self.similarity_threshold:
                    print(f"      ✅ Match por SIMILITUD encontrado ({sim_score*100:.1f}% coincidencia)")
                    return self._process_rpms_result(
                        best_match, consulta_procedimiento, edad_nt_rpms,
                        departamento, municipio, nombre_ips, True, sim_score
                    )

                print(f"      ❌ No se encontró servicio para la geografía especificada")
                print(f"      🚫 SERVICIO DESHABILITADO para esta ubicación geográfica")
                return {
                    'habilitado':                False,
                    'consulta_procedimiento':    consulta_procedimiento,
                    'edad_nt_rpms':              edad_nt_rpms,
                    'departamento':              departamento,
                    'municipio':                 municipio,
                    'nombre_ips':                nombre_ips,
                    'cups':                      '',
                    'periodo':                   'ANUAL',
                    'meta':                      0,
                    'frecuencia_indicada':       0,
                    'proyeccion_tiempo':         0,
                    'atenciones_realizar_anual': 0
                }

            print(f"      🔍 Buscando SIN filtros geográficos...")
            result_sin_geo = self._query_rpms_exact_match(
                consulta_norm, edad_norm, None, None, None
            )
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

    # ============================================================
    # QUERIES
    # ============================================================

    def _query_rpms_exact_match(
        self,
        consulta_norm: str,
        edad_norm: str,
        depto_norm: Optional[str],
        muni_norm: Optional[str],
        ips_norm: Optional[str]
    ) -> Optional[tuple]:
        conditions = [
            self._build_normalized_condition('consultas_procedimientos', consulta_norm),
            self._build_normalized_condition('frecuencia_edad', edad_norm)
        ]
        if depto_norm: conditions.append(self._build_normalized_condition('departamento', depto_norm))
        if muni_norm:  conditions.append(self._build_normalized_condition('municipio', muni_norm))
        if ips_norm:   conditions.append(self._build_normalized_condition('nombre_ips', ips_norm))

        select_clause = ', '.join(self._get_select_fields())
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
        base_conditions = [
            self._build_normalized_condition('consultas_procedimientos', consulta_norm),
            self._build_normalized_condition('frecuencia_edad', edad_norm)
        ]
        select_clause = ', '.join(self._get_select_fields())
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

            best_match, best_score = None, 0.0
            for candidate in candidates:
                candidate_depto = normalize_text(str(candidate[8])) if candidate[8] else ""
                candidate_muni  = normalize_text(str(candidate[9])) if candidate[9] else ""
                candidate_ips   = normalize_text(str(candidate[7])) if candidate[7] else ""

                score = self._calculate_geo_similarity(
                    depto_norm, muni_norm, ips_norm,
                    candidate_depto, candidate_muni, candidate_ips
                )
                print(f"         Candidato: Depto='{candidate_depto[:20]}', "
                      f"Muni='{candidate_muni[:20]}', IPS='{candidate_ips[:30]}' "
                      f"→ Similitud: {score*100:.1f}%")

                if score > best_score:
                    best_score = score
                    best_match = candidate

            if best_match:
                print(f"      🎯 Mejor match: {best_score*100:.1f}% de similitud")

            return best_match, best_score

        except Exception as e:
            print(f"      ⚠️ Error en query por similitud: {e}")
            return None, 0.0

    # ============================================================
    # SELECT FIELDS
    # ============================================================

    def _get_select_fields(self) -> List[str]:
        """
        Índices fijos en la tupla resultado:
          0  consultas_procedimientos
          1  frecuencia_edad
          2  meta
          3  frecuencia_indicada
          4  cups
          5  periodo
          6  proyeccion_tiempo
          7  nombre_ips
          8  departamento
          9  municipio
          10 intervenciones_realizadas  (NULL si no existe)
          11 atenciones_realizar_anual  (NULL si no existe)
          12 habilitado_column          (solo si existe)
        """
        fields = [
            'consultas_procedimientos',
            'frecuencia_edad',
            'meta',
            'frecuencia_indicada',
            'cups',
            'periodo',
            'proyeccion_tiempo',
            'nombre_ips',
            'departamento',
            'municipio',
        ]
        fields.append(f'"{self.historico_column}"' if self.historico_column else 'NULL')
        fields.append(f'"{self.atenciones_column}"' if self.atenciones_column else 'NULL')
        if self.habilitado_column:
            fields.append(f'"{self.habilitado_column}"')
        return fields

    # ============================================================
    # PROCESAMIENTO DE RESULTADO
    # ============================================================

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

        # ── Índice 10: histórico ──────────────────────────────────────
        historico_raw = result[10] if len(result) > 10 else None
        historico = _parse_float(historico_raw)
        print(f"      📋 Histórico (intervenciones_realizadas): raw={repr(historico_raw)} → usado={historico}")

        # ── Índice 11: atenciones_realizar_anual ─────────────────────
        atenciones_raw = result[11] if len(result) > 11 else None
        atenciones_realizar_anual = _parse_float(atenciones_raw)
        print(f"      📋 Atenciones a realizar anual: raw={repr(atenciones_raw)} → usado={atenciones_realizar_anual}")

        # ── Diagnóstico de meta para facilitar depuración ─────────────
        meta_raw = result[2]
        meta_parsed = _parse_float(meta_raw)
        print(f"      📋 Meta: raw={repr(meta_raw)} → usado={meta_parsed}")

        result_dict = {
            'consultas_procedimientos':  result[0],
            'frecuencia_edad':           result[1],
            # _parse_float maneja '70%' → 0.70, '25,94%' → 0.2594, '0.7' → 0.7
            'meta':                      meta_parsed,
            'frecuencia_indicada':       _parse_float(result[3]),
            'cups':                      result[4],
            'periodo':                   result[5],
            'proyeccion_tiempo':         _parse_int(result[6], default=12),
            'nombre_ips':                result[7],
            'departamento':              result[8],
            'municipio':                 result[9],
            'intervenciones_realizadas': historico,
            'atenciones_realizar_anual': atenciones_realizar_anual,
            'similarity_score':          similarity_score,
        }

        # ── Índice 12: habilitado ─────────────────────────────────────
        habilitado = 1
        if self.habilitado_column and len(result) > 12:
            habilitado_val = result[12]
            habilitado = 1 if habilitado_val in [1, '1', 'SI', 'si', 'Sí', True] else 0

        result_dict['habilitado'] = habilitado

        if habilitado == 0:
            print(f"      ⚠️ SERVICIO DESHABILITADO (servicios_habilitados = 0)")
            print(f"      💡 Numerador y denominador serán 0, semaforización NA")
            return {
                'habilitado':             False,
                'consulta_procedimiento': consulta_procedimiento,
                'edad_nt_rpms':           edad_nt_rpms,
                'departamento':           departamento,
                'municipio':              municipio,
                'nombre_ips':             nombre_ips,
                'similarity_score':       similarity_score,
                **result_dict
            }

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

    # ============================================================
    # SIMILITUD
    # ============================================================

    def _calculate_geo_similarity(
        self,
        depto_input: Optional[str],
        muni_input: Optional[str],
        ips_input: Optional[str],
        depto_candidate: str,
        muni_candidate: str,
        ips_candidate: str
    ) -> float:
        scores = []
        if depto_input:
            if depto_input == depto_candidate: scores.append(1.0)
            else: return 0.0
        if muni_input:
            if muni_input == muni_candidate: scores.append(1.0)
            else: return 0.0
        if ips_input:
            scores.append(self._calculate_text_similarity(ips_input, ips_candidate))
        return sum(scores) / len(scores) if scores else 0.0

    def _calculate_text_similarity(self, text1: str, text2: str) -> float:
        tokens1 = set(self._tokenize(text1))
        tokens2 = set(self._tokenize(text2))
        common  = tokens1 & tokens2
        if not tokens1 or not tokens2:
            return 0.0
        return len(common) / max(len(tokens1), len(tokens2))

    def _tokenize(self, text: str) -> List[str]:
        import re
        cleaned = re.sub(r'[.,\-():/]', ' ', text)
        return [t.upper() for t in cleaned.split() if len(t) >= 2]

    def _build_normalized_condition(self, column_name: str, normalized_value: str) -> str:
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

    # ============================================================
    # MÉTRICAS EXTENDIDAS
    # ============================================================

    def calculate_extended_metrics(
        self,
        poblacion_objeto: int,
        nt_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        try:
            if not nt_data.get('habilitado', True):
                return {
                    'poblacion_susceptible': 0, 'valor_mensual': 0,
                    'meta': 0.0, 'frecuencia_uso': 0.0, 'proyeccion_tiempo': 0
                }

            meta       = _parse_float(nt_data.get('meta', 0.0))
            frecuencia = _parse_float(nt_data.get('frecuencia_indicada', 1.0))
            proyeccion = _parse_int(nt_data.get('proyeccion_tiempo', 12), default=12)

            poblacion_susceptible = poblacion_objeto * meta
            denominador_mensual   = (poblacion_susceptible * frecuencia) / proyeccion if proyeccion > 0 else 0

            return {
                'poblacion_susceptible': round(poblacion_susceptible, 1),
                'valor_mensual':         round(denominador_mensual, 1),
                'meta':                  meta,
                'frecuencia_uso':        _parse_float(nt_data.get('frecuencia_uso', 0.0)),
                'proyeccion_tiempo':     proyeccion
            }
        except Exception as e:
            print(f"Error calculando métricas: {e}")
            return {
                'poblacion_susceptible': 0, 'valor_mensual': 0,
                'meta': 0.0, 'frecuencia_uso': 0.0, 'proyeccion_tiempo': 12
            }

    def __del__(self):
        if self.table_name and 'temp' in self.table_name:
            try:
                duckdb_service.conn.execute(f"DROP VIEW IF EXISTS {self.table_name}")
            except:
                pass