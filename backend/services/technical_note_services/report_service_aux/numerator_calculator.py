# services/technical_note_services/report_service_aux/numerator_calculator.py

from typing import Dict, Any, Optional, Tuple
import re
import json
import os
from datetime import datetime, date
from services.duckdb_service.duckdb_service import duckdb_service
from services.technical_note_services.report_service_aux.sex_filter_loader import sex_filter_config
from utils.text_normalizer import normalize_text


# ============================================================
# CARGA DEL JSON DE RANGOS DE FECHAS DE NACIMIENTO
# ============================================================

def _load_birth_date_ranges() -> Dict[str, Dict[str, list]]:
    """Carga el JSON de rangos de fechas de nacimiento desde config/"""
    try:
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        json_path = os.path.join(base_dir, "config", "birth_date_ranges_den.json")

        if not os.path.exists(json_path):
            print(f"⚠️ No se encontró el archivo: {json_path}")
            return {}

        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        print(f"✅ birth_date_ranges_den.json cargado: {len(data)} rangos de edad")
        return data

    except Exception as e:
        print(f"❌ Error cargando birth_date_ranges_den.json: {e}")
        return {}


# Instancia global cargada una sola vez al importar el módulo
BIRTH_DATE_RANGES_BY_AGE: Dict[str, Dict[str, list]] = _load_birth_date_ranges()


# ============================================================
# UTILIDADES DE EDAD
# ============================================================

def _normalize_age_key(edad: str) -> str:
    """Normaliza el texto de edad para comparaciones"""
    return edad.lower().strip()


def _find_birth_range_key(edad_aplicable: str) -> Optional[str]:
    """Busca la clave exacta en BIRTH_DATE_RANGES_BY_AGE comparando texto normalizado"""
    edad_norm = _normalize_age_key(edad_aplicable)
    for key in BIRTH_DATE_RANGES_BY_AGE:
        if _normalize_age_key(key) == edad_norm:
            return key
    return None


def _is_age_in_months(edad_aplicable: str) -> bool:
    """Detecta si la edad está expresada en meses"""
    return 'mes' in edad_aplicable.lower().strip()


def _is_age_in_years(edad_aplicable: str) -> bool:
    """Detecta si la edad está expresada en años"""
    edad_lower = edad_aplicable.lower().strip()
    return 'año' in edad_lower or 'ano' in edad_lower


def _parse_dd_mm_yyyy(fecha_str: str) -> Optional[date]:
    """Convierte string dd/mm/yyyy a objeto date"""
    try:
        return datetime.strptime(fecha_str.strip(), "%d/%m/%Y").date()
    except Exception:
        return None


def _calcular_mes_inicio(proyeccion_tiempo: int) -> int:
    """
    Calcula el mes de inicio según proyeccion_tiempo.
    proyeccion_tiempo=12 → mes_inicio=1  (enero a diciembre)
    proyeccion_tiempo=8  → mes_inicio=5  (mayo a diciembre)
    proyeccion_tiempo=6  → mes_inicio=7  (julio a diciembre)
    """
    mes_inicio = 13 - int(proyeccion_tiempo)
    return max(1, min(12, mes_inicio))


class NumeratorCalculator:
    """Responsable de calcular numeradores mensuales con soporte para filtros especiales"""

    @property
    def conn(self):
        return duckdb_service.conn

    def calculate_by_month_simple(
        self,
        data_source: str,
        column_name: str,
        where_clause: str,
        anio_corte: int,
        mes_limite: int,
        keyword: str = None,
        corte_fecha: str = None,
        edad_aplicable: str = None,
        proyeccion_tiempo: int = 12          # 🔥 NUEVO PARÁMETRO
    ) -> Dict[int, int]:
        """
        Calcula numerador MES A MES del AÑO de corte.
        Solo calcula los meses contratados según proyeccion_tiempo.
        proyeccion_tiempo=8 → calcula desde mayo (mes 5) hasta diciembre.
        """
        # 🔥 Calcular mes de inicio según proyeccion_tiempo
        mes_inicio = _calcular_mes_inicio(proyeccion_tiempo)

        print(f"      📊 Calculando numerador del AÑO {anio_corte} hasta {corte_fecha}")
        print(f"      📅 Columna de fechas: '{column_name}'")
        print(f"      📆 Meses contratados: {proyeccion_tiempo} → Mes {mes_inicio} al {mes_limite}")
        if edad_aplicable:
            print(f"      🎯 Edad aplicable: '{edad_aplicable}'")

        col_escaped = f'"{column_name}"'
        formato_detectado = self._detect_date_format(data_source, col_escaped, where_clause)

        # Variables para filtros especiales
        metodo_col = None
        tamizaje_col = None
        filter_type = None

        if keyword:
            keyword_lower = keyword.lower().strip()

            sex_filter_type = sex_filter_config.get_sex_filter_for_keyword(keyword_lower)
            if sex_filter_type:
                sex_label = "MUJERES (F)" if sex_filter_type == 'F' else "HOMBRES (M)"
                print(f"      🔍 Keyword: '{keyword}' → Filtro de sexo: {sex_label}")

            if self._is_tamizaje_keyword(keyword_lower):
                tamizaje_col = self._detect_tamizaje_column(data_source)
                if tamizaje_col:
                    filter_type = 'tamizaje'
                    print(f"      ✓ Columna tamizaje: '{tamizaje_col}'")
                    self._debug_tamizaje_values(
                        data_source, tamizaje_col, where_clause, keyword,
                        formato_detectado, col_escaped, anio_corte, edad_aplicable
                    )

            elif self._is_metodo_anticonceptivo_keyword(keyword_lower):
                metodo_col = self._detect_metodo_column(data_source)
                if metodo_col:
                    filter_type = 'metodo'
                    print(f"      ✓ Columna método: '{metodo_col}'")
                    self._debug_metodo_values(
                        data_source, metodo_col, where_clause, keyword,
                        formato_detectado, col_escaped, anio_corte
                    )

        # 🔥 Decidir estrategia según tipo de edad
        if edad_aplicable and _is_age_in_months(edad_aplicable):
            print(f"      🍼 Edad en MESES → usando rangos de fechas de nacimiento")
            numeradores = self._calculate_monthly_counts_by_birth_range(
                data_source, col_escaped, where_clause,
                formato_detectado, anio_corte, corte_fecha,
                mes_inicio, mes_limite,                      # 🔥 mes_inicio y mes_limite
                keyword, metodo_col, tamizaje_col, filter_type,
                edad_aplicable
            )
        else:
            print(f"      📆 Edad en AÑOS (o sin edad) → usando filtro por columna Edad")
            numeradores = self._calculate_monthly_counts(
                data_source, col_escaped, where_clause,
                formato_detectado, anio_corte, corte_fecha,
                mes_inicio, mes_limite,                      # 🔥 mes_inicio y mes_limite
                keyword, metodo_col, tamizaje_col, filter_type,
                edad_aplicable
            )

        total = sum(numeradores.values())
        meses_con_datos = sum(1 for v in numeradores.values() if v > 0)
        print(f"         📊 TOTAL NUMERADOR ({anio_corte}): {total} ({meses_con_datos} meses con datos)")

        return numeradores

    # ============================================================
    # 🔥 CÁLCULO POR RANGOS DE FECHAS DE NACIMIENTO (MESES)
    # ============================================================

    def _calculate_monthly_counts_by_birth_range(
        self,
        data_source: str,
        col_escaped: str,
        where_clause: str,
        formato: str,
        anio_corte: int,
        corte_fecha: str,
        mes_inicio: int,      # 🔥 Primer mes a calcular
        mes_limite: int,      # 🔥 Último mes a calcular
        keyword: Optional[str],
        metodo_col: Optional[str],
        tamizaje_col: Optional[str],
        filter_type: Optional[str],
        edad_aplicable: str
    ) -> Dict[int, int]:
        range_key = _find_birth_range_key(edad_aplicable)

        if not range_key:
            print(f"      ⚠️ No se encontró rango de nacimiento para '{edad_aplicable}', fallback sin filtro de edad")
            return self._calculate_monthly_counts(
                data_source, col_escaped, where_clause,
                formato, anio_corte, corte_fecha,
                mes_inicio, mes_limite,
                keyword, metodo_col, tamizaje_col, filter_type, None
            )

        birth_ranges = BIRTH_DATE_RANGES_BY_AGE[range_key]
        print(f"      ✅ Rango encontrado: '{range_key}'")

        birth_col = self._detect_birth_date_column(data_source)
        if not birth_col:
            print(f"      ⚠️ No se encontró columna de fecha de nacimiento, fallback sin filtro de edad")
            return self._calculate_monthly_counts(
                data_source, col_escaped, where_clause,
                formato, anio_corte, corte_fecha,
                mes_inicio, mes_limite,
                keyword, metodo_col, tamizaje_col, filter_type, None
            )

        print(f"      🗓️ Columna fecha nacimiento: '{birth_col}'")
        print(f"      📆 Calculando meses {mes_inicio} al {mes_limite}")

        special_filter = ""
        sex_filter = ""

        if keyword:
            sex_type = sex_filter_config.get_sex_filter_for_keyword(keyword)
            if sex_type:
                sex_filter = self._build_sex_filter(data_source, sex_type)
                sex_label = "MUJERES" if sex_type == 'F' else "HOMBRES"
                if sex_filter:
                    print(f"      {'👩' if sex_type == 'F' else '👨'} Filtro de sexo: {sex_label}")

        if filter_type == 'tamizaje' and keyword and tamizaje_col:
            special_filter = self._build_tamizaje_filter(keyword, tamizaje_col)
        elif filter_type == 'metodo' and keyword and metodo_col:
            special_filter = self._build_metodo_filter(keyword, metodo_col)

        birth_formato = self._detect_date_format(data_source, f'"{birth_col}"', where_clause)

        # Inicializar todos los meses en 0
        numeradores = {m: 0 for m in range(1, 13)}

        # 🔥 Iterar SOLO los meses contratados (mes_inicio a mes_limite)
        for mes in range(mes_inicio, mes_limite + 1):
            mes_str = str(mes)
            if mes_str not in birth_ranges:
                print(f"         ⚠️ Mes {mes} no encontrado en rangos de '{range_key}'")
                continue

            fecha_inicio_str, fecha_fin_str = birth_ranges[mes_str]
            fecha_inicio = _parse_dd_mm_yyyy(fecha_inicio_str)
            fecha_fin    = _parse_dd_mm_yyyy(fecha_fin_str)

            if not fecha_inicio or not fecha_fin:
                print(f"         ⚠️ Error parseando fechas mes {mes}: {fecha_inicio_str} - {fecha_fin_str}")
                continue

            fecha_inicio_sql = fecha_inicio.strftime("%Y-%m-%d")
            fecha_fin_sql    = fecha_fin.strftime("%Y-%m-%d")

            # Filtro nacimiento robusto: excluye NULL real, string "NULL" y vacíos
            if birth_formato == 'yyyy-mm-dd':
                birth_filter = f"""
                AND "{birth_col}" IS NOT NULL
                AND CAST("{birth_col}" AS VARCHAR) != ''
                AND UPPER(CAST("{birth_col}" AS VARCHAR)) != 'NULL'
                AND CAST(CAST("{birth_col}" AS VARCHAR) AS DATE) >= DATE '{fecha_inicio_sql}'
                AND CAST(CAST("{birth_col}" AS VARCHAR) AS DATE) <= DATE '{fecha_fin_sql}'
                """
            else:
                birth_filter = f"""
                AND "{birth_col}" IS NOT NULL
                AND CAST("{birth_col}" AS VARCHAR) != ''
                AND UPPER(CAST("{birth_col}" AS VARCHAR)) != 'NULL'
                AND strptime(CAST("{birth_col}" AS VARCHAR), '%d/%m/%Y') >= DATE '{fecha_inicio_sql}'
                AND strptime(CAST("{birth_col}" AS VARCHAR), '%d/%m/%Y') <= DATE '{fecha_fin_sql}'
                """

            if formato == 'yyyy-mm-dd':
                query = f"""
                SELECT COUNT(DISTINCT "Nro Identificación") as total
                FROM {data_source}
                WHERE {where_clause}
                  {sex_filter}
                  {special_filter}
                  {birth_filter}
                  AND {col_escaped} IS NOT NULL
                  AND CAST({col_escaped} AS VARCHAR) != ''
                  AND UPPER(CAST({col_escaped} AS VARCHAR)) != 'NULL'
                  AND CAST({col_escaped} AS VARCHAR) LIKE '____-__-__'
                  AND EXTRACT(YEAR  FROM CAST(CAST({col_escaped} AS VARCHAR) AS DATE)) = {anio_corte}
                  AND EXTRACT(MONTH FROM CAST(CAST({col_escaped} AS VARCHAR) AS DATE)) = {mes}
                  AND CAST(CAST({col_escaped} AS VARCHAR) AS DATE) <= DATE '{corte_fecha}'
                """
            else:
                query = f"""
                SELECT COUNT(DISTINCT "Nro Identificación") as total
                FROM {data_source}
                WHERE {where_clause}
                  {sex_filter}
                  {special_filter}
                  {birth_filter}
                  AND {col_escaped} IS NOT NULL
                  AND CAST({col_escaped} AS VARCHAR) != ''
                  AND UPPER(CAST({col_escaped} AS VARCHAR)) != 'NULL'
                  AND EXTRACT(YEAR  FROM strptime(CAST({col_escaped} AS VARCHAR), '%d/%m/%Y')) = {anio_corte}
                  AND EXTRACT(MONTH FROM strptime(CAST({col_escaped} AS VARCHAR), '%d/%m/%Y')) = {mes}
                  AND strptime(CAST({col_escaped} AS VARCHAR), '%d/%m/%Y') <= DATE '{corte_fecha}'
                """

            try:
                result = self.conn.execute(query).fetchone()
                count = int(result[0]) if result and result[0] else 0
                numeradores[mes] = count
                if count > 0:
                    print(f"         ✓ Mes {mes:2d} [{fecha_inicio_str} → {fecha_fin_str}]: {count} atenciones")
                else:
                    print(f"         · Mes {mes:2d} [{fecha_inicio_str} → {fecha_fin_str}]: 0")
            except Exception as e:
                print(f"         ❌ Error mes {mes}: {e}")
                import traceback
                traceback.print_exc()
                numeradores[mes] = 0

        return numeradores

    # ============================================================
    # CÁLCULO MENSUAL ESTÁNDAR (AÑOS)
    # ============================================================

    def _calculate_monthly_counts(
        self,
        data_source: str,
        col_escaped: str,
        where_clause: str,
        formato: str,
        anio_corte: int,
        corte_fecha: str,
        mes_inicio: int,      # 🔥 Primer mes a calcular
        mes_limite: int,      # 🔥 Último mes a calcular
        keyword: Optional[str],
        metodo_col: Optional[str],
        tamizaje_col: Optional[str],
        filter_type: Optional[str],
        edad_aplicable: Optional[str] = None
    ) -> Dict[int, int]:
        special_filter = ""
        age_filter     = ""
        sex_filter     = ""

        print(f"      📆 Calculando meses {mes_inicio} al {mes_limite}")

        if edad_aplicable and _is_age_in_years(edad_aplicable):
            age_filter = self._build_age_filter_years(edad_aplicable, data_source)
            if age_filter:
                print(f"      🎯 Filtro de edad en años aplicado: {edad_aplicable}")

        if keyword:
            sex_type = sex_filter_config.get_sex_filter_for_keyword(keyword)
            if sex_type:
                sex_filter = self._build_sex_filter(data_source, sex_type)
                if sex_filter:
                    sex_label = "MUJERES" if sex_type == 'F' else "HOMBRES"
                    print(f"      {'👩' if sex_type == 'F' else '👨'} Filtro de sexo: {sex_label}")

        if filter_type == 'tamizaje' and keyword and tamizaje_col:
            special_filter = self._build_tamizaje_filter(keyword, tamizaje_col)
        elif filter_type == 'metodo' and keyword and metodo_col:
            special_filter = self._build_metodo_filter(keyword, metodo_col)

        # 🔥 Filtro de mes en la query SQL (mes_inicio a mes_limite)
        if formato == 'yyyy-mm-dd':
            query = f"""
            SELECT
                EXTRACT(MONTH FROM CAST(CAST({col_escaped} AS VARCHAR) AS DATE)) as mes,
                COUNT(DISTINCT "Nro Identificación") as total
            FROM {data_source}
            WHERE {where_clause}
              {age_filter}
              {sex_filter}
              {special_filter}
              AND {col_escaped} IS NOT NULL
              AND CAST({col_escaped} AS VARCHAR) != ''
              AND UPPER(CAST({col_escaped} AS VARCHAR)) != 'NULL'
              AND CAST({col_escaped} AS VARCHAR) LIKE '____-__-__'
              AND EXTRACT(YEAR  FROM CAST(CAST({col_escaped} AS VARCHAR) AS DATE)) = {anio_corte}
              AND EXTRACT(MONTH FROM CAST(CAST({col_escaped} AS VARCHAR) AS DATE)) >= {mes_inicio}
              AND CAST(CAST({col_escaped} AS VARCHAR) AS DATE) <= DATE '{corte_fecha}'
            GROUP BY EXTRACT(MONTH FROM CAST(CAST({col_escaped} AS VARCHAR) AS DATE))
            ORDER BY mes
            """
        else:
            query = f"""
            SELECT
                EXTRACT(MONTH FROM strptime(CAST({col_escaped} AS VARCHAR), '%d/%m/%Y')) as mes,
                COUNT(DISTINCT "Nro Identificación") as total
            FROM {data_source}
            WHERE {where_clause}
              {age_filter}
              {sex_filter}
              {special_filter}
              AND {col_escaped} IS NOT NULL
              AND CAST({col_escaped} AS VARCHAR) != ''
              AND UPPER(CAST({col_escaped} AS VARCHAR)) != 'NULL'
              AND EXTRACT(YEAR  FROM strptime(CAST({col_escaped} AS VARCHAR), '%d/%m/%Y')) = {anio_corte}
              AND EXTRACT(MONTH FROM strptime(CAST({col_escaped} AS VARCHAR), '%d/%m/%Y')) >= {mes_inicio}
              AND strptime(CAST({col_escaped} AS VARCHAR), '%d/%m/%Y') <= DATE '{corte_fecha}'
            GROUP BY EXTRACT(MONTH FROM strptime(CAST({col_escaped} AS VARCHAR), '%d/%m/%Y'))
            ORDER BY mes
            """

        try:
            result = self.conn.execute(query).fetchall()
            numeradores = {m: 0 for m in range(1, 13)}
            for mes_num, total in result:
                # 🔥 Solo guardar si está en el rango contratado
                if mes_num and mes_inicio <= int(mes_num) <= mes_limite:
                    numeradores[int(mes_num)] = int(total)
                    print(f"         ✓ Mes {int(mes_num):2d} ({anio_corte}): {int(total)} atenciones")
            return numeradores
        except Exception as e:
            print(f"         ❌ Error: {e}")
            import traceback
            traceback.print_exc()
            return {m: 0 for m in range(1, 13)}

    # ============================================================
    # DETECCIÓN DE COLUMNA DE FECHA DE NACIMIENTO
    # ============================================================

    def _detect_birth_date_column(self, data_source: str) -> Optional[str]:
        """Detecta la columna de fecha de nacimiento en el dataset"""
        try:
            describe_query = f"DESCRIBE SELECT * FROM {data_source}"
            cols = [r[0] for r in self.conn.execute(describe_query).fetchall()]

            exact_candidates = [
                "Fecha de Nacimiento", "Fecha Nacimiento", "FechaNacimiento",
                "fecha_nacimiento", "Fecha_Nacimiento", "FECHA_NACIMIENTO",
                "FEC_NAC", "Fec Nac", "F. Nacimiento"
            ]
            for candidate in exact_candidates:
                if candidate in cols:
                    return candidate

            cols_norm = {normalize_text(c): c for c in cols}
            norm_candidates = ["fecha nacimiento", "nacimiento", "fec nac", "fnacimiento"]
            for cand in norm_candidates:
                if cand in cols_norm:
                    return cols_norm[cand]

            for col in cols:
                col_norm = normalize_text(col)
                if "nacimiento" in col_norm or "fec_nac" in col_norm:
                    return col

            print(f"      ⚠️ Columna de nacimiento no encontrada. Cols: {cols[:10]}")
            return None

        except Exception as e:
            print(f"      ❌ Error detectando columna nacimiento: {e}")
            return None

    # ============================================================
    # FILTROS DE EDAD EN AÑOS
    # ============================================================

    def _build_age_filter_years(self, edad_aplicable: str, data_source: str) -> str:
        """Construye filtro SQL de edad para AÑOS usando columna Edad"""
        numeros = re.findall(r'(\d+)', edad_aplicable)

        try:
            describe_query = f"DESCRIBE SELECT * FROM {data_source}"
            cols = [r[0] for r in self.conn.execute(describe_query).fetchall()]

            edad_col = None
            for col in cols:
                if col == "Edad":
                    edad_col = col
                    break

            if not edad_col:
                cols_norm = {normalize_text(c): c for c in cols}
                if "edad" in cols_norm:
                    edad_col = cols_norm["edad"]

            if not edad_col:
                print(f"      ⚠️ No se encontró columna 'Edad'")
                return ""

            if len(numeros) == 1:
                edad_valor = int(numeros[0])
                return f'AND CAST("{edad_col}" AS INTEGER) = {edad_valor}'
            elif len(numeros) == 2:
                edad_min = int(numeros[0])
                edad_max = int(numeros[1])
                edades_list = list(range(edad_min, edad_max + 1))
                edades_str = ','.join(map(str, edades_list))
                return f'AND CAST("{edad_col}" AS INTEGER) IN ({edades_str})'

        except Exception as e:
            print(f"      ⚠️ Error construyendo filtro de edad en años: {e}")

        return ""

    def _build_age_filter(self, edad_aplicable: str, data_source: str) -> str:
        """Alias de compatibilidad con llamadas anteriores"""
        if _is_age_in_years(edad_aplicable):
            return self._build_age_filter_years(edad_aplicable, data_source)
        return ""

    # ============================================================
    # FILTRO DE SEXO
    # ============================================================

    def _build_sex_filter(self, data_source: str, sex_type: str) -> str:
        """Construye filtro SQL para sexo específico"""
        try:
            describe_query = f"DESCRIBE SELECT * FROM {data_source}"
            cols = [r[0] for r in self.conn.execute(describe_query).fetchall()]

            sex_col = None
            for col in cols:
                if col in ["Sexo", "Genero", "Género", "Sexo Biológico", "Sexo Biologico"]:
                    sex_col = col
                    break

            if not sex_col:
                cols_norm = {normalize_text(c): c for c in cols}
                for candidate in ["Sexo", "Genero", "Género"]:
                    cand_norm = normalize_text(candidate)
                    if cand_norm in cols_norm:
                        sex_col = cols_norm[cand_norm]
                        break

            if sex_col:
                if sex_type == 'F':
                    return f"""
                    AND "{sex_col}" IS NOT NULL
                    AND (
                        UPPER(CAST("{sex_col}" AS VARCHAR)) = 'F'
                        OR UPPER(CAST("{sex_col}" AS VARCHAR)) LIKE '%FEMENINO%'
                        OR UPPER(CAST("{sex_col}" AS VARCHAR)) LIKE '%MUJER%'
                    )
                    """
                elif sex_type == 'M':
                    return f"""
                    AND "{sex_col}" IS NOT NULL
                    AND (
                        UPPER(CAST("{sex_col}" AS VARCHAR)) = 'M'
                        OR UPPER(CAST("{sex_col}" AS VARCHAR)) LIKE '%MASCULINO%'
                        OR UPPER(CAST("{sex_col}" AS VARCHAR)) LIKE '%HOMBRE%'
                        OR UPPER(CAST("{sex_col}" AS VARCHAR)) LIKE '%VARON%'
                    )
                    """
            return ""

        except Exception as e:
            print(f"      ❌ Error construyendo filtro de sexo: {e}")
            return ""

    # ============================================================
    # TAMIZAJE
    # ============================================================

    def _is_tamizaje_keyword(self, keyword: str) -> bool:
        keyword_norm = normalize_text(keyword)
        return any(ind in keyword_norm for ind in ['TAMIZAJE', 'CITOLOGIA', 'ADN-VPH', 'CUELLO UTERINO'])

    def _detect_tamizaje_column(self, data_source: str) -> Optional[str]:
        try:
            describe_query = f"DESCRIBE SELECT * FROM {data_source}"
            cols = [r[0] for r in self.conn.execute(describe_query).fetchall()]
            for col in cols:
                if "Tamizaje" in col and "Cuello Uterino" in col:
                    return col
            return None
        except:
            return None

    def _build_tamizaje_filter(self, keyword: str, tamizaje_col: str) -> str:
        keyword_norm = normalize_text(keyword)
        if 'CITOLOGIA' in keyword_norm:
            return f'AND "{tamizaje_col}" IS NOT NULL AND (UPPER(CAST("{tamizaje_col}" AS VARCHAR)) LIKE \'%1 -%\' OR UPPER(CAST("{tamizaje_col}" AS VARCHAR)) LIKE \'%4 -%\')'
        if 'ADN' in keyword_norm or 'VPH' in keyword_norm:
            return f'AND "{tamizaje_col}" IS NOT NULL AND (UPPER(CAST("{tamizaje_col}" AS VARCHAR)) LIKE \'%2 -%\' OR UPPER(CAST("{tamizaje_col}" AS VARCHAR)) LIKE \'%4 -%\')'
        return f'AND "{tamizaje_col}" IS NOT NULL'

    def _debug_tamizaje_values(self, data_source, tamizaje_col, where_clause, keyword, formato, col_escaped, anio_corte, edad_aplicable=None):
        print(f"      🧪 Debug tamizaje: '{keyword}'")

    # ============================================================
    # MÉTODO ANTICONCEPTIVO
    # ============================================================

    def _is_metodo_anticonceptivo_keyword(self, keyword: str) -> bool:
        keyword_norm = normalize_text(keyword)
        metodos = ['DIU', 'INTRAUTERINO', 'SUBDERMICO', 'IMPLANTE', 'ORAL', 'INYECTABLE', 'PRESERVATIVO', 'EMERGENCIA']
        return any(metodo in keyword_norm for metodo in metodos)

    def _detect_metodo_column(self, data_source: str) -> Optional[str]:
        try:
            describe_query = f"DESCRIBE SELECT * FROM {data_source}"
            cols = [r[0] for r in self.conn.execute(describe_query).fetchall()]
            for col in cols:
                if col == "Método Anticonceptivo":
                    return col
            return None
        except:
            return None

    def _build_metodo_filter(self, keyword: str, metodo_col: str) -> str:
        keyword_norm = normalize_text(keyword)
        if 'DIU' in keyword_norm or 'INTRAUTERINO' in keyword_norm:
            return f'AND "{metodo_col}" IS NOT NULL AND (UPPER(CAST("{metodo_col}" AS VARCHAR)) LIKE \'%DIU%\' OR UPPER(CAST("{metodo_col}" AS VARCHAR)) LIKE \'%INTRAUTERINO%\')'
        if 'SUBDERMICO' in keyword_norm or 'IMPLANTE' in keyword_norm:
            return f'AND "{metodo_col}" IS NOT NULL AND (UPPER(CAST("{metodo_col}" AS VARCHAR)) LIKE \'%IMPLANTE%\' OR UPPER(CAST("{metodo_col}" AS VARCHAR)) LIKE \'%SUBDERMICO%\')'
        if 'PRESERVATIVO' in keyword_norm:
            return f'AND "{metodo_col}" IS NOT NULL AND UPPER(CAST("{metodo_col}" AS VARCHAR)) LIKE \'%PRESERVATIVO%\''
        if 'ORAL' in keyword_norm:
            return f'AND "{metodo_col}" IS NOT NULL AND UPPER(CAST("{metodo_col}" AS VARCHAR)) LIKE \'%ORAL%\''
        if 'INYECTABLE' in keyword_norm:
            return f'AND "{metodo_col}" IS NOT NULL AND UPPER(CAST("{metodo_col}" AS VARCHAR)) LIKE \'%INYECTABLE%\''
        if 'EMERGENCIA' in keyword_norm:
            return f'AND "{metodo_col}" IS NOT NULL AND UPPER(CAST("{metodo_col}" AS VARCHAR)) LIKE \'%EMERGENCIA%\''
        return f'AND "{metodo_col}" IS NOT NULL'

    def _debug_metodo_values(self, data_source, metodo_col, where_clause, keyword, formato, col_escaped, anio_corte):
        print(f"      🧪 Debug método anticonceptivo: '{keyword}'")

    # ============================================================
    # UTILIDADES
    # ============================================================

    def _detect_date_format(self, data_source: str, col_escaped: str, where_clause: str) -> str:
        """Detecta formato de fecha automáticamente"""
        query = f"""
        SELECT CAST({col_escaped} AS VARCHAR)
        FROM {data_source}
        WHERE {where_clause}
          AND {col_escaped} IS NOT NULL
          AND CAST({col_escaped} AS VARCHAR) != ''
        LIMIT 3
        """
        try:
            samples = self.conn.execute(query).fetchall()
            if samples:
                first = samples[0][0]
                formato = 'yyyy-mm-dd' if '-' in first else 'dd/mm/yyyy'
                print(f"      📅 Formato: {formato}")
                return formato
        except:
            pass
        return 'yyyy-mm-dd'
