# services/technical_note_services/report_service_aux/numerator_calculator.py

from typing import Dict, Any, Optional
import re
from services.duckdb_service.duckdb_service import duckdb_service
from services.technical_note_services.report_service_aux.sex_filter_loader import sex_filter_config
from utils.text_normalizer import normalize_text


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
        edad_aplicable: str = None
    ) -> Dict[int, int]:
        """
        Calcula numerador MES A MES del AÑO de corte específico.
        
        Usa configuración externa (sex_filter_config.json) para determinar
        qué keywords requieren filtro de sexo.
        """
        print(f"      📊 Calculando numerador del AÑO {anio_corte} hasta {corte_fecha}")
        print(f"      📅 Columna de fechas: '{column_name}'")
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
            
            # Detectar tipo de filtro usando configuración
            sex_filter_type = sex_filter_config.get_sex_filter_for_keyword(keyword_lower)
            
            if sex_filter_type:
                sex_label = "MUJERES (F)" if sex_filter_type == 'F' else "HOMBRES (M)"
                print(f"      🔍 Keyword: '{keyword}' → Filtro de sexo: {sex_label}")
            
            # Detectar si es tamizaje de cáncer de cuello uterino
            if self._is_tamizaje_keyword(keyword_lower):
                tamizaje_col = self._detect_tamizaje_column(data_source)
                if tamizaje_col:
                    filter_type = 'tamizaje'
                    print(f"      ✓ Columna tamizaje: '{tamizaje_col}'")
                    self._debug_tamizaje_values(
                        data_source, tamizaje_col, where_clause, keyword,
                        formato_detectado, col_escaped, anio_corte,
                        edad_aplicable
                    )
            
            # Detectar si es método anticonceptivo
            elif self._is_metodo_anticonceptivo_keyword(keyword_lower):
                metodo_col = self._detect_metodo_column(data_source)
                if metodo_col:
                    filter_type = 'metodo'
                    print(f"      ✓ Columna método: '{metodo_col}'")
                    self._debug_metodo_values(
                        data_source, metodo_col, where_clause, keyword,
                        formato_detectado, col_escaped, anio_corte
                    )
        
        # Contar por mes con filtros
        numeradores = self._calculate_monthly_counts(
            data_source, col_escaped, where_clause,
            formato_detectado, anio_corte, corte_fecha, mes_limite,
            keyword, metodo_col, tamizaje_col, filter_type,
            edad_aplicable
        )
        
        total = sum(numeradores.values())
        print(f"         📊 TOTAL NUMERADOR ({anio_corte}): {total}")
        
        return numeradores
    
    def _calculate_monthly_counts(
        self,
        data_source: str,
        col_escaped: str,
        where_clause: str,
        formato: str,
        anio_corte: int,
        corte_fecha: str,
        mes_limite: int,
        keyword: Optional[str],
        metodo_col: Optional[str],
        tamizaje_col: Optional[str],
        filter_type: Optional[str],
        edad_aplicable: Optional[str] = None
    ) -> Dict[int, int]:
        """Cuenta registros por mes con filtros especiales"""
        special_filter = ""
        age_filter = ""
        sex_filter = ""
        
        # Filtro de edad
        if edad_aplicable:
            age_filter = self._build_age_filter(edad_aplicable, data_source)
            if age_filter:
                print(f"      🎯 Aplicando filtro de edad en numerador: {edad_aplicable}")
        
        # 🔥 FILTRO DE SEXO usando configuración externa
        if keyword:
            sex_type = sex_filter_config.get_sex_filter_for_keyword(keyword)
            if sex_type:
                sex_filter = self._build_sex_filter(data_source, sex_type)
                if sex_filter:
                    sex_label = "MUJERES" if sex_type == 'F' else "HOMBRES"
                    print(f"      {'👩' if sex_type == 'F' else '👨'} Aplicando filtro de sexo en NUMERADOR: SOLO {sex_label}")
        
        # Filtros especiales según tipo
        if filter_type == 'tamizaje' and keyword and tamizaje_col:
            special_filter = self._build_tamizaje_filter(keyword, tamizaje_col)
        
        elif filter_type == 'metodo' and keyword and metodo_col:
            special_filter = self._build_metodo_filter(keyword, metodo_col)
        
        # Query con todos los filtros
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
              AND CAST({col_escaped} AS VARCHAR) LIKE '____-__-__'
              AND EXTRACT(YEAR FROM CAST(CAST({col_escaped} AS VARCHAR) AS DATE)) = {anio_corte}
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
              AND EXTRACT(YEAR FROM strptime(CAST({col_escaped} AS VARCHAR), '%d/%m/%Y')) = {anio_corte}
              AND strptime(CAST({col_escaped} AS VARCHAR), '%d/%m/%Y') <= DATE '{corte_fecha}'
            GROUP BY EXTRACT(MONTH FROM strptime(CAST({col_escaped} AS VARCHAR), '%d/%m/%Y'))
            ORDER BY mes
            """
        
        try:
            result = self.conn.execute(query).fetchall()
            
            numeradores = {m: 0 for m in range(1, 13)}
            
            for mes_num, total in result:
                if mes_num and 1 <= mes_num <= mes_limite:
                    numeradores[int(mes_num)] = int(total)
                    print(f"         ✓ Mes {int(mes_num):2d} ({anio_corte}): {int(total)} atenciones")
            
            return numeradores
            
        except Exception as e:
            print(f"         ❌ Error: {e}")
            import traceback
            traceback.print_exc()
            return {m: 0 for m in range(1, 13)}
    
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
    
    def _build_age_filter(self, edad_aplicable: str, data_source: str) -> str:
        """Construye filtro SQL de edad SOLO para AÑOS"""
        edad_lower = edad_aplicable.lower().strip()
        
        if 'año' not in edad_lower and 'ano' not in edad_lower:
            return ""
        
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
            print(f"      ⚠️ Error construyendo filtro de edad: {e}")
            return ""
        
        return ""
    
    # ============================================================
    # TAMIZAJE
    # ============================================================
    
    def _is_tamizaje_keyword(self, keyword: str) -> bool:
        """Detecta si es tamizaje"""
        keyword_norm = normalize_text(keyword)
        return any(ind in keyword_norm for ind in ['TAMIZAJE', 'CITOLOGIA', 'ADN-VPH', 'CUELLO UTERINO'])
    
    def _detect_tamizaje_column(self, data_source: str) -> Optional[str]:
        """Detecta columna de tamizaje"""
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
        """Construye filtro para tamizaje"""
        keyword_norm = normalize_text(keyword)
        
        if 'CITOLOGIA' in keyword_norm:
            return f'AND "{tamizaje_col}" IS NOT NULL AND (UPPER(CAST("{tamizaje_col}" AS VARCHAR)) LIKE \'%1 -%\' OR UPPER(CAST("{tamizaje_col}" AS VARCHAR)) LIKE \'%4 -%\')'
        
        if 'ADN' in keyword_norm or 'VPH' in keyword_norm:
            return f'AND "{tamizaje_col}" IS NOT NULL AND (UPPER(CAST("{tamizaje_col}" AS VARCHAR)) LIKE \'%2 -%\' OR UPPER(CAST("{tamizaje_col}" AS VARCHAR)) LIKE \'%4 -%\')'
        
        return f'AND "{tamizaje_col}" IS NOT NULL'
    
    def _debug_tamizaje_values(self, data_source, tamizaje_col, where_clause, keyword, formato, col_escaped, anio_corte, edad_aplicable=None):
        """Debug de tamizaje"""
        print(f"      🧪 Debug tamizaje activado para '{keyword}'")
    
    # ============================================================
    # MÉTODO ANTICONCEPTIVO
    # ============================================================
    
    def _is_metodo_anticonceptivo_keyword(self, keyword: str) -> bool:
        """Detecta si es método anticonceptivo"""
        keyword_norm = normalize_text(keyword)
        metodos = ['DIU', 'INTRAUTERINO', 'SUBDERMICO', 'IMPLANTE', 'ORAL', 'INYECTABLE', 'PRESERVATIVO', 'EMERGENCIA']
        return any(metodo in keyword_norm for metodo in metodos)
    
    def _detect_metodo_column(self, data_source: str) -> Optional[str]:
        """Detecta columna de método anticonceptivo"""
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
        """Construye filtro para método anticonceptivo"""
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
        """Debug de método anticonceptivo"""
        print(f"      🧪 Debug método anticonceptivo activado para '{keyword}'")
    
    # ============================================================
    # UTILIDADES
    # ============================================================
    
    def _detect_date_format(self, data_source: str, col_escaped: str, where_clause: str) -> str:
        """Detecta formato de fecha"""
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
