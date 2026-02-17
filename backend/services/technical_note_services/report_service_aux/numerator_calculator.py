# services/technical_note_services/report_service_aux/numerator_calculator.py

from typing import Dict, Any, Optional
import re
from services.duckdb_service.duckdb_service import duckdb_service
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
        
        Soporta filtros especiales:
        - Método anticonceptivo (DIU, Subdérmico)
        - Tamizaje de cáncer de cuello uterino (Citología, ADN-VPH) + filtro de edad + sexo F
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
        filter_type = None  # 'metodo', 'tamizaje', o None
        
        if keyword:
            keyword_lower = keyword.lower().strip()
            
            # Detectar si es tamizaje de cáncer de cuello uterino
            if self._is_tamizaje_keyword(keyword_lower):
                print(f"      🔍 Keyword: '{keyword}' → Filtro de Tamizaje Cáncer Cuello Uterino")
                tamizaje_col = self._detect_tamizaje_column(data_source)
                if tamizaje_col:
                    filter_type = 'tamizaje'
                    print(f"      ✓ Columna tamizaje: '{tamizaje_col}'")
                    print(f"      👩 Filtro de sexo: SOLO MUJERES (F)")
                    self._debug_tamizaje_values(
                        data_source, tamizaje_col, where_clause, keyword,
                        formato_detectado, col_escaped, anio_corte,
                        edad_aplicable
                    )
            
            # Detectar método anticonceptivo (DIU, Subdérmico)
            elif keyword_lower in ['diu', 'subdermico']:
                print(f"      🔍 Keyword: '{keyword}' → Filtro de Método Anticonceptivo")
                metodo_col = self._detect_metodo_column(data_source)
                if metodo_col:
                    filter_type = 'metodo'
                    print(f"      ✓ Columna método: '{metodo_col}'")
                    self._debug_metodo_values(
                        data_source, metodo_col, where_clause, keyword,
                        formato_detectado, col_escaped, anio_corte
                    )
        
        # 🔥 Contar por mes con filtro de edad + sexo
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
        """
        Cuenta registros por mes DEL AÑO ESPECÍFICO con filtros especiales
        """
        special_filter = ""
        age_filter = ""
        sex_filter = ""
        
        # 🔥 Filtros para TAMIZAJE (edad + sexo)
        if filter_type == 'tamizaje':
            # Filtro de edad
            if edad_aplicable:
                age_filter = self._build_age_filter(edad_aplicable, data_source)
                if age_filter:
                    print(f"      🎯 Aplicando filtro de edad en numerador: {edad_aplicable}")
            
            # 🔥 NUEVO: Filtro de SEXO (solo mujeres)
            sex_filter = self._build_sex_filter(data_source)
            if sex_filter:
                print(f"      👩 Aplicando filtro de sexo: SOLO MUJERES")
            
            # Filtro de tipo de tamizaje
            if keyword and tamizaje_col:
                special_filter = self._build_tamizaje_filter(keyword, tamizaje_col)
        
        # Filtro de método anticonceptivo
        elif filter_type == 'metodo' and keyword and metodo_col:
            kw = keyword.lower().strip()
            if kw == "diu":
                special_filter = f"""
                  AND "{metodo_col}" IS NOT NULL
                  AND (
                      UPPER(CAST("{metodo_col}" AS VARCHAR)) LIKE '%DIU%'
                      OR UPPER(CAST("{metodo_col}" AS VARCHAR)) LIKE '%DISPOSITIVO%'
                      OR UPPER(CAST("{metodo_col}" AS VARCHAR)) LIKE '%INTRAUTERINO%'
                  )
                """
            elif kw == "subdermico":
                special_filter = f"""
                  AND "{metodo_col}" IS NOT NULL
                  AND (
                      UPPER(CAST("{metodo_col}" AS VARCHAR)) LIKE '%IMPLANTE%'
                      OR UPPER(CAST("{metodo_col}" AS VARCHAR)) LIKE '%SUBDERMICO%'
                      OR UPPER(CAST("{metodo_col}" AS VARCHAR)) LIKE '%SUBDERMIC%'
                  )
                """
        
        # Query con filtro de AÑO específico + EDAD + SEXO
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
            
            # Inicializar todos los meses en 0
            numeradores = {m: 0 for m in range(1, 13)}
            
            # Llenar con valores reales
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
    # NUEVO: FILTRO DE SEXO
    # ============================================================
    
    def _build_sex_filter(self, data_source: str) -> str:
        """
        Construye filtro SQL para SEXO = 'F' (solo mujeres).
        """
        try:
            describe_query = f"DESCRIBE SELECT * FROM {data_source}"
            cols = [r[0] for r in self.conn.execute(describe_query).fetchall()]
            
            print(f"      🔍 DEBUG - Buscando columna de sexo...")
            print(f"      📋 Columnas disponibles (primeras 15): {cols[:15]}")
            
            # Buscar columna de sexo
            sex_col = None
            for col in cols:
                if col in ["Sexo", "Genero", "Género", "Sexo Biológico", "Sexo Biologico"]:
                    sex_col = col
                    print(f"      ✅ Columna de sexo encontrada: '{sex_col}'")
                    break
            
            if not sex_col:
                # Buscar por normalización
                cols_norm = {normalize_text(c): c for c in cols}
                for candidate in ["Sexo", "Genero", "Género", "Sexo Biológico", "Sexo Biologico"]:
                    cand_norm = normalize_text(candidate)
                    if cand_norm in cols_norm:
                        sex_col = cols_norm[cand_norm]
                        print(f"      ✅ Columna de sexo encontrada (normalizada): '{sex_col}'")
                        break
            
            if sex_col:
                # 🔥 DEBUG: Ver qué valores tiene la columna de sexo
                debug_query = f"""
                SELECT DISTINCT CAST("{sex_col}" AS VARCHAR) as sexo_valor, 
                    COUNT(*) as total
                FROM {data_source}
                WHERE "{sex_col}" IS NOT NULL
                GROUP BY CAST("{sex_col}" AS VARCHAR)
                ORDER BY total DESC
                LIMIT 10
                """
                
                try:
                    sex_values = self.conn.execute(debug_query).fetchall()
                    print(f"      📊 Valores en columna '{sex_col}':")
                    for val, count in sex_values:
                        print(f"         - '{val}' ({count} registros)")
                except Exception as e:
                    print(f"      ⚠️ Error obteniendo valores de sexo: {e}")
                
                # 🔥 Construir filtro más flexible
                return f"""
                AND "{sex_col}" IS NOT NULL
                AND (
                    UPPER(CAST("{sex_col}" AS VARCHAR)) = 'F'
                    OR UPPER(CAST("{sex_col}" AS VARCHAR)) LIKE '%FEMENINO%'
                    OR UPPER(CAST("{sex_col}" AS VARCHAR)) LIKE '%MUJER%'
                )
                """
            else:
                print("      ❌ NO se encontró columna de sexo")
                print(f"      📋 Todas las columnas ({len(cols)} total):")
                for i, col in enumerate(cols[:30], 1):
                    print(f"         {i}. '{col}'")
                if len(cols) > 30:
                    print(f"         ... y {len(cols) - 30} columnas más")
                return ""
        
        except Exception as e:
            print(f"      ❌ Error construyendo filtro de sexo: {e}")
            import traceback
            traceback.print_exc()
            return ""

    
    # ============================================================
    # FILTRO DE EDAD
    # ============================================================
    
    def _build_age_filter(self, edad_aplicable: str, data_source: str) -> str:
        """
        Construye filtro SQL de edad SOLO para AÑOS.
        
        Soporta:
        - Años individuales: "25 Años" → CAST("Edad" AS INTEGER) = 25
        - Rangos de años: "25 a 28 Años" → CAST("Edad" AS INTEGER) IN (25,26,27,28)
        - Meses: Retorna "" (no aplica filtro, se maneja con fecha de nacimiento)
        """
        edad_lower = edad_aplicable.lower().strip()
        
        # Detectar si son años o meses
        if 'año' not in edad_lower and 'ano' not in edad_lower:
            # ES MESES - No aplicar filtro aquí
            return ""
        
        # ES AÑOS
        numeros = re.findall(r'(\d+)', edad_aplicable)
        
        # Detectar columna de edad
        try:
            describe_query = f"DESCRIBE SELECT * FROM {data_source}"
            cols = [r[0] for r in self.conn.execute(describe_query).fetchall()]
            edad_col = None
            for col in cols:
                if col == "Edad":
                    edad_col = col
                    break
            
            if not edad_col:
                print("      ⚠️ No se encontró columna 'Edad'")
                return ""
            
            if len(numeros) == 1:
                # Edad única
                edad_valor = int(numeros[0])
                return f'AND CAST("{edad_col}" AS INTEGER) = {edad_valor}'
            elif len(numeros) == 2:
                # Rango de edad
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
    # MÉTODOS PARA TAMIZAJE DE CÁNCER DE CUELLO UTERINO
    # ============================================================
    
    def _is_tamizaje_keyword(self, keyword: str) -> bool:
        """Detecta si la keyword es de tamizaje de cáncer de cuello uterino"""
        keyword_norm = normalize_text(keyword)
        
        tamizaje_indicators = [
            'TAMIZAJE',
            'CITOLOGIA',
            'ADN-VPH',
            'CANALIZACION',
            'CUELLO UTERINO'
        ]
        
        return any(indicator in keyword_norm for indicator in tamizaje_indicators)
    
    def _detect_tamizaje_column(self, data_source: str) -> Optional[str]:
        """Detecta columna de tamizaje de cáncer de cuello uterino"""
        try:
            describe_query = f"DESCRIBE SELECT * FROM {data_source}"
            cols = [r[0] for r in self.conn.execute(describe_query).fetchall()]
            
            exact_matches = [
                "Tamizaje Cáncer de Cuello Uterino",
                "Tamizaje Cancer de Cuello Uterino",
                "Tamizaje Cáncer Cuello Uterino",
                "Tamizaje Cancer Cuello Uterino"
            ]
            
            for col in cols:
                if col in exact_matches:
                    return col
            
            cols_norm = {normalize_text(c): c for c in cols}
            for candidate in exact_matches:
                cand_norm = normalize_text(candidate)
                if cand_norm in cols_norm:
                    return cols_norm[cand_norm]
            
            return None
        except:
            return None
    
    def _build_tamizaje_filter(self, keyword: str, tamizaje_col: str) -> str:
        """Construye filtro SQL para tamizaje según la keyword"""
        keyword_norm = normalize_text(keyword)
        
        conditions = []
        
        if 'CITOLOGIA CONVENCIONAL' in keyword_norm:
            conditions.append("UPPER(CAST(\"{col}\" AS VARCHAR)) LIKE '%1 -%'")
            conditions.append("UPPER(CAST(\"{col}\" AS VARCHAR)) LIKE '%CITOLOGIA%'")
        elif 'CITOLOGIA' in keyword_norm:
            conditions.append("(UPPER(CAST(\"{col}\" AS VARCHAR)) LIKE '%1 -%' OR UPPER(CAST(\"{col}\" AS VARCHAR)) LIKE '%4 -%')")
        
        if 'ADN' in keyword_norm or 'VPH' in keyword_norm:
            if 'CANALIZACION' in keyword_norm:
                conditions.append("(UPPER(CAST(\"{col}\" AS VARCHAR)) LIKE '%2 -%' OR UPPER(CAST(\"{col}\" AS VARCHAR)) LIKE '%4 -%')")
            else:
                conditions.append("(UPPER(CAST(\"{col}\" AS VARCHAR)) LIKE '%2 -%' OR UPPER(CAST(\"{col}\" AS VARCHAR)) LIKE '%4 -%')")
        
        if 'INSPECCION' in keyword_norm or 'VISUAL' in keyword_norm:
            conditions.append("UPPER(CAST(\"{col}\" AS VARCHAR)) LIKE '%3 -%'")
        
        if not conditions:
            conditions.append("CAST(\"{col}\" AS VARCHAR) IS NOT NULL")
            conditions.append("CAST(\"{col}\" AS VARCHAR) != ''")
            conditions.append("CAST(\"{col}\" AS VARCHAR) NOT LIKE '%(Vacías)%'")
        
        if len(conditions) == 1:
            combined_filter = conditions[0].replace('{col}', tamizaje_col)
        else:
            filter_parts = [cond.replace('{col}', tamizaje_col) for cond in conditions]
            combined_filter = " AND ".join(filter_parts)
        
        return f"""
          AND "{tamizaje_col}" IS NOT NULL
          AND {combined_filter}
        """
    
    def _debug_tamizaje_values(
        self,
        data_source: str,
        tamizaje_col: str,
        where_clause: str,
        keyword: str,
        formato: str,
        col_escaped: str,
        anio_corte: int,
        edad_aplicable: str = None
    ):
        """Debug mostrando valores de tamizaje DEL AÑO ESPECÍFICO con filtro de edad + sexo"""
        try:
            # Filtro de edad
            age_filter = ""
            if edad_aplicable:
                age_filter = self._build_age_filter(edad_aplicable, data_source)
            
            # Filtro de sexo
            sex_filter = self._build_sex_filter(data_source)
            
            # 🔥 Filtro base para fechas NO VACÍAS
            if formato == 'yyyy-mm-dd':
                base_date_filter = f"""
                AND {col_escaped} IS NOT NULL
                AND CAST({col_escaped} AS VARCHAR) != ''
                AND CAST({col_escaped} AS VARCHAR) LIKE '____-__-__'
                AND EXTRACT(YEAR FROM CAST(CAST({col_escaped} AS VARCHAR) AS DATE)) = {anio_corte}
                """
            else:
                base_date_filter = f"""
                AND {col_escaped} IS NOT NULL
                AND CAST({col_escaped} AS VARCHAR) != ''
                AND LENGTH(CAST({col_escaped} AS VARCHAR)) >= 8
                AND EXTRACT(YEAR FROM strptime(CAST({col_escaped} AS VARCHAR), '%d/%m/%Y')) = {anio_corte}
                """
            
            # 🔥 COMPARACIÓN: Contar SIN filtro de sexo vs CON filtro de sexo
            print(f"      🧪 PRUEBA: Comparando conteos con/sin filtro de sexo...")
            
            query_sin_sexo = f"""
            SELECT COUNT(DISTINCT "Nro Identificación") as total
            FROM {data_source}
            WHERE {where_clause}
            {age_filter}
            AND "{tamizaje_col}" IS NOT NULL
            {base_date_filter}
            """
            
            try:
                result_sin_sexo = self.conn.execute(query_sin_sexo).fetchone()
                total_sin_sexo = result_sin_sexo[0] if result_sin_sexo else 0
                print(f"      📊 Total SIN filtro de sexo: {total_sin_sexo}")
            except Exception as e:
                print(f"      ⚠️ Error contando sin sexo: {e}")
                total_sin_sexo = -1
            
            query_con_sexo = f"""
            SELECT COUNT(DISTINCT "Nro Identificación") as total
            FROM {data_source}
            WHERE {where_clause}
            {age_filter}
            {sex_filter}
            AND "{tamizaje_col}" IS NOT NULL
            {base_date_filter}
            """
            
            try:
                result_con_sexo = self.conn.execute(query_con_sexo).fetchone()
                total_con_sexo = result_con_sexo[0] if result_con_sexo else 0
                print(f"      📊 Total CON filtro de sexo: {total_con_sexo}")
                
                if total_sin_sexo > 0:
                    if total_sin_sexo == total_con_sexo:
                        print(f"      🚨 🚨 🚨 ALERTA: Totales IGUALES - Filtro NO funciona 🚨 🚨 🚨")
                    elif total_con_sexo < total_sin_sexo:
                        reduccion = total_sin_sexo - total_con_sexo
                        porcentaje = (reduccion / total_sin_sexo * 100) if total_sin_sexo > 0 else 0
                        print(f"      ✅ Filtro funcionando: {reduccion} registros filtrados ({porcentaje:.1f}%)")
                        print(f"         Antes: {total_sin_sexo} → Después: {total_con_sexo}")
            except Exception as e:
                print(f"      ⚠️ Error contando con sexo: {e}")
            
            # Detalle por tipo de tamizaje (CON filtro de sexo)
            query = f"""
            SELECT "{tamizaje_col}", COUNT(DISTINCT "Nro Identificación") as total
            FROM {data_source}
            WHERE {where_clause}
            {age_filter}
            {sex_filter}
            AND "{tamizaje_col}" IS NOT NULL
            AND CAST("{tamizaje_col}" AS VARCHAR) != ''
            {base_date_filter}
            GROUP BY "{tamizaje_col}"
            ORDER BY total DESC
            LIMIT 20
            """
            
            result = self.conn.execute(query).fetchall()
            if result:
                edad_msg = f" (edad: {edad_aplicable})" if edad_aplicable else ""
                print(f"      📋 Detalle de tamizajes en {anio_corte}{edad_msg} [CON FILTROS]:")
                keyword_norm = normalize_text(keyword)
                matches = 0
                
                for tamizaje_val, count in result:
                    tamizaje_upper = normalize_text(str(tamizaje_val))
                    match = self._check_tamizaje_match(keyword_norm, tamizaje_upper)
                    
                    marker = " ← ✓ COINCIDE" if match else ""
                    print(f"         - '{tamizaje_val}' ({count}){marker}")
                    if match:
                        matches += count
                
                if matches > 0:
                    print(f"      ✅ Total registros que coinciden: {matches}")
                else:
                    print(f"      ❌ NO hay registros de '{keyword}' en {anio_corte}")
            else:
                print(f"      ⚠️ No hay tamizajes en {anio_corte}")
        except Exception as e:
            print(f"         ❌ Error debug: {e}")
            import traceback
            traceback.print_exc()


    
    def _check_tamizaje_match(self, keyword_norm: str, tamizaje_norm: str) -> bool:
        """Verifica si un valor de tamizaje coincide con la keyword"""
        if 'CITOLOGIA CONVENCIONAL' in keyword_norm:
            return '1 -' in tamizaje_norm or ('CITOLOGIA' in tamizaje_norm and '4 -' not in tamizaje_norm)
        
        if 'CITOLOGIA' in keyword_norm and 'CONVENCIONAL' not in keyword_norm:
            return '1 -' in tamizaje_norm or '4 -' in tamizaje_norm
        
        if 'ADN' in keyword_norm or 'VPH' in keyword_norm:
            return '2 -' in tamizaje_norm or '4 -' in tamizaje_norm
        
        if 'INSPECCION' in keyword_norm or 'VISUAL' in keyword_norm:
            return '3 -' in tamizaje_norm
        
        if 'CANALIZACION' in keyword_norm:
            return '2 -' in tamizaje_norm or '4 -' in tamizaje_norm
        
        return False
    
    # ============================================================
    # MÉTODOS PARA MÉTODO ANTICONCEPTIVO (DIU, SUBDÉRMICO)
    # ============================================================
    
    def _debug_metodo_values(
        self,
        data_source: str,
        metodo_col: str,
        where_clause: str,
        keyword: str,
        formato: str,
        col_escaped: str,
        anio_corte: int
    ):
        """Debug mostrando valores de método anticonceptivo DEL AÑO ESPECÍFICO"""
        try:
            if formato == 'yyyy-mm-dd':
                year_filter = f"AND EXTRACT(YEAR FROM CAST(CAST({col_escaped} AS VARCHAR) AS DATE)) = {anio_corte}"
            else:
                year_filter = f"AND EXTRACT(YEAR FROM strptime(CAST({col_escaped} AS VARCHAR), '%d/%m/%Y')) = {anio_corte}"
            
            query = f"""
            SELECT "{metodo_col}", COUNT(DISTINCT "Nro Identificación") as total
            FROM {data_source}
            WHERE {where_clause}
              AND "{metodo_col}" IS NOT NULL
              AND CAST("{metodo_col}" AS VARCHAR) != ''
              {year_filter}
            GROUP BY "{metodo_col}"
            ORDER BY total DESC
            LIMIT 10
            """
            
            result = self.conn.execute(query).fetchall()
            if result:
                print(f"      📋 Métodos anticonceptivos en {anio_corte}:")
                kw = keyword.lower().strip()
                matches = 0
                for metodo_val, count in result:
                    metodo_upper = str(metodo_val).upper()
                    match = False
                    if kw == 'diu':
                        match = 'DIU' in metodo_upper or 'DISPOSITIVO' in metodo_upper or 'INTRAUTERINO' in metodo_upper
                    elif kw == 'subdermico':
                        match = 'IMPLANTE' in metodo_upper or 'SUBDERMICO' in metodo_upper
                    
                    marker = " ← ✓ COINCIDE" if match else ""
                    print(f"         - '{metodo_val}' ({count}){marker}")
                    if match:
                        matches += count
                
                if matches > 0:
                    print(f"      ✅ Total registros que coinciden: {matches}")
                else:
                    print(f"      ❌ NO hay registros de '{keyword}' en {anio_corte}")
            else:
                print(f"      ⚠️ No hay métodos anticonceptivos en {anio_corte}")
        except Exception as e:
            print(f"         Error debug: {e}")
    
    def _detect_metodo_column(self, data_source: str) -> Optional[str]:
        """Detecta columna de método anticonceptivo"""
        try:
            describe_query = f"DESCRIBE SELECT * FROM {data_source}"
            cols = [r[0] for r in self.conn.execute(describe_query).fetchall()]
            
            for col in cols:
                if col == "Método Anticonceptivo":
                    return col
            
            cols_norm = {normalize_text(c): c for c in cols}
            for cand in ['Método Anticonceptivo', 'Metodo Anticonceptivo']:
                if normalize_text(cand) in cols_norm:
                    return cols_norm[normalize_text(cand)]
            return None
        except:
            return None
    
    # ============================================================
    # UTILIDADES
    # ============================================================
    
    def _detect_date_format(
        self,
        data_source: str,
        col_escaped: str,
        where_clause: str
    ) -> str:
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
