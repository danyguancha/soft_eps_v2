# services/technical_note_services/report_service_aux/denominator_calculator.py

from typing import Dict, Any
import re
from utils.text_normalizer import normalize_text
from services.duckdb_service.duckdb_service import duckdb_service


class PopulationCalculator:
    """Responsable de calcular población (denominadores)"""
    
    def __init__(
        self,
        birth_date_ranges_den: Dict[str, Dict[int, tuple]],
        rangos_compuestos: Dict[str, Any],
        corrected_months,
        corrected_years,
        identity_document
    ):
        self.birth_date_ranges_den = birth_date_ranges_den
        self.rangos_compuestos = rangos_compuestos
        self.corrected_months = corrected_months
        self.corrected_years = corrected_years
        self.identity_document = identity_document
    
    @property
    def conn(self):
        """Obtiene conexión dinámicamente"""
        return duckdb_service.conn
    
    def get_population_by_predefined_dates(
        self,
        data_source: str,
        where_clause: str,
        edad_key: str,
        corte_fecha: str = None,
        keyword: str = None  # ← NUEVO PARÁMETRO
    ) -> Dict[int, int]:
        """
        Calcula población (DENOMINADOR) con filtro de edad.
        Soporta rangos compuestos con normalización.
        Aplica filtro de sexo para tamizajes de cáncer de cuello uterino.
        """
        if not edad_key:
            print("   edad_key está vacío")
            return {i: 0 for i in range(1, 13)}
        
        edad_norm = normalize_text(edad_key)
        
        if edad_norm in self.rangos_compuestos:
            print(f"   Rango compuesto detectado: {edad_key}")
            return self._get_population_for_composite_range(
                data_source, where_clause, edad_norm, corte_fecha, keyword  # ← PASAR keyword
            )
        
        edad_lower = edad_key.lower().strip()
        edad_sin_tildes = edad_lower.replace('ñ', 'n')
        
        print(f"   Edad recibida: '{edad_key}'")
        
        if 'ano' in edad_sin_tildes or 'año' in edad_lower:
            print("   Detectada palabra 'año' - ES AÑOS")
            
            numeros = re.findall(r'(\d+)', edad_sin_tildes)
            
            if len(numeros) == 1:
                anios_valor = int(numeros[0])
                print(f"   DETECTADO: {anios_valor} años (único)")
                return self._get_population_for_years(
                    data_source, where_clause, anios_valor, corte_fecha, keyword  # ← PASAR keyword
                )
            elif len(numeros) == 2:
                anio_min = int(numeros[0])
                anio_max = int(numeros[1])
                print(f"   DETECTADO: {anio_min} a {anio_max} años (rango)")
                return self._get_population_for_year_range(
                    data_source, where_clause, anio_min, anio_max, corte_fecha, keyword  # ← PASAR keyword
                )
            else:
                print(f"   ⚠️ Formato de años no reconocido: {numeros}")
                return {-1: 0}
        
        print("   No contiene 'año' - ES MESES")
        return self._get_population_for_months(
            data_source, where_clause, edad_key
        )

    
    def _get_population_for_months(
        self,
        data_source: str,
        where_clause: str,
        edad_key: str
    ) -> Dict[int, int]:
        """Calcula población para rangos de meses"""
        
        edad_key_norm = self._normalize_age_key(edad_key)
        
        if not edad_key_norm:
            print(f"      ⚠️ No se pudo normalizar: '{edad_key}'")
            print(f"      📋 Rangos disponibles en archivo de fechas:")
            for idx, key in enumerate(list(self.birth_date_ranges_den.keys())[:10], 1):
                key_normalized = normalize_text(key)
                print(f"         [{idx}] '{key}' → '{key_normalized}'")
            if len(self.birth_date_ranges_den) > 10:
                print(f"         ... y {len(self.birth_date_ranges_den) - 10} más")
            
            print(f"      🔍 Buscando variaciones del rango:")
            edad_norm_input = normalize_text(edad_key)
            # Extraer números del input
            numeros_input = re.findall(r'\d+', edad_key)
            if numeros_input:
                for key in self.birth_date_ranges_den.keys():
                    numeros_key = re.findall(r'\d+', key)
                    if numeros_input == numeros_key:
                        print(f"         ✓ Posible coincidencia: '{key}'")
            
            return {i: 0 for i in range(1, 13)}
        
        if edad_key_norm not in self.birth_date_ranges_den:
            print(f"      ⚠️ '{edad_key_norm}' no está en birth_date_ranges_den después de normalizar")
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
        print(f"      Total población (meses): {total}")
        
        return poblaciones
    
    def _get_population_for_year_range(
        self,
        data_source: str,
        where_clause: str,
        anio_min: int,
        anio_max: int,
        corte_fecha: str,
        keyword: str = None  # ← NUEVO PARÁMETRO
    ) -> Dict[int, int]:
        """
        Calcula población para un RANGO DE AÑOS con filtro de sexo si es tamizaje.
        """
        print(f"      BUSCANDO POBLACIÓN DE {anio_min} A {anio_max} AÑOS")
        
        # 🔥 NUEVO: Detectar si es tamizaje y construir filtro de sexo
        sex_filter = ""
        if keyword:
            keyword_lower = keyword.lower().strip()
            if self._is_tamizaje_keyword(keyword_lower):
                sex_filter = self._build_sex_filter(data_source)
                if sex_filter:
                    print(f"      👩 Aplicando filtro de sexo en DENOMINADOR: SOLO MUJERES")
        
        edad_field = self.corrected_years.get_age_years_field_corrected(data_source)
        
        if not edad_field:
            print("      No se pudo obtener columna de edad")
            return {-1: 0}
        
        document_field = self.identity_document.get_document_field(data_source)
        
        # Generar lista de años del rango
        anios_list = list(range(anio_min, anio_max + 1))
        anios_str = ','.join(map(str, anios_list))
        
        query = f"""
        SELECT COUNT(DISTINCT {document_field})
        FROM {data_source}
        WHERE {where_clause}
        {sex_filter}
        AND "Fecha Nacimiento" IS NOT NULL
        AND {edad_field} IS NOT NULL
        AND CAST({edad_field} AS INTEGER) IN ({anios_str})
        """
        
        try:
            result = self.conn.execute(query).fetchone()
            count = int(result[0]) if result else 0
            
            sex_msg = " (SOLO MUJERES)" if sex_filter else ""
            print(f"      POBLACIÓN {anio_min}-{anio_max} AÑOS{sex_msg}: {count}")
            print(f"         Edades incluidas: {anios_list}")
            
            return {-1: count}
        
        except Exception as e:
            print(f"      Error: {e}")
            return {-1: 0}

    
    def _get_population_for_composite_range(
        self,
        data_source: str,
        where_clause: str,
        edad_key_norm: str,
        corte_fecha: str,
        keyword: str = None  # ← NUEVO PARÁMETRO
    ) -> Dict[int, int]:
        """
        Calcula población para rangos compuestos (meses + años).
        Aplica filtro de sexo si es tamizaje de cáncer de cuello uterino.
        """
        
        rango_info = self.rangos_compuestos[edad_key_norm]
        
        print("      Procesando rango compuesto:")
        print(f"         Meses: {rango_info.get('incluye_meses', [])[:5] if rango_info.get('incluye_meses') else 'Ninguno'}...")
        print(f"         Años: {rango_info.get('incluye_anios', [])}")
        
        # 🔥 NUEVO: Detectar si es tamizaje y construir filtro de sexo
        sex_filter = ""
        if keyword:
            keyword_lower = keyword.lower().strip()
            if self._is_tamizaje_keyword(keyword_lower):
                sex_filter = self._build_sex_filter(data_source)
                if sex_filter:
                    print(f"      👩 Aplicando filtro de sexo en DENOMINADOR: SOLO MUJERES")
        
        poblacion_total = 0
        
        # Calcular población para MESES
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
            {sex_filter}
            AND "Fecha Nacimiento" IS NOT NULL
            AND ({edad_meses_expr}) IN ({meses_str})
            """
            
            try:
                result = self.conn.execute(query).fetchone()
                pob_meses = int(result[0]) if result else 0
                poblacion_total += pob_meses
                
                sex_msg = " (SOLO MUJERES)" if sex_filter else ""
                print(f"         Población (meses){sex_msg}: {pob_meses}")
            except Exception as e:
                print(f"         Error calculando población de meses: {e}")
                pob_meses = 0
        
        # Calcular población para AÑOS
        if 'incluye_anios' in rango_info and rango_info['incluye_anios']:
            anios_list = rango_info['incluye_anios']
            anios_str = ','.join(map(str, anios_list))
            
            edad_field = self.corrected_years.get_age_years_field_corrected(data_source)
            
            if edad_field:
                query = f"""
                SELECT COUNT(DISTINCT "Nro Identificación")
                FROM {data_source}
                WHERE {where_clause}
                {sex_filter}
                AND "Fecha Nacimiento" IS NOT NULL
                AND CAST({edad_field} AS INTEGER) IN ({anios_str})
                """
                
                try:
                    result = self.conn.execute(query).fetchone()
                    pob_anios = int(result[0]) if result else 0
                    poblacion_total += pob_anios
                    
                    sex_msg = " (SOLO MUJERES)" if sex_filter else ""
                    print(f"         Población (años){sex_msg}: {pob_anios}")
                except Exception as e:
                    print(f"         Error calculando población de años: {e}")
                    pob_anios = 0
        
        sex_msg = " (SOLO MUJERES)" if sex_filter else ""
        print(f"      Población total (compuesto){sex_msg}: {poblacion_total}")
        
        return {-1: poblacion_total}

    
    def _get_population_for_years(
        self,
        data_source: str,
        where_clause: str,
        anios_valor: int,
        corte_fecha: str,
        keyword: str = None  # ← NUEVO PARÁMETRO
    ) -> Dict[int, int]:
        """
        Calcula población para UN AÑO específico.
        Aplica filtro de sexo si es tamizaje de cáncer de cuello uterino.
        """
        print(f"      BUSCANDO POBLACIÓN DE {anios_valor} AÑOS")
        
        # 🔥 NUEVO: Detectar si es tamizaje y construir filtro de sexo
        sex_filter = ""
        if keyword:
            keyword_lower = keyword.lower().strip()
            if self._is_tamizaje_keyword(keyword_lower):
                sex_filter = self._build_sex_filter(data_source)
                if sex_filter:
                    print(f"      👩 Aplicando filtro de sexo en DENOMINADOR: SOLO MUJERES")
        
        edad_field = self.corrected_years.get_age_years_field_corrected(data_source)
        
        if not edad_field:
            print("      No se pudo obtener columna de edad")
            return {-1: 0}
        
        print(f"      ✓ Columna 'Edad' encontrada en el dataset")
        
        document_field = self.identity_document.get_document_field(data_source)
        
        query = f"""
        SELECT COUNT(DISTINCT {document_field})
        FROM {data_source}
        WHERE {where_clause}
        {sex_filter}
        AND "Fecha Nacimiento" IS NOT NULL
        AND {edad_field} IS NOT NULL
        AND CAST({edad_field} AS INTEGER) = {anios_valor}
        """
        
        try:
            result = self.conn.execute(query).fetchone()
            count = int(result[0]) if result else 0
            
            sex_msg = " (SOLO MUJERES)" if sex_filter else ""
            print(f"      POBLACIÓN {anios_valor} AÑOS{sex_msg}: {count}")
            
            return {-1: count}
        
        except Exception as e:
            print(f"      Error: {e}")
            return {-1: 0}

        
    def _is_tamizaje_keyword(self, keyword: str) -> bool:
        """Detecta si la keyword es de tamizaje de cáncer de cuello uterino"""
        from utils.text_normalizer import normalize_text
        keyword_norm = normalize_text(keyword)
        
        tamizaje_indicators = [
            'TAMIZAJE',
            'CITOLOGIA',
            'ADN-VPH',
            'CANALIZACION',
            'CUELLO UTERINO'
        ]
        
        return any(indicator in keyword_norm for indicator in tamizaje_indicators)

    def _build_sex_filter(self, data_source: str) -> str:
        """Construye filtro SQL para SEXO = 'F' (solo mujeres)"""
        from utils.text_normalizer import normalize_text
        
        try:
            describe_query = f"DESCRIBE SELECT * FROM {data_source}"
            cols = [r[0] for r in self.conn.execute(describe_query).fetchall()]
            
            # Buscar columna de sexo
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
                return f"""
                AND "{sex_col}" IS NOT NULL
                AND (
                    UPPER(CAST("{sex_col}" AS VARCHAR)) = 'F'
                    OR UPPER(CAST("{sex_col}" AS VARCHAR)) LIKE '%FEMENINO%'
                    OR UPPER(CAST("{sex_col}" AS VARCHAR)) LIKE '%MUJER%'
                )
                """
            else:
                return ""
        
        except Exception as e:
            print(f"      ⚠️ Error construyendo filtro de sexo: {e}")
            return ""

    
    def _normalize_age_key(self, edad_str: str) -> str:
        """
        Normaliza clave de edad con FUZZY MATCHING.
        
        Intenta múltiples estrategias:
        1. Coincidencia exacta normalizada
        2. Coincidencia por números (ej: "18 a 24" coincide con "18 a 24")
        3. Variaciones comunes (Mes/Meses, capitalización)
        """
        if not edad_str:
            return ""
        
        edad_norm = normalize_text(edad_str)
        
        # Estrategia 1: Coincidencia exacta normalizada
        for key in self.birth_date_ranges_den.keys():
            key_norm = normalize_text(key)
            if edad_norm == key_norm:
                return key
        
        # Estrategia 2: Coincidencia por números
        # Extraer números del input
        numeros_input = re.findall(r'\d+', edad_str)
        
        if numeros_input:
            for key in self.birth_date_ranges_den.keys():
                numeros_key = re.findall(r'\d+', key)
                
                # Si los números coinciden exactamente
                if numeros_input == numeros_key:
                    # Verificar que ambos hablen de la misma unidad (meses)
                    input_lower = edad_str.lower()
                    key_lower = key.lower()
                    
                    # Ambos deben tener "mes" o "meses"
                    if ('mes' in input_lower and 'mes' in key_lower):
                        print(f"      🔍 Fuzzy match: '{edad_str}' → '{key}' (por números)")
                        return key
        
        # Estrategia 3: Variaciones comunes
        # Normalizar variaciones: "1 mes" → "1 mes", "1 Mes" → "1 mes"
        edad_variations = [
            edad_str,
            edad_str.lower(),
            edad_str.title(),
            edad_str.replace('Mes', 'mes').replace('Meses', 'meses'),
            edad_str.replace('mes', 'Mes').replace('meses', 'Meses'),
        ]
        
        for variation in edad_variations:
            if variation in self.birth_date_ranges_den:
                print(f"      🔍 Match por variación: '{edad_str}' → '{variation}'")
                return variation
        
        return ""
    
    def filter_mappings_by_population_availability(
        self,
        all_mappings: list,
        data_source: str,
        where_clause: str,
        corte_fecha: str
    ) -> list:
        """Filtra mappings verificando si hay población disponible"""
        print(f"      Filtrando {len(all_mappings)} mappings por población...")
        
        valid_mappings = []
        
        for mapping in all_mappings:
            consolidado_info = mapping.get('consolidado', {})
            edad_aplicable = consolidado_info.get('edad_aplicable', '')
            
            if not edad_aplicable:
                continue
            
            try:
                poblaciones = self.get_population_by_predefined_dates(
                    data_source=data_source,
                    where_clause=where_clause,
                    edad_key=edad_aplicable,
                    corte_fecha=corte_fecha
                )
                
                if -1 in poblaciones:
                    total_poblacion = poblaciones[-1]
                else:
                    total_poblacion = sum(poblaciones.values())
                
                if total_poblacion > 0:
                    print(f"         {edad_aplicable}: {total_poblacion} personas")
                    valid_mappings.append(mapping)
                else:
                    print(f"         {edad_aplicable}: 0 personas (omitido)")
            
            except Exception as e:
                print(f"         Error verificando {edad_aplicable}: {e}")
                continue
        
        print(f"   {len(valid_mappings)} mappings con población > 0")
        return valid_mappings
