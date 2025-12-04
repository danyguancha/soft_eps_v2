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
        corte_fecha: str = None
    ) -> Dict[int, int]:
        """
        Calcula población (DENOMINADOR) con filtro de edad.
        Soporta rangos compuestos con normalización.
        """
        if not edad_key:
            print("   edad_key está vacío")
            return {i: 0 for i in range(1, 13)}
        
        edad_norm = normalize_text(edad_key)
        
        if edad_norm in self.rangos_compuestos:
            print(f"   Rango compuesto detectado: {edad_key}")
            return self._get_population_for_composite_range(
                data_source, where_clause, edad_norm, corte_fecha
            )
        
        edad_lower = edad_key.lower().strip()
        edad_sin_tildes = edad_lower.replace('ñ', 'n')
        
        print(f"   Edad recibida: '{edad_key}'")
        
        if 'ano' in edad_sin_tildes or 'año' in edad_lower:
            print("   Detectada palabra 'año' - ES AÑOS")
            match = re.search(r'(\d+)', edad_sin_tildes)
            if match:
                anios_valor = int(match.group(1))
                print(f"   DETECTADO: {anios_valor} años")
                return self._get_population_for_years(
                    data_source, where_clause, anios_valor, corte_fecha
                )
        
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
            print(f"      No se pudo normalizar: '{edad_key}'")
            return {i: 0 for i in range(1, 13)}
        
        if edad_key_norm not in self.birth_date_ranges_den:
            print(f"      '{edad_key_norm}' no está en birth_date_ranges_den")
            return {i: 0 for i in range(1, 13)}
        
        print(f"      Usando rango: '{edad_key_norm}'")
        
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
    
    def _get_population_for_composite_range(
        self,
        data_source: str,
        where_clause: str,
        edad_key_norm: str,
        corte_fecha: str
    ) -> Dict[int, int]:
        """Calcula población para rangos compuestos (meses + años)"""
        
        rango_info = self.rangos_compuestos[edad_key_norm]
        
        print("      Procesando rango compuesto:")
        print(f"         Meses: {rango_info.get('incluye_meses', [])[:5] if rango_info.get('incluye_meses') else 'Ninguno'}...")
        print(f"         Años: {rango_info.get('incluye_anios', [])}")
        
        poblacion_total = 0
        
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
        
        print(f"      Población total (compuesto): {poblacion_total}")
        
        return {-1: poblacion_total}
    
    def _get_population_for_years(
        self,
        data_source: str,
        where_clause: str,
        anios: int,
        corte_fecha: str
    ) -> Dict[int, int]:
        """Calcula población para AÑOS usando columna Edad"""
        
        print(f"      BUSCANDO POBLACIÓN DE {anios} AÑOS")
        
        edad_field = self.corrected_years.get_age_years_field_corrected(data_source)
        
        if not edad_field:
            print("      No se pudo obtener columna de edad")
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
            
            print(f"      POBLACIÓN {anios} AÑOS: {count}")
            
            return {-1: count}
        
        except Exception as e:
            print(f"      Error: {e}")
            return {-1: 0}
    
    def _normalize_age_key(self, edad_str: str) -> str:
        """Normaliza clave de edad"""
        if not edad_str:
            return ""
        
        edad_norm = normalize_text(edad_str)
        
        for key in self.birth_date_ranges_den.keys():
            key_norm = normalize_text(key)
            if edad_norm == key_norm:
                return key
        
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
        
        return valid_mappings
