import re
from typing import Optional
from dataclasses import dataclass

@dataclass
class AgeRange:
    """Representa un rango de edad extraído de una columna"""
    min_age: int
    max_age: int
    unit: str  # 'months' o 'years'
    column_name: str
    
    def get_age_filter_sql(self, age_months_field: str, age_years_field: str) -> str:
        """Genera filtro SQL para este rango de edad específico"""
        if self.unit == 'months':
            if self.min_age == self.max_age:
                return f"{age_months_field} = {self.min_age}"
            else:
                return f"{age_months_field} BETWEEN {self.min_age} AND {self.max_age}"
        elif self.unit == 'years':
            if self.min_age == self.max_age:
                return f"{age_years_field} = {self.min_age}"
            else:
                return f"{age_years_field} BETWEEN {self.min_age} AND {self.max_age}"
        return "1=1"
    
    def get_description(self) -> str:
        """Descripción legible del rango"""
        if self.min_age == self.max_age:
            return f"{self.min_age} {self.unit}"
        else:
            return f"{self.min_age} a {self.max_age} {self.unit}"

class AgeRangeExtractor:
    """Extrae rangos de edad desde nombres de columnas de actividades médicas"""
    
    def __init__(self):
        # PATRONES CORREGIDOS - RANGOS PRIMERO, MÁS ESPECÍFICOS
        self.patterns = [
            # RANGOS DE MESES (más específicos primero)
            (r'(\d+)\s*a\s*(\d+)\s*mes(?:es)?\b', 'months', 'range'),      # "4 a 5 meses"
            (r'de\s+(\d+)\s*a\s*(\d+)\s*mes(?:es)?\b', 'months', 'range'), # "de 12 a 17 meses"
            (r'(\d+)\s*-\s*(\d+)\s*mes(?:es)?\b', 'months', 'range'),      # "4-5 meses" 
            (r'(\d+)\s*y\s*(\d+)\s*mes(?:es)?\b', 'months', 'range'),      # "4 y 5 meses"
            
            # RANGOS DE AÑOS (más específicos primero)
            (r'(\d+)\s*a\s*(\d+)\s*año(?:s)?\b', 'years', 'range'),        # "1 a 2 años"
            (r'de\s+(\d+)\s*a\s*(\d+)\s*año(?:s)?\b', 'years', 'range'),   # "de 3 a 5 años"
            (r'(\d+)\s*-\s*(\d+)\s*año(?:s)?\b', 'years', 'range'),        # "1-2 años"
            (r'(\d+)\s*y\s*(\d+)\s*año(?:s)?\b', 'years', 'range'),        # "1 y 2 años"
            
            # EDADES ESPECÍFICAS (menos específicos)
            (r'(\d+)\s*mes(?:es)?\b', 'months', 'specific'),               # "1 mes", "2 meses"
            (r'(\d+)\s*año(?:s)?\b', 'years', 'specific'),                 # "1 año", "2 años"
            
            # CASOS ESPECIALES
            (r'recién\s*nacid[oa]|neonat[oa]', 'months', 'special'),       # "recién nacido", "neonato"
            (r'lactante', 'months', 'special'),                            # "lactante"
        ]
    
    def extract_age_range(self, column_name: str) -> Optional[AgeRange]:
        """CORREGIDO: Extrae rango de edad desde nombre de columna"""
        try:
            normalized = column_name.lower().strip().replace('"', '')
            
            print(f"Analizando columna: '{column_name}'")
            print(f"   Normalizado: '{normalized}'")
            
            # Casos especiales primero
            if any(word in normalized for word in ['recién nacido', 'recien nacido', 'neonato']):
                print("Detectado: Recién nacido = 0 months")
                return AgeRange(0, 0, 'months', column_name)
            
            if 'lactante' in normalized:
                print("Detectado: Lactante = 1 a 12 months")
                return AgeRange(1, 12, 'months', column_name)
            
            # Probar cada patrón en orden
            for i, (pattern, unit, match_type) in enumerate(self.patterns):
                match = re.search(pattern, normalized, re.IGNORECASE)
                if match:
                    print(f"   Patrón {i+1} ({match_type}) coincide: {pattern}")
                    result = self._parse_match(match, normalized, column_name, (pattern, unit, match_type))
                    if result:
                        print(f"   Extraído: {result.get_description()}")
                        return result
            
            print("No se pudo extraer rango de edad")
            return None
            
        except Exception as e:
            print(f"   Error extrayendo rango: {e}")
            return None
    
    def _parse_match(self, match: re.Match, normalized: str, original_column: str, pattern_info: tuple) -> Optional[AgeRange]:
        """COMPLETAMENTE CORREGIDO: Parsea resultado usando información del patrón"""
        groups = match.groups()
        _, unit, match_type = pattern_info
        
        print(f"      Parsing groups: {groups}, type: {match_type}, unit: {unit}")
        
        # Casos especiales
        if match_type == 'special':
            if any(word in normalized for word in ['recién nacido', 'recien nacido', 'neonato']):
                return AgeRange(0, 0, 'months', original_column)
            if 'lactante' in normalized:
                return AgeRange(1, 12, 'months', original_column)
        
        # RANGOS (2 números)
        if match_type == 'range' and len(groups) >= 2 and groups[0] and groups[1]:
            try:
                min_age = int(groups[0])
                max_age = int(groups[1])
                # Asegurar orden correcto
                if min_age > max_age:
                    min_age, max_age = max_age, min_age
                print(f"      Rango detectado: {min_age} a {max_age} {unit}")
                return AgeRange(min_age, max_age, unit, original_column)
            except ValueError as e:
                print(f"      Error convirtiendo números de rango: {e}")
                return None
        
        # EDADES ESPECÍFICAS (1 número)
        if match_type == 'specific' and len(groups) >= 1 and groups[0]:
            try:
                age = int(groups[0])
                print(f"      Edad específica detectada: {age} {unit}")
                return AgeRange(age, age, unit, original_column)
            except ValueError as e:
                print(f"      Error convirtiendo número específico: {e}")
                return None
        
        print(f"      No se pudo parsear: groups={groups}, type={match_type}")
        return None
