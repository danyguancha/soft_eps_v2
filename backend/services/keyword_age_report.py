# services/reports/keyword_age_report.py - ✅ CÓDIGO COMPLETO CORREGIDO
import re
from dataclasses import dataclass
from typing import List, Dict, Optional
from utils.keywords_NT import DEFAULT_KEYWORDS, KeywordRule

@dataclass(frozen=True)
class AgeRangePattern:
    name: str
    regex: re.Pattern

DEFAULT_AGE_PATTERNS: List[AgeRangePattern] = [
    AgeRangePattern("X meses", re.compile(r"\b(\d+)\s*mes(?:es)?\b", re.IGNORECASE)),
    AgeRangePattern("X a Y meses", re.compile(r"\b(\d+)\s*(?:a|-|–|to)\s*(\d+)\s*mes(?:es)?\b", re.IGNORECASE)),
]

class ColumnKeywordReportService:
    def __init__(self, keywords: List[KeywordRule] = None, patterns: List[AgeRangePattern] = None):
        self.keywords = keywords or DEFAULT_KEYWORDS
        self.patterns = patterns or DEFAULT_AGE_PATTERNS

    def match_columns(self, columns: List[str]) -> List[Dict]:
        """Encuentra columnas que coinciden con palabras clave"""
        matches = []
        
        for col in columns:
            col_lower = col.lower()
            for rule in self.keywords:
                if any(synonym in col_lower for synonym in rule.synonyms):
                    age_range = self._extract_age_range(col)
                    match = {
                        "column": col,
                        "keyword": rule.name,
                        "age_range": age_range
                    }
                    matches.append(match)
                    break
        
        return matches

    def _extract_age_range(self, col: str) -> str:
        """Extrae rango de edad del nombre de columna"""
        for pattern in self.patterns:
            match = pattern.regex.search(col)
            if match:
                groups = match.groups()
                if len(groups) == 2:
                    return f"{groups[0]} a {groups[1]} meses"
                elif len(groups) == 1:
                    return f"{groups[0]} meses"
        
        return "Sin especificar"

    def build_report_sql_with_filters(
        self, 
        data_source: str, 
        matches: List[Dict], 
        escape_identifier_func,
        departamento: Optional[str] = None,
        municipio: Optional[str] = None,
        ips: Optional[str] = None
    ) -> str:
        """Construye SQL con TODOS los filtros geográficos aplicados en conjunto"""
        if not matches:
            return "SELECT 'Sin datos' AS column_name, 'ninguna' AS keyword, 'N/A' AS age_range, 0 AS count LIMIT 0"
        
        # ✅ APLICAR TODOS LOS FILTROS DISPONIBLES EN CONJUNTO
        geo_filters = []
        
        # ✅ Filtro por departamento (si existe)
        if departamento and departamento.strip():
            departamento_col = self._find_geographic_column(data_source, 'departamento', escape_identifier_func)
            if departamento_col:
                dept_escaped = departamento.replace("'", "''")
                geo_filters.append(f"{departamento_col} = '{dept_escaped}'")
                print(f"✅ Filtro por departamento: {departamento}")
        
        # ✅ Filtro por municipio (si existe)
        if municipio and municipio.strip():
            municipio_col = self._find_geographic_column(data_source, 'municipio', escape_identifier_func)
            if municipio_col:
                mun_escaped = municipio.replace("'", "''")
                geo_filters.append(f"{municipio_col} = '{mun_escaped}'")
                print(f"✅ Filtro por municipio: {municipio}")
        
        # ✅ Filtro por IPS (si existe)
        if ips and ips.strip():
            ips_col = self._find_geographic_column(data_source, 'ips', escape_identifier_func)
            if ips_col:
                ips_escaped = ips.replace("'", "''")
                geo_filters.append(f"{ips_col} = '{ips_escaped}'")
                print(f"✅ Filtro por IPS: {ips}")
        
        # ✅ MOSTRAR COMBINACIÓN DE FILTROS APLICADA
        if geo_filters:
            print(f"🔍 Aplicando filtros COMBINADOS: {' AND '.join(geo_filters)}")
        else:
            print(f"📊 Sin filtros geográficos - consultando todo el archivo")
        
        union_parts = []
        
        for match in matches:
            col_escaped = escape_identifier_func(match["column"])
            
            column_safe = match["column"].replace("'", "''")
            keyword_safe = match["keyword"].replace("'", "''")
            age_range_safe = match["age_range"].replace("'", "''")
            
            # ✅ CONSTRUIR WHERE CON TODOS LOS FILTROS
            where_conditions = [
                f"{col_escaped} IS NOT NULL",
                f"TRIM(CAST({col_escaped} AS VARCHAR)) <> ''",
                f"TRIM(CAST({col_escaped} AS VARCHAR)) NOT IN ('NULL', 'null', 'None', 'none', 'NaN', 'nan')"
            ]
            
            # ✅ AGREGAR TODOS LOS FILTROS GEOGRÁFICOS
            where_conditions.extend(geo_filters)
            
            where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
            
            union_part = f"""
            SELECT 
                '{column_safe}' AS column_name,
                '{keyword_safe}' AS keyword, 
                '{age_range_safe}' AS age_range,
                COUNT(*) AS count
            FROM {data_source}
            {where_clause}
            """
            
            union_parts.append(union_part)
        
        return " UNION ALL ".join(union_parts)

    def build_temporal_report_sql_with_filters(
        self, 
        data_source: str, 
        matches: List[Dict], 
        escape_identifier_func,
        departamento: Optional[str] = None,
        municipio: Optional[str] = None,
        ips: Optional[str] = None
    ) -> str:
        """Construye SQL temporal con TODOS los filtros geográficos aplicados en conjunto"""
        if not matches:
            return "SELECT 'Sin datos' AS column_name, 'ninguna' AS keyword, 'N/A' AS age_range, 0 AS year, 0 AS month, 0 AS count LIMIT 0"
        
        # ✅ APLICAR TODOS LOS FILTROS DISPONIBLES EN CONJUNTO
        geo_filters = []
        
        # ✅ Filtro por departamento (si existe)
        if departamento and departamento.strip():
            departamento_col = self._find_geographic_column(data_source, 'departamento', escape_identifier_func)
            if departamento_col:
                dept_escaped = departamento.replace("'", "''")
                geo_filters.append(f"{departamento_col} = '{dept_escaped}'")
                print(f"✅ Filtro temporal por departamento: {departamento}")
        
        # ✅ Filtro por municipio (si existe)
        if municipio and municipio.strip():
            municipio_col = self._find_geographic_column(data_source, 'municipio', escape_identifier_func)
            if municipio_col:
                mun_escaped = municipio.replace("'", "''")
                geo_filters.append(f"{municipio_col} = '{mun_escaped}'")
                print(f"✅ Filtro temporal por municipio: {municipio}")
        
        # ✅ Filtro por IPS (si existe)
        if ips and ips.strip():
            ips_col = self._find_geographic_column(data_source, 'ips', escape_identifier_func)
            if ips_col:
                ips_escaped = ips.replace("'", "''")
                geo_filters.append(f"{ips_col} = '{ips_escaped}'")
                print(f"✅ Filtro temporal por IPS: {ips}")
        
        # ✅ MOSTRAR COMBINACIÓN DE FILTROS TEMPORALES
        if geo_filters:
            print(f"🔍 Filtros temporales COMBINADOS: {' AND '.join(geo_filters)}")

        union_parts = []
        
        for match in matches:
            col_escaped = escape_identifier_func(match["column"])
            
            column_safe = match["column"].replace("'", "''")
            keyword_safe = match["keyword"].replace("'", "''")
            age_range_safe = match["age_range"].replace("'", "''")
            
            # ✅ CONSTRUIR WHERE COMPLETA CON TODOS LOS FILTROS
            where_conditions = [
                f"{col_escaped} IS NOT NULL",
                f"TRIM(CAST({col_escaped} AS VARCHAR)) <> ''",
                f"TRIM(CAST({col_escaped} AS VARCHAR)) NOT IN ('NULL', 'null', 'None', 'none', 'NaN', 'nan')",
                f"""(
                    TRY_CAST({col_escaped} AS DATE) IS NOT NULL OR
                    try_strptime({col_escaped}, '%d/%m/%Y') IS NOT NULL OR
                    try_strptime({col_escaped}, '%Y-%m-%d') IS NOT NULL OR
                    try_strptime({col_escaped}, '%m/%d/%Y') IS NOT NULL OR
                    try_strptime({col_escaped}, '%d-%m-%Y') IS NOT NULL OR
                    try_strptime(SUBSTR(CAST({col_escaped} AS VARCHAR), 1, 10), '%Y-%m-%d') IS NOT NULL
                )"""
            ]
            
            # ✅ AGREGAR TODOS LOS FILTROS GEOGRÁFICOS
            where_conditions.extend(geo_filters)
            
            where_clause = f"WHERE {' AND '.join(where_conditions)}"
            
            temporal_part = f"""
            SELECT 
                '{column_safe}' AS column_name,
                '{keyword_safe}' AS keyword, 
                '{age_range_safe}' AS age_range,
                COALESCE(
                    YEAR(TRY_CAST({col_escaped} AS DATE)),
                    YEAR(try_strptime({col_escaped}, '%d/%m/%Y')),
                    YEAR(try_strptime({col_escaped}, '%Y-%m-%d')),
                    YEAR(try_strptime({col_escaped}, '%m/%d/%Y')),
                    YEAR(try_strptime({col_escaped}, '%d-%m-%Y')),
                    YEAR(try_strptime(SUBSTR(CAST({col_escaped} AS VARCHAR), 1, 10), '%Y-%m-%d'))
                ) AS year,
                COALESCE(
                    MONTH(TRY_CAST({col_escaped} AS DATE)),
                    MONTH(try_strptime({col_escaped}, '%d/%m/%Y')),
                    MONTH(try_strptime({col_escaped}, '%Y-%m-%d')),
                    MONTH(try_strptime({col_escaped}, '%m/%d/%Y')),
                    MONTH(try_strptime({col_escaped}, '%d-%m-%Y')),
                    MONTH(try_strptime(SUBSTR(CAST({col_escaped} AS VARCHAR), 1, 10), '%Y-%m-%d'))
                ) AS month,
                COUNT(*) AS count
            FROM {data_source}
            {where_clause}
            GROUP BY 
                COALESCE(
                    YEAR(TRY_CAST({col_escaped} AS DATE)),
                    YEAR(try_strptime({col_escaped}, '%d/%m/%Y')),
                    YEAR(try_strptime({col_escaped}, '%Y-%m-%d')),
                    YEAR(try_strptime({col_escaped}, '%m/%d/%Y')),
                    YEAR(try_strptime({col_escaped}, '%d-%m-%Y')),
                    YEAR(try_strptime(SUBSTR(CAST({col_escaped} AS VARCHAR), 1, 10), '%Y-%m-%d'))
                ),
                COALESCE(
                    MONTH(TRY_CAST({col_escaped} AS DATE)),
                    MONTH(try_strptime({col_escaped}, '%d/%m/%Y')),
                    MONTH(try_strptime({col_escaped}, '%Y-%m-%d')),
                    MONTH(try_strptime({col_escaped}, '%m/%d/%Y')),
                    MONTH(try_strptime({col_escaped}, '%d-%m-%Y')),
                    MONTH(try_strptime(SUBSTR(CAST({col_escaped} AS VARCHAR), 1, 10), '%Y-%m-%d'))
                )
            HAVING 
                COALESCE(
                    YEAR(TRY_CAST({col_escaped} AS DATE)),
                    YEAR(try_strptime({col_escaped}, '%d/%m/%Y')),
                    YEAR(try_strptime({col_escaped}, '%Y-%m-%d')),
                    YEAR(try_strptime({col_escaped}, '%m/%d/%Y')),
                    YEAR(try_strptime({col_escaped}, '%d-%m-%Y')),
                    YEAR(try_strptime(SUBSTR(CAST({col_escaped} AS VARCHAR), 1, 10), '%Y-%m-%d'))
                ) IS NOT NULL
                AND COALESCE(
                    MONTH(TRY_CAST({col_escaped} AS DATE)),
                    MONTH(try_strptime({col_escaped}, '%d/%m/%Y')),
                    MONTH(try_strptime({col_escaped}, '%Y-%m-%d')),
                    MONTH(try_strptime({col_escaped}, '%m/%d/%Y')),
                    MONTH(try_strptime({col_escaped}, '%d-%m-%Y')),
                    MONTH(try_strptime(SUBSTR(CAST({col_escaped} AS VARCHAR), 1, 10), '%Y-%m-%d'))
                ) IS NOT NULL
            """
            
            union_parts.append(temporal_part)
        
        return " UNION ALL ".join(union_parts)

    # ✅ MÉTODO FALTANTE - AGREGAR ESTE MÉTODO
    def get_unique_geographic_values_sql(
        self, 
        data_source: str, 
        geo_type: str, 
        escape_identifier_func,
        parent_filter: Optional[Dict[str, str]] = None
    ) -> str:
        """SQL para obtener valores únicos geográficos con filtros en cascada"""
        geo_column = self._find_geographic_column(data_source, geo_type, escape_identifier_func)
        
        if not geo_column:
            print(f"❌ No se encontró columna para {geo_type}")
            return "SELECT 'Sin datos' AS value WHERE 1=0"
        
        print(f"✅ Usando columna {geo_column} para {geo_type}")
        
        # Construir condiciones WHERE correctamente
        where_conditions = []
        
        # ✅ AGREGAR FILTROS PADRE CORRECTAMENTE
        if parent_filter:
            print(f"🔍 Aplicando filtros padre: {parent_filter}")
            for parent_type, parent_value in parent_filter.items():
                parent_col = self._find_geographic_column(data_source, parent_type, escape_identifier_func)
                if parent_col and parent_value:
                    parent_value_escaped = parent_value.replace("'", "''")
                    filter_condition = f"{parent_col} = '{parent_value_escaped}'"
                    where_conditions.append(filter_condition)
                    print(f"  ✅ Filtro agregado: {filter_condition}")
        
        # Siempre agregar condiciones básicas
        basic_conditions = [
            f"{geo_column} IS NOT NULL",
            f"TRIM(CAST({geo_column} AS VARCHAR)) <> ''",
            f"TRIM(CAST({geo_column} AS VARCHAR)) NOT IN ('NULL', 'null', 'None', 'none', 'NaN', 'nan', '')"
        ]
        where_conditions.extend(basic_conditions)
        
        # Construir SQL correcto
        sql = f"""
        SELECT DISTINCT {geo_column} AS value
        FROM {data_source}
        WHERE {' AND '.join(where_conditions)}
        ORDER BY {geo_column}
        LIMIT 1000
        """
        
        return sql

    def _find_geographic_column(self, data_source: str, geo_type: str, escape_identifier_func) -> Optional[str]:
        """Busca columnas geográficas verificando columnas reales con espacios"""
        geo_patterns = {
            'departamento': [
                'departamento', 'depto', 'dept', 'department', 
                'Departamento', 'DEPARTAMENTO', 'Depto', 'DEPTO'
            ],
            'municipio': [
                'municipio', 'mun', 'municipality', 'ciudad', 'city', 
                'Municipio', 'MUNICIPIO', 'Ciudad', 'CIUDAD'
            ],
            'ips': [
                'ips', 'nombre ips', 'Nombre IPS', 'NOMBRE IPS',
                'institucion', 'prestador', 'entidad', 'eps', 
                'hospital', 'clinica', 'IPS', 'INSTITUCION',
                'nombre_ips', 'NombreIPS', 'nombre institucion'
            ]
        }
        
        patterns = geo_patterns.get(geo_type, [])
        
        try:
            from services.duckdb_service.duckdb_service import duckdb_service
            
            # ✅ SINTAXIS CORRECTA PARA PARQUET SEGÚN DOCS DUCKDB
            if 'read_parquet' in data_source:
                # Para read_parquet('path'), usar: DESCRIBE SELECT * FROM read_parquet('path')
                describe_sql = f"DESCRIBE SELECT * FROM {data_source}"
            else:
                # Para tablas normales
                describe_sql = f"DESCRIBE {data_source}"
            
            print(f"🔍 SQL DESCRIBE: {describe_sql}")
            
            columns_result = duckdb_service.conn.execute(describe_sql).fetchall()
            
            # Crear mapeo: columna_lower -> nombre_original
            actual_columns = {}
            column_names = []
            
            for row in columns_result:
                original_name = row[0]
                column_names.append(original_name)
                
                # Crear múltiples variantes para búsqueda
                variants = [
                    original_name.lower(),
                    original_name.lower().replace(' ', ''),
                    original_name.lower().replace('_', ''),
                    original_name.lower().replace(' ', '_'),
                    original_name.lower().replace('-', ''),
                    original_name.lower().replace('.', '')
                ]
                for variant in variants:
                    actual_columns[variant] = original_name
            
            # Buscar coincidencias con los patrones
            for pattern in patterns:
                # Crear múltiples variantes del patrón
                pattern_variants = [
                    pattern.lower(),
                    pattern.lower().replace(' ', ''),
                    pattern.lower().replace('_', ''),
                    pattern.lower().replace(' ', '_'),
                    pattern.lower().replace('-', ''),
                    pattern.lower().replace('.', '')
                ]
                
                for pattern_variant in pattern_variants:
                    if pattern_variant in actual_columns:
                        original_name = actual_columns[pattern_variant]
                        print(f"✅ Columna geográfica encontrada: {pattern} -> {original_name}")
                        return escape_identifier_func(original_name)
            
            print(f"⚠️ No se encontró columna geográfica para {geo_type}")
            print(f"🔍 Patrones buscados: {patterns}")
            return None
            
        except Exception as e:
            print(f"❌ Error verificando columnas geográficas: {e}")
            print(f"💡 SQL que falló: {describe_sql if 'describe_sql' in locals() else 'No se generó SQL'}")
            import traceback
            traceback.print_exc()
            return None
