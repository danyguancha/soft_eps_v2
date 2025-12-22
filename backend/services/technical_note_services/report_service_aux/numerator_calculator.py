# services/technical_note_services/report_service_aux/numerator_calculator.py

from typing import Dict, Any, Optional
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
        corte_fecha: str = None
    ) -> Dict[int, int]:
        """
        Calcula numerador MES A MES del AÑO de corte específico.
        
        Lógica CORRECTA:
        1. Filtros geográficos
        2. Filtro de método anticonceptivo (si aplica)
        3. Fecha del AÑO específico (anio_corte)
        4. Fecha <= fecha_corte
        5. Agrupar por mes
        """
        print(f"      📊 Calculando numerador del AÑO {anio_corte} hasta {corte_fecha}")
        print(f"      📅 Columna de fechas: '{column_name}'")
        
        col_escaped = f'"{column_name}"'
        formato_detectado = self._detect_date_format(data_source, col_escaped, where_clause)
        
        # Detectar columna de método anticonceptivo
        metodo_col = None
        if keyword and keyword.lower().strip() in ['diu', 'subdermico']:
            print(f"      🔍 Keyword: '{keyword}' → Filtro de Método Anticonceptivo")
            metodo_col = self._detect_metodo_column(data_source)
            if metodo_col:
                print(f"      ✓ Columna método: '{metodo_col}'")
                self._debug_metodo_values(data_source, metodo_col, where_clause, keyword, formato_detectado, col_escaped, anio_corte)
        
        # 🔥 Contar por mes DEL AÑO ESPECÍFICO
        numeradores = self._calculate_monthly_counts(
            data_source, col_escaped, where_clause,
            formato_detectado, anio_corte, corte_fecha, mes_limite,
            keyword, metodo_col
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
        metodo_col: Optional[str]
    ) -> Dict[int, int]:
        """
        Cuenta registros por mes DEL AÑO ESPECÍFICO
        """
        # Filtro de método anticonceptivo
        metodo_filter = ""
        if keyword and metodo_col:
            kw = keyword.lower().strip()
            if kw == "diu":
                metodo_filter = f"""
                  AND "{metodo_col}" IS NOT NULL
                  AND (
                      UPPER(CAST("{metodo_col}" AS VARCHAR)) LIKE '%DIU%'
                      OR UPPER(CAST("{metodo_col}" AS VARCHAR)) LIKE '%DISPOSITIVO%'
                      OR UPPER(CAST("{metodo_col}" AS VARCHAR)) LIKE '%INTRAUTERINO%'
                  )
                """
            elif kw == "subdermico":
                metodo_filter = f"""
                  AND "{metodo_col}" IS NOT NULL
                  AND (
                      UPPER(CAST("{metodo_col}" AS VARCHAR)) LIKE '%IMPLANTE%'
                      OR UPPER(CAST("{metodo_col}" AS VARCHAR)) LIKE '%SUBDERMICO%'
                      OR UPPER(CAST("{metodo_col}" AS VARCHAR)) LIKE '%SUBDERMIC%'
                  )
                """
        
        # 🔥 Query con filtro de AÑO específico
        if formato == 'yyyy-mm-dd':
            query = f"""
            SELECT 
                EXTRACT(MONTH FROM CAST(CAST({col_escaped} AS VARCHAR) AS DATE)) as mes,
                COUNT(DISTINCT "Nro Identificación") as total
            FROM {data_source}
            WHERE {where_clause}
              {metodo_filter}
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
              {metodo_filter}
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
            return {m: 0 for m in range(1, 13)}
    
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
            # 🔥 Query con filtro de año
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
