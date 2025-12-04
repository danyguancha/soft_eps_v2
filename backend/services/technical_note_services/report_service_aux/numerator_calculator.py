from typing import Dict, Any
from services.duckdb_service.duckdb_service import duckdb_service


class NumeratorCalculator:
    """Responsable de calcular numeradores mensuales"""
    
    @property
    def conn(self):
        """Obtiene conexión dinámicamente"""
        return duckdb_service.conn
    
    def calculate_by_month_simple(
        self,
        data_source: str,
        column_name: str,
        where_clause: str,
        anio_corte: int,
        mes_limite: int
    ) -> Dict[int, int]:
        """
        Calcula numerador SIN filtro de edad.
        Solo filtra por: filtros geográficos + columna + mes
        """
        print("      Calculando numerador (SIN filtro de edad)")
        
        col_escaped = f'"{column_name}"'
        formato_detectado = self._detect_date_format(data_source, col_escaped, where_clause)
        
        numeradores = {}
        
        for mes_num in range(1, 13):
            if mes_num > mes_limite:
                numeradores[mes_num] = 0
                continue
            
            query = self._build_query(
                data_source, col_escaped, where_clause,
                mes_num, anio_corte, formato_detectado
            )
            
            try:
                result = self.conn.execute(query).fetchone()
                count = int(result[0]) if result else 0
                numeradores[mes_num] = count
                
                if count > 0:
                    print(f"         Mes {mes_num}: {count} atenciones")
            
            except Exception as e:
                print(f"         Error mes {mes_num}: {str(e)[:80]}")
                numeradores[mes_num] = 0
        
        total = sum(numeradores.values())
        print(f"         Total numerador: {total}")
        
        return numeradores
    
    def _detect_date_format(
        self,
        data_source: str,
        col_escaped: str,
        where_clause: str
    ) -> str:
        """Detecta formato de fecha en la columna"""
        
        format_detection_query = f"""
        SELECT CAST({col_escaped} AS VARCHAR) as fecha_str
        FROM {data_source}
        WHERE {where_clause}
          AND {col_escaped} IS NOT NULL
          AND CAST({col_escaped} AS VARCHAR) != ''
        LIMIT 1
        """
        
        try:
            sample = self.conn.execute(format_detection_query).fetchone()
            if sample:
                fecha_sample = sample[0]
                formato = 'yyyy-mm-dd' if '-' in fecha_sample else 'dd/mm/yyyy'
                print(f"         Formato detectado: {formato}")
                return formato
        except:
            pass
        
        return 'yyyy-mm-dd'
    
    def _build_query(
        self,
        data_source: str,
        col_escaped: str,
        where_clause: str,
        mes_num: int,
        anio_corte: int,
        formato: str
    ) -> str:
        """Construye query SQL según formato de fecha"""
        
        if formato == 'yyyy-mm-dd':
            return f"""
            SELECT COUNT(DISTINCT "Nro Identificación")
            FROM {data_source}
            WHERE {where_clause}
              AND {col_escaped} IS NOT NULL
              AND CAST({col_escaped} AS VARCHAR) != ''
              AND CAST({col_escaped} AS VARCHAR) LIKE '____-__-__'
              AND EXTRACT(MONTH FROM CAST(CAST({col_escaped} AS VARCHAR) AS DATE)) = {mes_num}
              AND EXTRACT(YEAR FROM CAST(CAST({col_escaped} AS VARCHAR) AS DATE)) = {anio_corte}
            """
        else:
            return f"""
            SELECT COUNT(DISTINCT "Nro Identificación")
            FROM {data_source}
            WHERE {where_clause}
              AND {col_escaped} IS NOT NULL
              AND CAST({col_escaped} AS VARCHAR) != ''
              AND EXTRACT(MONTH FROM strptime(CAST({col_escaped} AS VARCHAR), '%d/%m/%Y')) = {mes_num}
              AND EXTRACT(YEAR FROM strptime(CAST({col_escaped} AS VARCHAR), '%d/%m/%Y')) = {anio_corte}
            """
