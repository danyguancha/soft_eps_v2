# services/technical_note_services/date_range_calculator.py
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Dict, List, Tuple
import calendar

class DateRangeCalculator:
    """Calcula rangos de fechas de nacimiento mes a mes según edad"""
    
    MONTH_NAMES_ES = {
        1: "enero", 2: "febrero", 3: "marzo", 4: "abril",
        5: "mayo", 6: "junio", 7: "julio", 8: "agosto",
        9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre"
    }
    
    @staticmethod
    def get_months_until_cutoff(corte_fecha: str) -> List[Tuple[int, int, str]]:
        """
        Genera lista de meses desde enero hasta la fecha de corte
        
        Returns:
            Lista de tuplas (mes_num, anio, nombre_mes)
        """
        cutoff = datetime.strptime(corte_fecha, '%Y-%m-%d')
        months = []
        
        # Desde enero del año de corte hasta el mes de corte
        for month in range(1, cutoff.month + 1):
            month_name = DateRangeCalculator.MONTH_NAMES_ES[month]
            months.append((month, cutoff.year, month_name))
        
        return months
    
    @staticmethod
    def calculate_birth_range_for_month_age(
        mes_reporte: int,
        anio_reporte: int,
        min_meses_edad: int,
        max_meses_edad: int
    ) -> Tuple[str, str]:
        """
        Calcula rango de fechas de nacimiento para una edad en meses en un mes específico
        
        Args:
            mes_reporte: Mes del reporte (1-12)
            anio_reporte: Año del reporte
            min_meses_edad: Edad mínima en meses
            max_meses_edad: Edad máxima en meses
        
        Returns:
            Tupla (fecha_inicio, fecha_fin) en formato DD/MM/YYYY
        """
        # Fecha de referencia (último día del mes de reporte)
        last_day = calendar.monthrange(anio_reporte, mes_reporte)[1]
        fecha_referencia = datetime(anio_reporte, mes_reporte, last_day)
        
        # Calcular fechas de nacimiento
        # Para max_meses_edad: fecha inicio del rango
        fecha_inicio = fecha_referencia - relativedelta(months=max_meses_edad)
        fecha_inicio = fecha_inicio.replace(day=1)
        
        # Para min_meses_edad: fecha fin del rango
        fecha_fin_temp = fecha_referencia - relativedelta(months=min_meses_edad)
        ultimo_dia_mes = calendar.monthrange(fecha_fin_temp.year, fecha_fin_temp.month)[1]
        fecha_fin = fecha_fin_temp.replace(day=ultimo_dia_mes)
        
        return (
            fecha_inicio.strftime('%d/%m/%Y'),
            fecha_fin.strftime('%d/%m/%Y')
        )
    
    @staticmethod
    def calculate_birth_range_for_year_age(
        mes_reporte: int,
        anio_reporte: int,
        edad_anios: int
    ) -> Tuple[str, str]:
        """
        Calcula rango de fechas de nacimiento para una edad en años en un mes específico
        
        Args:
            mes_reporte: Mes del reporte (1-12)
            anio_reporte: Año del reporte
            edad_anios: Edad en años
        
        Returns:
            Tupla (fecha_inicio, fecha_fin) en formato DD/MM/YYYY
        """
        # Para edad en años, el rango es todo el mes del año correspondiente
        anio_nacimiento = anio_reporte - edad_anios
        
        fecha_inicio = datetime(anio_nacimiento, mes_reporte, 1)
        ultimo_dia = calendar.monthrange(anio_nacimiento, mes_reporte)[1]
        fecha_fin = datetime(anio_nacimiento, mes_reporte, ultimo_dia)
        
        return (
            fecha_inicio.strftime('%d/%m/%Y'),
            fecha_fin.strftime('%d/%m/%Y')
        )
    
    @staticmethod
    def generate_sql_date_filter(fecha_inicio: str, fecha_fin: str) -> str:
        """
        Genera filtro SQL para rango de fechas de nacimiento
        
        Args:
            fecha_inicio: Fecha en formato DD/MM/YYYY
            fecha_fin: Fecha en formato DD/MM/YYYY
        
        Returns:
            Condición SQL
        """
        return f"""(
            strptime("Fecha Nacimiento", '%d/%m/%Y') >= strptime('{fecha_inicio}', '%d/%m/%Y')
            AND strptime("Fecha Nacimiento", '%d/%m/%Y') <= strptime('{fecha_fin}', '%d/%m/%Y')
        )"""


# Instancia global
date_range_calculator = DateRangeCalculator()
