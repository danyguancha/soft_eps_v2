# services/technical_note_services/report_service_aux/corrected_months.py

class CorrectedMonths:
    """Calcula edad en meses dinámicamente"""
    
    def get_age_months_field_corrected(self, data_source: str, corte_fecha: str) -> str:
        """
        Retorna expresión SQL que calcula edad en meses hasta la fecha de corte
        
        Args:
            data_source: Nombre de la tabla
            corte_fecha: Fecha de corte en formato YYYY-MM-DD
        
        Returns:
            String con expresión SQL para calcular edad en meses
        """
        # Expresión SQL que calcula meses de diferencia
        edad_meses_expr = f"""
        (
            EXTRACT(YEAR FROM DATE '{corte_fecha}') * 12 + 
            EXTRACT(MONTH FROM DATE '{corte_fecha}')
        ) - (
            EXTRACT(YEAR FROM strptime("Fecha Nacimiento", '%d/%m/%Y')) * 12 + 
            EXTRACT(MONTH FROM strptime("Fecha Nacimiento", '%d/%m/%Y'))
        )
        """
        
        return edad_meses_expr
