# services/technical_note_services/report_service_aux/corrected_years.py

from services.duckdb_service.duckdb_service import duckdb_service

class CorrectedYear:
    """Usa columna Edad existente en el dataset"""
    
    def get_age_years_field_corrected(self, data_source: str) -> str:
        """
        Retorna el nombre de la columna de edad en años
        
        Args:
            data_source: Nombre de la tabla
            corte_fecha: No se usa, pero se mantiene por compatibilidad
        
        Returns:
            String con el nombre de la columna
        """
        # Buscar la columna "Edad" en el dataset
        try:
            describe_query = f"DESCRIBE SELECT * FROM {data_source}"
            columns_result = duckdb_service.conn.execute(describe_query).fetchall()
            all_columns = [row[0] for row in columns_result]
            
            # Buscar columna "Edad"
            if "Edad" in all_columns:
                print(f"      ✓ Columna 'Edad' encontrada en el dataset")
                return '"Edad"'
            
            # Alternativas
            for col in all_columns:
                if col.lower() == 'edad' or 'edad' in col.lower():
                    print(f"      ✓ Columna de edad encontrada: {col}")
                    return f'"{col}"'
            
            print(f"      ⚠️ No se encontró columna de edad, usando cálculo dinámico")
            return None
            
        except Exception as e:
            print(f"      ⚠️ Error detectando columna edad: {e}")
            return None
