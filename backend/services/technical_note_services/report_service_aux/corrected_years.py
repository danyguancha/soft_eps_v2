# services/technical_note_services/report_service_aux/corrected_years.py - CORREGIDO
from services.duckdb_service.duckdb_service import duckdb_service

class CorrectedYear:
    def get_age_years_field_corrected(self, data_source: str, corte_fecha: str) -> str:
        """
        SOLUCIÓN DEFINITIVA: Usar date_diff() para años (igual que Excel)
        
        Excel: =SIFECHA(fecha_nacimiento; fecha_corte; "y")
        DuckDB: date_diff('year', fecha_nacimiento, fecha_corte)
        """
        try:
            print(f"\n===== DEBUG get_age_years_field_corrected =====")
            print(f"   corte_fecha RECIBIDA: {corte_fecha}")
            
            describe_sql = f"DESCRIBE SELECT * FROM {data_source}"
            columns_result = duckdb_service.conn.execute(describe_sql).fetchall()
            column_names = [row[0] for row in columns_result]
            
            # Buscar columnas existentes
            edad_candidates = ['edad', 'Edad', 'edad_años', 'age', 'Age']
            
            for candidate in edad_candidates:
                if candidate in column_names:
                    print(f"   Campo edad años detectado: {candidate}")
                    return f'TRY_CAST("{candidate}" AS INTEGER)'
            
            # Buscar fecha de nacimiento
            fecha_candidates = ['Fecha Nacimiento', 'fecha_nacimiento', 'FechaNacimiento']
            
            fecha_field = None
            for candidate in fecha_candidates:
                if candidate in column_names:
                    fecha_field = candidate
                    break
            
            if fecha_field:
                # CAMBIO CRÍTICO: Usar date_diff() para años completos
                calc_field = f"date_diff('year', strptime(\"{fecha_field}\", '%d/%m/%Y'), DATE '{corte_fecha}')"                
                # Validación
                try:
                    test_sql = f"""
                    SELECT 
                        "{fecha_field}" as fecha_nac,
                        {calc_field} as edad_años_calculada,
                        DATE '{corte_fecha}' as fecha_corte
                    FROM {data_source}
                    WHERE "{fecha_field}" IS NOT NULL 
                    AND TRY_CAST(strptime("{fecha_field}", '%d/%m/%Y') AS DATE) IS NOT NULL
                    LIMIT 5
                    """
                    test_result = duckdb_service.conn.execute(test_sql).fetchall()
                    
                    print(f"   Validación OK - Ejemplos:")
                    for row in test_result:
                        print(f"      Nac: {row[0]} → Edad: {row[1]} años (corte: {row[2]})")
                        
                except Exception as test_error:
                    print(f"   Error en validación: {test_error}")
                    raise
                
                print(f"===== FIN DEBUG =====\n")
                return calc_field
            
            raise Exception("No se encontró campo de edad en años ni fecha de nacimiento")
            
        except Exception as e:
            print(f"   Error: {e}")
            raise
