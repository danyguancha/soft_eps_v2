# services/technical_note_services/report_service_aux/corrected_months.py
from services.duckdb_service.duckdb_service import duckdb_service

class CorrectedMonths:
    def _find_existing_age_months_column(self, column_names: list) -> str:
        """Busca columna existente de edad en meses"""
        edad_candidates = ['edad_meses', 'meses_edad', 'edad_en_meses', 'EdadMeses']
        
        for candidate in edad_candidates:
            if candidate in column_names:
                print(f"   anioCampo edad meses detectado: {candidate}")
                return f'"{candidate}"'
        
        return ""


    def _find_birth_date_column(self, column_names: list) -> str:
        """Busca columna de fecha de nacimiento"""
        fecha_candidates = ['Fecha Nacimiento', 'fecha_nacimiento', 'FechaNacimiento']
        
        for candidate in fecha_candidates:
            if candidate in column_names:
                return candidate
        
        return ""


    def _build_excel_sifecha_formula(self, fecha_field: str, corte_fecha: str) -> str:
        """Construye fórmula de cálculo de meses exactamente como SIFECHA de Excel"""
        return f"""(
            (date_part('year', DATE '{corte_fecha}') - date_part('year', strptime("{fecha_field}", '%d/%m/%Y'))) * 12
            + (date_part('month', DATE '{corte_fecha}') - date_part('month', strptime("{fecha_field}", '%d/%m/%Y')))
            + CASE 
                WHEN date_part('day', strptime("{fecha_field}", '%d/%m/%Y')) <= date_part('day', DATE '{corte_fecha}')
                THEN 0
                ELSE -1
            END
        )"""


    def _process_validation_row(self, row: tuple) -> str:
        """Procesa una fila de validación y retorna mensaje formateado"""
        fecha_nac = row[0]
        dia_nac = row[1]
        edad_excel = row[3]
        edad_date_diff = row[4]
        
        diferencia = abs(edad_excel - edad_date_diff) if edad_date_diff else 0
        simbolo = "⚠️" if diferencia > 0 else "✅"
        
        return f"      {simbolo} Nac: {fecha_nac} (día {dia_nac}) → Excel: {edad_excel} meses | date_diff: {edad_date_diff} | Dif: {diferencia}"


    def _validate_age_calculation(self, data_source: str, fecha_field: str, 
                                calc_field: str, corte_fecha: str):
        """Valida el cálculo de edad con casos de prueba"""
        test_sql = f"""
        SELECT 
            "{fecha_field}" as fecha_nac,
            date_part('day', strptime("{fecha_field}", '%d/%m/%Y')) as dia_nac,
            date_part('day', DATE '{corte_fecha}') as dia_corte,
            {calc_field} as edad_meses_calculada,
            date_diff('month', strptime("{fecha_field}", '%d/%m/%Y'), DATE '{corte_fecha}') as edad_date_diff,
            DATE '{corte_fecha}' as fecha_corte
        FROM {data_source} 
        WHERE "{fecha_field}" IS NOT NULL 
        AND TRY_CAST(strptime("{fecha_field}", '%d/%m/%Y') AS DATE) IS NOT NULL
        LIMIT 10
        """
        
        try:
            test_result = duckdb_service.conn.execute(test_sql).fetchall()
            
            print("Validación - Ejemplos (comparando con date_diff):")
            for row in test_result:
                print(self._process_validation_row(row))
                
        except Exception as test_error:
            print(f"Error en validación: {test_error}")
            raise


    def get_age_months_field_corrected(self, data_source: str, corte_fecha: str) -> str:
        """
        CÁLCULO DE MESES EXACTAMENTE IGUAL QUE EXCEL SIFECHA()
        
        SIFECHA en Excel calcula meses completos considerando:
        - Si el día de nacimiento <= día de corte → mes completo
        - Si el día de nacimiento > día de corte → mes incompleto (resta 1)
        """
        try:
            print("\n===== DEBUG get_age_months_field_corrected =====")
            print(f"corte_fecha RECIBIDA: {corte_fecha}")
            
            # PASO 1: Obtener columnas disponibles
            describe_sql = f"DESCRIBE SELECT * FROM {data_source}"
            columns_result = duckdb_service.conn.execute(describe_sql).fetchall()
            column_names = [row[0] for row in columns_result]
            
            # PASO 2: Buscar columna existente de edad en meses
            existing_age_field = self._find_existing_age_months_column(column_names)
            if existing_age_field:
                print("===== FIN DEBUG =====\n")
                return existing_age_field
            
            # PASO 3: Buscar fecha de nacimiento
            fecha_field = self._find_birth_date_column(column_names)
            
            if not fecha_field:
                raise ValueError("No se encontró campo de edad en meses ni fecha de nacimiento")
            
            # PASO 4: Construir fórmula SIFECHA
            calc_field = self._build_excel_sifecha_formula(fecha_field, corte_fecha)
            
            # PASO 5: Validar cálculo
            self._validate_age_calculation(data_source, fecha_field, calc_field, corte_fecha)
            
            print("===== FIN DEBUG =====\n")
            return calc_field
            
        except Exception as e:
            print(f"Error: {e}")
            raise

