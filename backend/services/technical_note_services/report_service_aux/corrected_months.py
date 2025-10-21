# services/technical_note_services/report_service_aux/corrected_months.py - ✅ VERSIÓN EXACTA EXCEL
from services.duckdb_service.duckdb_service import duckdb_service

class CorrectedMonths:
    def get_age_months_field_corrected(self, data_source: str, corte_fecha: str) -> str:
        """
        ✅ CÁLCULO DE MESES EXACTAMENTE IGUAL QUE EXCEL SIFECHA()
        
        SIFECHA en Excel calcula meses completos considerando:
        - Si el día de nacimiento <= día de corte → mes completo
        - Si el día de nacimiento > día de corte → mes incompleto (resta 1)
        
        Fórmula equivalente:
        (YEAR(corte) - YEAR(nac)) * 12 + (MONTH(corte) - MONTH(nac)) + 
        CASE WHEN DAY(nac) <= DAY(corte) THEN 0 ELSE -1 END
        """
        try:
            print(f"\n🔍 ===== DEBUG get_age_months_field_corrected =====")
            print(f"   📅 corte_fecha RECIBIDA: {corte_fecha}")
            
            describe_sql = f"DESCRIBE SELECT * FROM {data_source}"
            columns_result = duckdb_service.conn.execute(describe_sql).fetchall()
            column_names = [row[0] for row in columns_result]
            
            # Buscar columnas existentes de edad en meses
            edad_candidates = ['edad_meses', 'meses_edad', 'edad_en_meses', 'EdadMeses']
            
            for candidate in edad_candidates:
                if candidate in column_names:
                    print(f"   ✅ Campo edad meses detectado: {candidate}")
                    return f'"{candidate}"'
            
            # Buscar fecha de nacimiento
            fecha_candidates = ['Fecha Nacimiento', 'fecha_nacimiento', 'FechaNacimiento']
            
            fecha_field = None
            for candidate in fecha_candidates:
                if candidate in column_names:
                    fecha_field = candidate
                    break
            
            if fecha_field:
                # ✅ FÓRMULA EXACTA DE SIFECHA DE EXCEL
                calc_field = f"""(
                    (date_part('year', DATE '{corte_fecha}') - date_part('year', strptime("{fecha_field}", '%d/%m/%Y'))) * 12
                    + (date_part('month', DATE '{corte_fecha}') - date_part('month', strptime("{fecha_field}", '%d/%m/%Y')))
                    + CASE 
                        WHEN date_part('day', strptime("{fecha_field}", '%d/%m/%Y')) <= date_part('day', DATE '{corte_fecha}')
                        THEN 0
                        ELSE -1
                      END
                )"""
                
                print(f"   ✅ Calculando edad meses desde: {fecha_field}")
                print(f"   📅 Con fecha de corte DINÁMICA: {corte_fecha}")
                print(f"   🔧 Fórmula SQL (EXACTA EXCEL): Años*12 + Meses + Ajuste días")
                print(f"   💡 Nota: Replica EXACTAMENTE SIFECHA() de Excel")
                
                # Validación con casos de prueba
                try:
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
                    test_result = duckdb_service.conn.execute(test_sql).fetchall()
                    
                    print(f"   ✅ Validación - Ejemplos (comparando con date_diff):")
                    for row in test_result:
                        fecha_nac = row[0]
                        dia_nac = row[1]
                        dia_corte = row[2]
                        edad_excel = row[3]
                        edad_date_diff = row[4]
                        fecha_corte_val = row[5]
                        
                        diferencia = abs(edad_excel - edad_date_diff) if edad_date_diff else 0
                        simbolo = "⚠️" if diferencia > 0 else "✅"
                        
                        print(f"      {simbolo} Nac: {fecha_nac} (día {dia_nac}) → Excel: {edad_excel} meses | date_diff: {edad_date_diff} | Dif: {diferencia}")
                        
                except Exception as test_error:
                    print(f"   ❌ Error en validación: {test_error}")
                    raise
                
                print(f"===== FIN DEBUG =====\n")
                return calc_field
            
            raise Exception("No se encontró campo de edad en meses ni fecha de nacimiento")
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            raise
