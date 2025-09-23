

from services.duckdb_service.duckdb_service import duckdb_service


class CorrectedMonths:

    def get_age_months_field_corrected(self, data_source: str, corte_fecha: str) -> str:
        """
        CORREGIDO: Determina el campo correcto para edad en meses
        """
        try:
            describe_sql = f"DESCRIBE SELECT * FROM {data_source}"
            columns_result = duckdb_service.conn.execute(describe_sql).fetchall()
            column_names = [row[0] for row in columns_result]
            
            print(f"   🔍 Buscando campo edad meses en {len(column_names)} columnas...")
            
            # Buscar columnas existentes de edad en meses
            edad_candidates = ['edad_meses', 'meses_edad', 'edad_en_meses', 'EdadMeses', 'Edad_Meses',
                             'edad_meses_calculada', 'meses', 'Meses']
            
            for candidate in edad_candidates:
                if candidate in column_names:
                    print(f"   Campo edad meses detectado: {candidate}")
                    return f'"{candidate}"'
            
            # BUSCAR POR SIMILITUD PARCIAL
            for column in column_names:
                if 'meses' in column.lower() and 'edad' in column.lower():
                    print(f"   Campo edad meses por similitud: {column}")
                    return f'"{column}"'
            
            # Si no hay campo directo, verificar si se puede calcular desde fecha nacimiento
            fecha_candidates = ['Fecha Nacimiento', 'fecha_nacimiento', 'FechaNacimiento', 'nacimiento',
                              'fecha_nac', 'fec_nacimiento', 'birth_date', 'birthdate']
            
            fecha_field = None
            for candidate in fecha_candidates:
                if candidate in column_names:
                    fecha_field = candidate
                    break
            
            # Buscar por similitud
            if not fecha_field:
                for column in column_names:
                    if any(keyword in column.lower() for keyword in ['nacimiento', 'fecha', 'birth', 'nac']):
                        if 'fecha' in column.lower() or 'birth' in column.lower():
                            fecha_field = column
                            break
            
            if fecha_field:
                calc_field = f"date_diff('month', strptime(\"{fecha_field}\", '%d/%m/%Y'), DATE '{corte_fecha}')"
                print(f"   Calculando edad meses desde: {fecha_field}")
                return calc_field
            
            raise Exception("No se encontró campo de edad en meses ni fecha de nacimiento")
            
        except Exception as e:
            print(f"   ❌ Error detectando campo edad meses: {e}")
            raise