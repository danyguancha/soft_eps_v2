

from services.duckdb_service.duckdb_service import duckdb_service


class CorrectedYear:
    def get_age_years_field_corrected(self, data_source: str, corte_fecha: str) -> str:
        """
        CORREGIDO: Determina el campo correcto para edad en años
        """
        try:
            describe_sql = f"DESCRIBE SELECT * FROM {data_source}"
            columns_result = duckdb_service.conn.execute(describe_sql).fetchall()
            column_names = [row[0] for row in columns_result]
            
            print(f"   🔍 Buscando campo edad años en {len(column_names)} columnas...")
            
            # Buscar columnas existentes de edad en años
            edad_candidates = ['edad_años', 'años_edad', 'edad_en_años', 'EdadAños', 'Edad_Años', 
                             'edad', 'Edad', 'age', 'Age', 'años', 'Anos', 'years']
            
            for candidate in edad_candidates:
                if candidate in column_names:
                    # Verificar si necesita conversión
                    if candidate.lower() in ['edad', 'age']:
                        print(f"   Campo edad años detectado (con conversión): {candidate}")
                        return f'TRY_CAST("{candidate}" AS INTEGER)'
                    else:
                        print(f"   Campo edad años detectado: {candidate}")
                        return f'"{candidate}"'
            
            # BUSCAR POR SIMILITUD PARCIAL
            for column in column_names:
                if any(keyword in column.lower() for keyword in ['edad', 'age', 'años', 'year']):
                    if not any(exclude in column.lower() for exclude in ['meses', 'month', 'dias', 'day']):
                        print(f"   Campo edad años por similitud: {column}")
                        return f'TRY_CAST("{column}" AS INTEGER)'
            
            # Calcular desde meses
            try:
                edad_meses_field = self._get_age_months_field_corrected(data_source, corte_fecha)
                calc_field = f"FLOOR(({edad_meses_field}) / 12)"
                print(f"   Calculando edad años desde meses")
                return calc_field
            except:
                pass
            
            # Fallback final
            print(f"   ⚠️ Usando fallback para edad años")
            return 'TRY_CAST("edad" AS INTEGER)'
            
        except Exception as e:
            print(f"   ❌ Error detectando campo edad años: {e}")
            return 'TRY_CAST("edad" AS INTEGER)'