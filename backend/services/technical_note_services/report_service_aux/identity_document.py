# services/technical_note_services/report_service_aux/identity_document.py

from services.duckdb_service.duckdb_service import duckdb_service

class IdentityDocument:
    """Detecta campo de codsiris que es el identificador unico de cada registro en el dataset"""
    
    def get_document_field(self, data_source: str) -> str:
        """
        Detecta el campo de codsiris de identidad en el dataset
        
        Returns:
            Nombre del campo de codsiris YA ESCAPADO con comillas
        """
        try:
            describe_query = f"DESCRIBE SELECT * FROM {data_source}"
            columns_result = duckdb_service.conn.execute(describe_query).fetchall()
            all_columns = [row[0] for row in columns_result]
            
            # Buscar columnas típicas de codsiris
            possible_fields = [
                'codsiris',
                'cod_siris',
                'CodSiris',
                'Codigo Siris',
                'Código Siris'
            ]
            
            for field in possible_fields:
                if field in all_columns:
                    # 🔥 RETORNAR CON COMILLAS ESCAPADAS
                    return f'"{field}"'
            
            # Si no encuentra, usar la primera columna que parezca un ID
            for col in all_columns:
                col_lower = col.lower()
                if 'codsiris' in col_lower or 'cod_siris' in col_lower:
                    # 🔥 RETORNAR CON COMILLAS ESCAPADAS
                    return f'"{col}"'
            
            # Fallback: retornar primera columna
            if all_columns:
                return f'"{all_columns[0]}"'
            else:
                return '"id"'
            
        except Exception as e:
            print(f"Error detectando campo codsiris: {e}")
            return '"id"'
