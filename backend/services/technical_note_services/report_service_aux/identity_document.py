from services.duckdb_service.duckdb_service import duckdb_service


class IdentityDocument:

    def get_document_field(self, data_source: str) -> str:
        """
        Devuelve el nombre de la columna que identifica a la persona.
        """
        describe_sql = f'DESCRIBE SELECT * FROM {data_source}'
        cols = [row[0] for row in duckdb_service.conn.execute(describe_sql).fetchall()]

        doc_candidates = [
            'Nro Identificación', 'Nro Identificacion',
            'Número Documento', 'Numero Documento',
            'Documento', 'documento', 'Identificación', 'Identificacion',
            'cedula', 'Cedula', 'ID', 'id'
        ]
        # 1️⃣ búsqueda exacta
        for field in doc_candidates:
            if field in cols:
                print(f'Campo documento detectado: {field}')
                return f'"{field}"'

        # 2️⃣ búsqueda por similitud (contiene “doc”, “ident”, “ced”)
        for field in cols:
            if any(k in field.lower() for k in ['doc', 'ident', 'ced']):
                print(f'Campo documento por similitud: {field}')
                return f'"{field}"'

        # 3️⃣ último recurso: primer campo de la tabla
        print(f'Usando primer campo como ID: {cols[0]}')
        return f'"{cols[0]}"'