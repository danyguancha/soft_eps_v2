# test_query.py - QUERY FINAL QUE DEBE DAR 93

import duckdb

conn = duckdb.connect(':memory:')

# ✅ QUITAR TODOS LOS FILTROS EXTRA
query_simple = """
SELECT COUNT(*) as numerador
FROM read_parquet('C:\\Users\\SALUD\\software_eps\\backend\\parquet_cache\\a63448f1ebc42493.parquet')
WHERE 
    date_diff('month', strptime("Fecha Nacimiento", '%d/%m/%Y'), DATE '2025-09-30') = 1
    AND "Consulta por Medicina 1 mes" IS NOT NULL
    AND CAST("Consulta por Medicina 1 mes" AS VARCHAR) != ''
"""

result = conn.execute(query_simple).fetchone()
print(f"✅ NUMERADOR SIMPLIFICADO: {result[0]}")

# Si esto da 93, el problema está en tus filtros adicionales
# Si esto da 91, Excel está mal o tiene datos diferentes
