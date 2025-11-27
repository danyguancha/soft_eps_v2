# test_nt_rpms_enfermeria.py

import duckdb

# Conectar a DuckDB
conn = duckdb.connect()

# Query para verificar datos de enfermería en NT_RPMS
query = """
SELECT 
    consultas_procedimientos,
    frecuencia_edad,
    nombre_ips,
    departamento,
    municipio,
    meta,
    frecuencia_indicada,
    frecuencia_uso
FROM read_parquet('C:\\Users\\SALUD\\software_eps\\backend\\parquet_cache\\60c79a2a07e33b9b.parquet')
WHERE 
    UPPER(consultas_procedimientos) LIKE '%ENFERMERIA%ADOLESCENCIA%'
    AND UPPER(departamento) LIKE '%NARIÑO%'
    AND UPPER(municipio) LIKE '%IPIALES%'
LIMIT 5;
"""

print("="*80)
print("VERIFICANDO DATOS DE ENFERMERÍA EN NT_RPMS")
print("="*80)

try:
    result = conn.execute(query).fetchall()
    
    if result:
        print(f"\n✅ Se encontraron {len(result)} registros:\n")
        
        for idx, row in enumerate(result, 1):
            print(f"--- Registro {idx} ---")
            print(f"Consulta/Procedimiento: {row[0]}")
            print(f"Frecuencia Edad: {row[1]}")
            print(f"Nombre IPS: {row[2]}")
            print(f"Departamento: {row[3]}")
            print(f"Municipio: {row[4]}")
            print(f"Meta: {row[5]}")
            print(f"Frecuencia Indicada: {row[6]}")
            print(f"Frecuencia Uso: {row[7]}")
            print()
    else:
        print("\n❌ No se encontraron registros con esos filtros")
        print("\nIntentando sin filtros geográficos...\n")
        
        # Query sin filtros geográficos
        query_sin_filtros = """
        SELECT 
            consultas_procedimientos,
            frecuencia_edad,
            nombre_ips,
            departamento,
            municipio
        FROM read_parquet('C:\\Users\\SALUD\\software_eps\\backend\\parquet_cache\\60c79a2a07e33b9b.parquet')
        WHERE 
            UPPER(consultas_procedimientos) LIKE '%ENFERMERIA%ADOLESCENCIA%'
        LIMIT 10;
        """
        
        result_sin_filtros = conn.execute(query_sin_filtros).fetchall()
        
        if result_sin_filtros:
            print(f"✅ Se encontraron {len(result_sin_filtros)} registros SIN filtros geográficos:\n")
            
            for idx, row in enumerate(result_sin_filtros, 1):
                print(f"[{idx}] {row[0]}")
                print(f"    Edad: {row[1]}")
                print(f"    IPS: {row[2]}")
                print(f"    Depto: {row[3]} | Muni: {row[4]}")
                print()
        else:
            print("❌ No se encontró ningún registro de enfermería - adolescencia")
            
            # Mostrar todos los registros de adolescencia
            print("\n📋 Mostrando TODOS los registros de adolescencia:")
            
            query_adolescencia = """
            SELECT DISTINCT
                consultas_procedimientos,
                frecuencia_edad
            FROM read_parquet('C:\\Users\\SALUD\\software_eps\\backend\\parquet_cache\\60c79a2a07e33b9b.parquet')
            WHERE 
                UPPER(consultas_procedimientos) LIKE '%ADOLESCENCIA%'
            LIMIT 20;
            """
            
            result_adolescencia = conn.execute(query_adolescencia).fetchall()
            
            for idx, row in enumerate(result_adolescencia, 1):
                print(f"[{idx}] {row[0]}")
                print(f"    Edad: {row[1]}")
                print()

except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()

finally:
    conn.close()

print("="*80)
