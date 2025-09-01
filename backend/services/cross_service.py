# services/cross_service.py
import pandas as pd

class CrossService:
    @staticmethod
    def cross_files(df1: pd.DataFrame, df2: pd.DataFrame, key1: str, key2: str, how='left'):
        """
        Cruce tipo VLOOKUP:
        - df1: Archivo base (se mantienen TODOS los registros)
        - df2: Archivo de búsqueda (solo PRIMERA coincidencia por clave)
        """
        df1_copy = df1.copy()
        df2_copy = df2.copy()
        
        # Normalizar tipos de columnas clave
        df1_copy[key1] = df1_copy[key1].astype(str).str.strip()
        df2_copy[key2] = df2_copy[key2].astype(str).str.strip()
        
        # ✅ CLAVE: Eliminar duplicados del archivo de búsqueda (solo primer match)
        df2_lookup = df2_copy.drop_duplicates(subset=[key2], keep='first')
        
        print(f"🔍 VLOOKUP - DF2 antes: {len(df2_copy):,}, después: {len(df2_lookup):,}")
        
        # Realizar merge tipo VLOOKUP
        result = pd.merge(
            df1_copy, df2_lookup,
            left_on=key1, right_on=key2, 
            how=how,
            suffixes=('', '_f2'),
            indicator=True
        )
        
        print(f"✅ Resultado VLOOKUP: {len(result):,} registros (= tamaño archivo base)")
        
        return result
