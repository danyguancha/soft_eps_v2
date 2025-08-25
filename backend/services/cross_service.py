import pandas as pd

class CrossService:
    @staticmethod
    def cross_files(df1: pd.DataFrame, df2: pd.DataFrame, key1: str, key2: str, how='inner'):
        # Cruce tipo BUSCARX
        result = pd.merge(df1, df2, left_on=key1, right_on=key2, how=how)
        return result
