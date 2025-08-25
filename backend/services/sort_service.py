import pandas as pd
from typing import List
from models.schemas import SortCondition, SortDirection

class SortService:
    @staticmethod
    def apply_sort(df: pd.DataFrame, sort_conditions: List[SortCondition]) -> pd.DataFrame:
        """Aplica ordenamiento múltiple al DataFrame"""
        if not sort_conditions:
            return df
        
        # Extraer columnas y direcciones
        columns = [condition.column for condition in sort_conditions if condition.column in df.columns]
        ascending = [condition.direction == SortDirection.ASC for condition in sort_conditions if condition.column in df.columns]
        
        if not columns:
            return df
        
        return df.sort_values(by=columns, ascending=ascending)
