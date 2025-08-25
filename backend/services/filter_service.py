import pandas as pd
from typing import List
from models.schemas import FilterCondition, FilterOperator

class FilterService:
    @staticmethod
    def apply_filters(df: pd.DataFrame, filters: List[FilterCondition]) -> pd.DataFrame:
        """Aplica múltiples filtros al DataFrame"""
        filtered_df = df.copy()
        
        for filter_condition in filters:
            filtered_df = FilterService._apply_single_filter(filtered_df, filter_condition)
        
        return filtered_df
    
    @staticmethod
    def _apply_single_filter(df: pd.DataFrame, filter_condition: FilterCondition) -> pd.DataFrame:
        """Aplica un filtro individual"""
        column = filter_condition.column
        operator = filter_condition.operator
        value = filter_condition.value
        values = filter_condition.values
        
        if column not in df.columns:
            return df
        
        if operator == FilterOperator.EQUALS:
            return df[df[column] == value]
        elif operator == FilterOperator.CONTAINS:
            return df[df[column].astype(str).str.contains(str(value), na=False, case=False)]
        elif operator == FilterOperator.STARTS_WITH:
            return df[df[column].astype(str).str.startswith(str(value), na=False)]
        elif operator == FilterOperator.ENDS_WITH:
            return df[df[column].astype(str).str.endswith(str(value), na=False)]
        elif operator == FilterOperator.GREATER_THAN:
            return df[pd.to_numeric(df[column], errors='coerce') > float(value)]
        elif operator == FilterOperator.LESS_THAN:
            return df[pd.to_numeric(df[column], errors='coerce') < float(value)]
        elif operator == FilterOperator.GREATER_EQUAL:
            return df[pd.to_numeric(df[column], errors='coerce') >= float(value)]
        elif operator == FilterOperator.LESS_EQUAL:
            return df[pd.to_numeric(df[column], errors='coerce') <= float(value)]
        elif operator == FilterOperator.IN:
            return df[df[column].isin(values)]
        elif operator == FilterOperator.NOT_IN:
            return df[~df[column].isin(values)]
        elif operator == FilterOperator.IS_NULL:
            return df[df[column].isnull()]
        elif operator == FilterOperator.IS_NOT_NULL:
            return df[df[column].notnull()]
        
        return df
    
    @staticmethod
    def apply_search(df: pd.DataFrame, search_term: str) -> pd.DataFrame:
        """Aplica búsqueda global en todas las columnas"""
        if not search_term:
            return df
        
        # Crear máscara booleana para búsqueda en todas las columnas
        mask = df.astype(str).apply(
            lambda x: x.str.contains(search_term, case=False, na=False)
        ).any(axis=1)
        
        return df[mask]
