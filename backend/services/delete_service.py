import pandas as pd
from typing import List
from models.schemas import FilterCondition
from services.filter_service import FilterService

class DeleteService:
    @staticmethod
    def delete_rows_by_indices(df: pd.DataFrame, indices: List[int]) -> dict:
        """Elimina filas por índices específicos"""
        initial_count = len(df)
        
        # Validar índices
        valid_indices = [idx for idx in indices if 0 <= idx < len(df)]
        invalid_indices = [idx for idx in indices if idx not in valid_indices]
        
        if invalid_indices:
            print(f"Advertencia: Índices inválidos ignorados: {invalid_indices}")
        
        # Eliminar filas
        df_filtered = df.drop(df.index[valid_indices]).reset_index(drop=True)
        
        deleted_count = len(valid_indices)
        remaining_count = len(df_filtered)
        
        return {
            "dataframe": df_filtered,
            "deleted_count": deleted_count,
            "remaining_count": remaining_count,
            "invalid_indices": invalid_indices
        }
    
    @staticmethod
    def delete_rows_by_filters(df: pd.DataFrame, filters: List[FilterCondition]) -> dict:
        """Elimina filas que cumplan con los filtros especificados"""
        initial_count = len(df)
        
        # Encontrar filas que cumplen con los filtros (estas se eliminarán)
        rows_to_delete = FilterService.apply_filters(df, filters)
        delete_indices = rows_to_delete.index.tolist()
        
        # Mantener filas que NO cumplen con los filtros
        df_filtered = df.drop(delete_indices).reset_index(drop=True)
        
        deleted_count = len(delete_indices)
        remaining_count = len(df_filtered)
        
        return {
            "dataframe": df_filtered,
            "deleted_count": deleted_count,
            "remaining_count": remaining_count,
            "deleted_indices": delete_indices
        }
    
    @staticmethod
    def preview_delete_by_filters(df: pd.DataFrame, filters: List[FilterCondition]) -> dict:
        """Previsualiza qué filas serían eliminadas por los filtros"""
        rows_to_delete = FilterService.apply_filters(df, filters)
        
        return {
            "rows_to_delete_count": len(rows_to_delete),
            "total_rows": len(df),
            "preview_data": rows_to_delete.head(10).fillna("").to_dict(orient="records"),
            "would_remain": len(df) - len(rows_to_delete)
        }
    
    @staticmethod
    def delete_duplicates(df: pd.DataFrame, columns: List[str] = None, keep: str = 'first') -> dict:
        """Elimina filas duplicadas"""
        initial_count = len(df)
        
        # Si no se especifican columnas, usar todas
        subset = columns if columns else None
        
        df_no_duplicates = df.drop_duplicates(subset=subset, keep=keep).reset_index(drop=True)
        
        deleted_count = initial_count - len(df_no_duplicates)
        
        return {
            "dataframe": df_no_duplicates,
            "deleted_count": deleted_count,
            "remaining_count": len(df_no_duplicates),
            "columns_checked": subset or "all"
        }
    
    @staticmethod
    def delete_empty_rows(df: pd.DataFrame, columns: List[str] = None) -> dict:
        """Elimina filas completamente vacías o vacías en columnas específicas"""
        initial_count = len(df)
        
        if columns:
            # Eliminar filas donde las columnas especificadas están todas vacías
            mask = df[columns].notna().any(axis=1)
            df_filtered = df[mask].reset_index(drop=True)
        else:
            # Eliminar filas completamente vacías
            df_filtered = df.dropna(how='all').reset_index(drop=True)
        
        deleted_count = initial_count - len(df_filtered)
        
        return {
            "dataframe": df_filtered,
            "deleted_count": deleted_count,
            "remaining_count": len(df_filtered)
        }
