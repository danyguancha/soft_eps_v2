import pandas as pd
import re
from models.schemas import TransformOperation

class TransformationService:
    @staticmethod
    def apply_transformation(df: pd.DataFrame, operation: TransformOperation, params: dict) -> pd.DataFrame:
        """Aplica una transformación específica al DataFrame"""
        if operation == TransformOperation.CONCATENATE:
            return TransformationService._concatenate_columns(df, params)
        elif operation == TransformOperation.SPLIT_COLUMN:
            return TransformationService._split_column(df, params)
        elif operation == TransformOperation.REPLACE_VALUES:
            return TransformationService._replace_values(df, params)
        elif operation == TransformOperation.CREATE_CALCULATED:
            return TransformationService._create_calculated_column(df, params)
        elif operation == TransformOperation.RENAME_COLUMN:
            return TransformationService._rename_column(df, params)
        elif operation == TransformOperation.DELETE_COLUMN:
            return TransformationService._delete_column(df, params)
        elif operation == TransformOperation.FILL_NULL:
            return TransformationService._fill_null(df, params)
        elif operation == TransformOperation.TO_UPPERCASE:
            return TransformationService._to_uppercase(df, params)
        elif operation == TransformOperation.TO_LOWERCASE:
            return TransformationService._to_lowercase(df, params)
        elif operation == TransformOperation.EXTRACT_SUBSTRING:
            return TransformationService._extract_substring(df, params)
        else:
            raise ValueError(f"Operación no soportada: {operation}")
    
    @staticmethod
    def _concatenate_columns(df: pd.DataFrame, params: dict) -> pd.DataFrame:
        """Concatena múltiples columnas"""
        columns = params.get("columns", [])
        new_column = params.get("new_column", "concatenated")
        separator = params.get("separator", "_")
        
        df[new_column] = df[columns].astype(str).agg(separator.join, axis=1)
        return df
    
    @staticmethod
    def _split_column(df: pd.DataFrame, params: dict) -> pd.DataFrame:
        """Divide una columna en múltiples columnas"""
        column = params.get("column")
        separator = params.get("separator", ",")
        new_columns = params.get("new_columns", [])
        
        split_data = df[column].astype(str).str.split(separator, expand=True)
        for i, new_col in enumerate(new_columns):
            if i < split_data.shape[1]:
                df[new_col] = split_data[i]
        
        return df
    
    @staticmethod
    def _replace_values(df: pd.DataFrame, params: dict) -> pd.DataFrame:
        """Reemplaza valores en una columna"""
        column = params.get("column")
        old_value = params.get("old_value")
        new_value = params.get("new_value")
        
        df[column] = df[column].replace(old_value, new_value)
        return df
    
    @staticmethod
    def _create_calculated_column(df: pd.DataFrame, params: dict) -> pd.DataFrame:
        """Crea una columna calculada basada en una expresión"""
        new_column = params.get("new_column")
        expression = params.get("expression")  # Ej: "col1 + col2"
        
        try:
            # Evaluación segura de expresiones matemáticas básicas
            df[new_column] = df.eval(expression)
        except:
            df[new_column] = None
        
        return df
    
    @staticmethod
    def _rename_column(df: pd.DataFrame, params: dict) -> pd.DataFrame:
        """Renombra una columna"""
        old_name = params.get("old_name")
        new_name = params.get("new_name")
        
        df = df.rename(columns={old_name: new_name})
        return df
    
    @staticmethod
    def _delete_column(df: pd.DataFrame, params: dict) -> pd.DataFrame:
        """Elimina una columna"""
        column = params.get("column")
        
        if column in df.columns:
            df = df.drop(columns=[column])
        
        return df
    
    @staticmethod
    def _fill_null(df: pd.DataFrame, params: dict) -> pd.DataFrame:
        """Llena valores nulos"""
        column = params.get("column")
        fill_value = params.get("fill_value", "")
        
        df[column] = df[column].fillna(fill_value)
        return df
    
    @staticmethod
    def _to_uppercase(df: pd.DataFrame, params: dict) -> pd.DataFrame:
        """Convierte texto a mayúsculas"""
        column = params.get("column")
        
        df[column] = df[column].astype(str).str.upper()
        return df
    
    @staticmethod
    def _to_lowercase(df: pd.DataFrame, params: dict) -> pd.DataFrame:
        """Convierte texto a minúsculas"""
        column = params.get("column")
        
        df[column] = df[column].astype(str).str.lower()
        return df
    
    @staticmethod
    def _extract_substring(df: pd.DataFrame, params: dict) -> pd.DataFrame:
        """Extrae substring usando posiciones o regex"""
        column = params.get("column")
        new_column = params.get("new_column")
        start = params.get("start", 0)
        end = params.get("end")
        pattern = params.get("pattern")  # Regex pattern
        
        if pattern:
            df[new_column] = df[column].astype(str).str.extract(pattern)[0]
        else:
            df[new_column] = df[column].astype(str).str[start:end]
        
        return df
