# utils/text_normalizer.py

import unicodedata
import re


def normalize_text(text: str) -> str:
    """
    Normaliza texto removiendo tildes, convirtiendo a mayúsculas y limpiando espacios
    
    Args:
        text: Texto a normalizar
    
    Returns:
        Texto normalizado sin tildes, en mayúsculas y sin espacios extras
    """
    if not text or not isinstance(text, str):
        return ""
    
    # Remover tildes (NFD decomposition)
    text_sin_tildes = ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )
    
    # 🔥 AGREGAR: Convertir Ñ a N (mayúscula y minúscula)
    text_sin_tildes = text_sin_tildes.replace('Ñ', 'N').replace('ñ', 'n')
    
    # Convertir a mayúsculas y limpiar espacios múltiples
    text_normalizado = re.sub(r'\s+', ' ', text_sin_tildes.upper()).strip()
    
    return text_normalizado


def texts_match(text1: str, text2: str) -> bool:
    """
    Compara dos textos ignorando tildes, mayúsculas y espacios extras
    
    Returns:
        True si los textos son equivalentes
    """
    return normalize_text(text1) == normalize_text(text2)


def normalize_for_sql(text: str) -> str:
    """
    Normaliza texto para usar en cláusulas SQL LIKE
    Escapa caracteres especiales de SQL
    
    Returns:
        Texto normalizado y escapado para SQL
    """
    if not text:
        return ""
    
    normalized = normalize_text(text)
    
    # Escapar comillas simples para SQL
    normalized = normalized.replace("'", "''")
    
    return normalized


def generate_normalized_column_expr(column_name: str) -> str:
    """
    🔥 MEJORADO: Genera expresión SQL para normalizar una columna en DuckDB
    Ahora incluye conversión de Ñ a N
    
    Args:
        column_name: Nombre de la columna (ya escapado con comillas si es necesario)
    
    Returns:
        Expresión SQL que normaliza la columna
    """
    # Normalización completa incluyendo Ñ → N
    expr = f"""
    UPPER(
        TRIM(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        CAST({column_name} AS VARCHAR),
                                        '[áàäâ]', 'A', 'g'
                                    ),
                                    '[éèëê]', 'E', 'g'
                                ),
                                '[íìïî]', 'I', 'g'
                            ),
                            '[óòöô]', 'O', 'g'
                        ),
                        '[úùüû]', 'U', 'g'
                    ),
                    '[ñÑ]', 'N', 'g'
                ),
                '\s+', ' ', 'g'
            )
        )
    )
    """
    
    return expr.strip()
