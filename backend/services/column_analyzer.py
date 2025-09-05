# services/column_analyzer.py
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
import itertools


class ColumnAnalyzer:
    """
    Analizador inteligente de columnas para encontrar compatibilidades 
    basándose en contenido y patrones, no solo nombres
    """
    
    def __init__(self):
        self.similarity_threshold = 0.25  # Umbral mínimo de similitud
        self.sample_size = 150  # Tamaño de muestra para análisis
    
    def analyze_column_patterns(self, series: pd.Series) -> Dict:
        """Analiza patrones y características de una columna"""
        sample = series.dropna().head(self.sample_size)
        
        if len(sample) == 0:
            return {"type": "empty", "patterns": [], "stats": {}}
        
        # Convertir a string para análisis de patrones
        str_sample = sample.astype(str).str.strip()
        
        analysis = {
            "data_type": self._detect_data_type(str_sample),
            "patterns": self._extract_value_patterns(str_sample),
            "stats": self._calculate_column_stats(sample, str_sample),
            "sample_values": str_sample.head(10).tolist()
        }
        
        return analysis
    
    def _detect_data_type(self, str_sample: pd.Series) -> str:
        """Detecta el tipo principal de datos"""
        patterns = {
            "document_id": r'^\d{7,12}$',  # Cédulas, IDs numéricos (7-12 dígitos)
            "phone": r'^[\d\-\+\(\)\s]{7,15}$',  # Teléfonos
            "email": r'^[^\s@]+@[^\s@]+\.[^\s@]+$',  # Emails
            "date": r'^\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}$',  # Fechas
            "currency": r'^\$?\d+[\.,]?\d*$',  # Monedas
            "code": r'^[A-Z0-9\-]{3,20}$',  # Códigos alfanuméricos
            "percentage": r'^\d+[\.,]?\d*%?$',  # Porcentajes
            "numeric": r'^\d+[\.,]?\d*$',  # Números generales
        }
        
        type_scores = {}
        for data_type, pattern in patterns.items():
            matches = str_sample.str.match(pattern, case=False).sum()
            score = matches / len(str_sample) if len(str_sample) > 0 else 0
            type_scores[data_type] = score
        
        # Clasificación adicional para textos
        avg_length = str_sample.str.len().mean()
        if avg_length > 25:  # Textos largos
            type_scores["long_text"] = 0.8
        elif not any(score > 0.4 for score in type_scores.values()):
            type_scores["mixed"] = 0.6
        
        return max(type_scores.items(), key=lambda x: x[1])[0]
    
    def _extract_value_patterns(self, str_sample: pd.Series) -> List[str]:
        """Extrae patrones comunes en los valores"""
        patterns = []
        
        # Longitud promedio
        avg_len = str_sample.str.len().mean()
        patterns.append(f"avg_length_{int(avg_len)}")
        
        # Patrones de contenido
        if str_sample.str.contains(r'^\d+$').mean() > 0.7:
            patterns.append("numeric_only")
        
        if str_sample.str.contains(r'^[A-Z]+$', case=False).mean() > 0.5:
            patterns.append("alpha_only")
        
        if str_sample.str.contains(r'[\-\s\.]').mean() > 0.3:
            patterns.append("has_separators")
        
        # Consistencia de longitud
        lengths = str_sample.str.len()
        if lengths.std() < 2:
            patterns.append("fixed_length")
        
        return patterns
    
    def _calculate_column_stats(self, original_sample: pd.Series, str_sample: pd.Series) -> Dict:
        """Calcula estadísticas descriptivas"""
        return {
            "unique_count": len(str_sample.unique()),
            "total_count": len(str_sample),
            "uniqueness_ratio": len(str_sample.unique()) / len(str_sample) if len(str_sample) > 0 else 0,
            "avg_length": str_sample.str.len().mean(),
            "length_variance": str_sample.str.len().var(),
            "null_ratio": original_sample.isna().mean()
        }
    
    def calculate_compatibility_score(self, analysis1: Dict, analysis2: Dict) -> float:
        """Calcula score de compatibilidad entre dos análisis de columnas"""
        
        # Pesos para diferentes factores
        type_weight = 0.5
        pattern_weight = 0.3  
        stats_weight = 0.2
        
        # 1. Similitud de tipo de datos
        type_sim = self._calculate_type_similarity(analysis1["data_type"], analysis2["data_type"])
        
        # 2. Similitud de patrones
        patterns1 = set(analysis1["patterns"])
        patterns2 = set(analysis2["patterns"])
        pattern_sim = len(patterns1 & patterns2) / max(len(patterns1 | patterns2), 1)
        
        # 3. Similitud estadística
        stats_sim = self._calculate_stats_similarity(analysis1["stats"], analysis2["stats"])
        
        # Score final ponderado
        total_score = (
            type_sim * type_weight +
            pattern_sim * pattern_weight +
            stats_sim * stats_weight
        )
        
        return total_score
    
    def _calculate_type_similarity(self, type1: str, type2: str) -> float:
        """Calcula similitud entre tipos de datos"""
        if type1 == type2:
            return 1.0
        
        # Tipos compatibles con scores altos
        compatible_pairs = {
            ("document_id", "numeric"): 0.9,
            ("phone", "numeric"): 0.7,
            ("code", "mixed"): 0.8,
            ("currency", "numeric"): 0.8,
            ("percentage", "numeric"): 0.7,
            ("document_id", "code"): 0.6,
        }
        
        pair = (type1, type2)
        return compatible_pairs.get(pair, compatible_pairs.get(pair[::-1], 0.0))
    
    def _calculate_stats_similarity(self, stats1: Dict, stats2: Dict) -> float:
        """Calcula similitud estadística"""
        similarities = []
        
        # Similitud de unicidad (importante para claves primarias)
        uniqueness_diff = abs(stats1["uniqueness_ratio"] - stats2["uniqueness_ratio"])
        uniqueness_sim = 1.0 - uniqueness_diff
        similarities.append(uniqueness_sim)
        
        # Similitud de longitud promedio
        if stats1["avg_length"] > 0 and stats2["avg_length"] > 0:
            length_ratio = min(stats1["avg_length"], stats2["avg_length"]) / max(stats1["avg_length"], stats2["avg_length"])
            similarities.append(length_ratio)
        
        return np.mean(similarities) if similarities else 0.0
    
    def find_best_column_matches(
        self, 
        df1: pd.DataFrame, 
        df2: pd.DataFrame,
        min_overlap_ratio: float = 0.15
    ) -> List[Dict]:
        """
        Encuentra las mejores correspondencias entre columnas de dos DataFrames
        combinando análisis de patrones con solapamiento de valores reales
        """        
        suggestions = []
        
        for col1, col2 in itertools.product(df1.columns, df2.columns):
            
            # 1. Análisis de patrones
            analysis1 = self.analyze_column_patterns(df1[col1])
            analysis2 = self.analyze_column_patterns(df2[col2])
            
            pattern_score = self.calculate_compatibility_score(analysis1, analysis2)
            
            # 2. Análisis de solapamiento de valores reales
            overlap_score, overlap_examples = self._calculate_value_overlap(df1[col1], df2[col2])
            
            # 3. Score combinado (priorizando patrones para casos como el tuyo)
            if pattern_score >= 0.6:  # Patrones muy compatibles
                combined_score = pattern_score * 0.7 + overlap_score * 0.3
            else:  # Patrones menos compatibles, dar más peso al solapamiento
                combined_score = pattern_score * 0.4 + overlap_score * 0.6
            
            # Solo considerar si supera el umbral mínimo
            if combined_score >= self.similarity_threshold or overlap_score >= min_overlap_ratio:
                
                suggestion = {
                    "left_column": col1,
                    "right_column": col2,
                    "combined_score": round(combined_score, 3),
                    "pattern_score": round(pattern_score, 3),
                    "overlap_score": round(overlap_score, 3),
                    "overlap_examples": overlap_examples,
                    "left_type": analysis1["data_type"],
                    "right_type": analysis2["data_type"],
                    "compatible": combined_score >= self.similarity_threshold,
                    "recommendation": self._generate_recommendation(pattern_score, overlap_score, combined_score)
                }
                
                suggestions.append(suggestion)
                        
        # Ordenar por score combinado descendente
        suggestions.sort(key=lambda x: x["combined_score"], reverse=True)
        
        return suggestions
    
    def _calculate_value_overlap(self, series1: pd.Series, series2: pd.Series) -> Tuple[float, List[str]]:
        """Calcula el solapamiento de valores entre dos series"""
        # Obtener muestras y limpiar valores
        sample1 = set(series1.dropna().astype(str).str.strip().unique()[:self.sample_size])
        sample2 = set(series2.dropna().astype(str).str.strip().unique()[:self.sample_size])
        
        if not sample1 or not sample2:
            return 0.0, []
        
        # Solapamiento exacto
        overlap = sample1 & sample2
        overlap_score = len(overlap) / min(len(sample1), len(sample2))
        
        return overlap_score, list(overlap)[:5]
    
    def _generate_recommendation(self, pattern_score: float, overlap_score: float, combined_score: float) -> str:
        """Genera recomendación para el usuario"""
        if combined_score >= 0.7:
            return "✅ Altamente recomendado para cruce"
        elif combined_score >= 0.4:
            if pattern_score > overlap_score:
                return "⚠️ Tipos compatibles, pocos valores coincidentes. Verificar manualmente"
            else:
                return "⚠️ Buenos valores coincidentes, verificar tipos"
        else:
            return "❌ No recomendado para cruce"


# Instancia global
column_analyzer = ColumnAnalyzer()
