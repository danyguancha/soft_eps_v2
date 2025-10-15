# controllers/aux_ai_controller/query_analyzer.py
from typing import Dict, Any, Optional, List


class QueryAnalyzer:
    """Analiza el tipo de consulta del usuario"""
    
    def analyze(self, question: str, available_files: List[Dict] = None) -> Dict[str, Any]:
        """Analiza la pregunta y extrae información relevante"""
        question_lower = question.lower()
        
        analysis = {
            'type': self._determine_type(question_lower),
            'requires_file': self._requires_specific_file(question_lower),
            'target_file': self._extract_target_file(question_lower, available_files or []),
            'intent': self._extract_intent(question_lower)
        }
        
        return analysis
    
    def _determine_type(self, question: str) -> str:
        """Determina el tipo de consulta"""
        
        # Saludos
        if any(word in question for word in ['hola', 'hello', 'hi', 'buenos', 'buenas']):
            return 'greeting'
        
        # Análisis de estructura
        if any(word in question for word in ['estructura', 'columnas', 'campos', 'esquema', 'analiza']):
            return 'structure_analysis'
        
        # Estadísticas
        if any(word in question for word in ['estadística', 'promedio', 'suma', 'contar', 'total', 'máximo', 'mínimo']):
            return 'statistical'
        
        # Filtrado/búsqueda
        if any(word in question for word in ['filtrar', 'buscar', 'encontrar', 'busca', 'donde']):
            return 'filtering'
        
        # Temporal
        if any(word in question for word in ['temporal', 'tiempo', 'fecha', 'mes', 'año']):
            return 'temporal'
        
        # Ayuda
        if any(word in question for word in ['ayuda', 'cómo', 'tutorial', 'enseña']):
            return 'help'
        
        return 'general'
    
    def _requires_specific_file(self, question: str) -> bool:
        """Determina si la pregunta requiere un archivo específico"""
        keywords = ['estructura', 'columnas', 'datos', 'archivo', 'analiza', 'muestra', 'este']
        return any(keyword in question for keyword in keywords)
    
    def _extract_target_file(self, question: str, available_files: List[Dict]) -> Optional[str]:
        """Intenta extraer el nombre del archivo de la pregunta"""
        if not available_files:
            return None
        
        # Buscar menciones de archivos en la pregunta
        for file_info in available_files:
            file_name = file_info['original_name'].lower()
            
            # Buscar nombre completo
            if file_name in question:
                return file_info['file_id']
            
            # Buscar nombre sin extensión
            name_without_ext = file_name.rsplit('.', 1)[0]
            if name_without_ext in question:
                return file_info['file_id']
            
            # Buscar partes del nombre (mínimo 5 caracteres)
            if len(name_without_ext) >= 5:
                name_parts = name_without_ext.split()
                for part in name_parts:
                    if len(part) >= 5 and part in question:
                        return file_info['file_id']
        
        return None
    
    def _extract_intent(self, question: str) -> str:
        """Extrae la intención específica"""
        if 'columnas' in question or 'campos' in question:
            return 'show_columns'
        elif 'filas' in question or 'registros' in question:
            return 'show_rows'
        elif 'muestra' in question or 'ejemplo' in question:
            return 'show_sample'
        elif 'tipos' in question or 'formato' in question:
            return 'show_types'
        else:
            return 'analyze_full'


# Instancia global
query_analyzer = QueryAnalyzer()
