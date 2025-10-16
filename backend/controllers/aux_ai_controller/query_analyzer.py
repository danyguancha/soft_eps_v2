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
        
        # ✅ PRIORIDAD 1: Análisis estadístico (ANTES de saludos)
        statistical_keywords = [
            'estadística', 'estadisticas', 'promedio', 'suma', 'contar', 'total', 
            'máximo', 'mínimo', 'media', 'mediana', 'desviación', 'varianza',
            'generame', 'calcula', 'muéstrame', 'dame', 'obtener'
        ]
        if any(word in question for word in statistical_keywords):
            # Verificar que no sea solo un saludo
            greeting_only = ['hola', 'hello', 'hi', 'buenos', 'buenas']
            if not any(greeting in question and len(question) < 20 for greeting in greeting_only):
                return 'statistical'
        
        # ✅ PRIORIDAD 2: Análisis de estructura
        structure_keywords = [
            'estructura', 'columnas', 'campos', 'esquema', 'analiza', 
            'muestra', 'describe', 'información', 'detalles'
        ]
        if any(word in question for word in structure_keywords):
            return 'structure_analysis'
        
        # ✅ PRIORIDAD 3: Saludos (solo si es corto y sin otras palabras clave)
        greeting_keywords = ['hola', 'hello', 'hi', 'buenos', 'buenas']
        if any(word in question for word in greeting_keywords) and len(question) < 30:
            # Verificar que no tenga palabras de análisis
            analysis_words = ['archivo', 'datos', 'columnas', 'estadística', 'analiza']
            if not any(word in question for word in analysis_words):
                return 'greeting'
        
        # ✅ PRIORIDAD 4: Filtrado/búsqueda
        filter_keywords = ['filtrar', 'buscar', 'encontrar', 'busca', 'donde', 'consulta']
        if any(word in question for word in filter_keywords):
            return 'filtering'
        
        # ✅ PRIORIDAD 5: Temporal
        temporal_keywords = ['temporal', 'tiempo', 'fecha', 'mes', 'año', 'día', 'periodo']
        if any(word in question for word in temporal_keywords):
            return 'temporal'
        
        # ✅ PRIORIDAD 6: Ayuda
        help_keywords = ['ayuda', 'cómo', 'como', 'tutorial', 'enseña', 'explica']
        if any(word in question for word in help_keywords):
            return 'help'
        
        return 'general'
    
    def _requires_specific_file(self, question: str) -> bool:
        """Determina si la pregunta requiere un archivo específico"""
        keywords = [
            'estructura', 'columnas', 'datos', 'archivo', 'analiza', 
            'muestra', 'este', 'del archivo', 'estadística', 'generame'
        ]
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
            
            # ✅ Buscar por extensión mencionada
            if '.csv' in question and file_name.endswith('.csv'):
                # Buscar palabras del nombre sin extensión
                name_words = name_without_ext.replace('_', ' ').replace('-', ' ').split()
                matches = sum(1 for word in name_words if len(word) >= 4 and word in question)
                if matches >= 1:
                    return file_info['file_id']
            
            if '.xlsx' in question and file_name.endswith('.xlsx'):
                name_words = name_without_ext.replace('_', ' ').replace('-', ' ').split()
                matches = sum(1 for word in name_words if len(word) >= 4 and word in question)
                if matches >= 1:
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
        elif any(word in question for word in ['estadística', 'promedio', 'generame', 'calcula']):
            return 'calculate_statistics'
        else:
            return 'analyze_full'


# Instancia global
query_analyzer = QueryAnalyzer()
