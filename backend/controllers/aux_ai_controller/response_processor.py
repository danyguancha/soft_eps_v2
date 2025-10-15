# controllers/aux_ai_controller/response_processor.py
from typing import Dict, Any


class ResponseProcessor:
    """Procesa y mejora las respuestas del AI"""
    
    def process(self, ai_response: str, query_analysis: Dict[str, Any], file_context: str = None) -> str:
        """Procesa la respuesta del AI"""
        
        query_type = query_analysis['type']
        
        # Limpiar respuesta
        processed = self._clean_response(ai_response)
        
        # Agregar sugerencias según el tipo
        if query_type == 'structure_analysis':
            processed = self._add_structure_suggestions(processed)
        elif query_type == 'statistical':
            processed = self._add_statistical_suggestions(processed)
        elif query_type == 'filtering':
            processed = self._add_filtering_suggestions(processed)
        
        return processed
    
    def _clean_response(self, response: str) -> str:
        """Limpia la respuesta"""
        # Eliminar frases redundantes
        remove_phrases = [
            "Permíteme un momento",
            "Voy a cargar",
            "Necesito cargar",
            "Para proporcionarte",
            "Con gusto te ayudo"
        ]
        
        for phrase in remove_phrases:
            if phrase in response:
                # Eliminar la oración completa que contiene la frase
                sentences = response.split('.')
                response = '. '.join([s for s in sentences if phrase not in s])
        
        return response.strip()
    
    def _add_structure_suggestions(self, response: str) -> str:
        """Agrega sugerencias para análisis de estructura"""
        suggestions = """

💡 **Próximos pasos:**
• Usa los filtros para buscar datos específicos
• Genera estadísticas en la sección "Análisis"
• Exporta los datos en formato CSV o Excel
"""
        return response + suggestions
    
    def _add_statistical_suggestions(self, response: str) -> str:
        """Agrega sugerencias para estadísticas"""
        suggestions = """

💡 **Sugerencia:** Ve a la sección "Análisis" para estadísticas automáticas.
"""
        return response + suggestions
    
    def _add_filtering_suggestions(self, response: str) -> str:
        """Agrega sugerencias para filtrado"""
        suggestions = """

💡 **Sugerencia:** Usa la barra de búsqueda y filtros en la tabla de datos.
"""
        return response + suggestions


# Instancia global
response_processor = ResponseProcessor()
