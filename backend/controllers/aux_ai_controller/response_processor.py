# controllers/aux_ai_controller/response_processor.py
from typing import Dict, Any
import re


class ResponseProcessor:
    """Procesa y mejora las respuestas del AI"""
    
    def process(self, ai_response: str, query_analysis: Dict[str, Any], file_context: str = None) -> str:
        """Procesa la respuesta del AI"""
        
        query_type = query_analysis['type']
        
        # Limpiar respuesta
        processed = self._clean_response(ai_response)
        
        # Eliminar placeholders inventados
        processed = self._remove_placeholders(processed)
        
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
            "Con gusto te ayudo",
            "te mostraré",
            "voy a procesar"
        ]
        
        for phrase in remove_phrases:
            if phrase.lower() in response.lower():
                sentences = response.split('.')
                response = '. '.join([s for s in sentences if phrase.lower() not in s.lower()])
        
        return response.strip()
    
    def _remove_placeholders(self, response: str) -> str:
        """Elimina placeholders inventados como [Valor Promedio]"""
        # Detectar y eliminar placeholders entre corchetes
        patterns = [
            r'\[Valor.*?\]',
            r'\[.*?Promedio.*?\]',
            r'\[.*?Mínimo.*?\]',
            r'\[.*?Máximo.*?\]',
            r'\[.*?Mediana.*?\]',
            r'\[.*?Desviación.*?\]',
        ]
        
        for pattern in patterns:
            response = re.sub(pattern, '**[dato no disponible]**', response, flags=re.IGNORECASE)
        
        # Si hay muchos placeholders, agregar aviso
        if response.count('[dato no disponible]') > 3:
            response = response.replace('[dato no disponible]', '')
            response += "\n\n⚠️ **Nota:** Los valores estadísticos específicos no están disponibles en este momento. Para ver estadísticas calculadas en tiempo real, usa la sección **Análisis** de la aplicación."
        
        return response
    
    def _add_structure_suggestions(self, response: str) -> str:
        """Agrega sugerencias para análisis de estructura"""
        if "columnas" in response.lower() or "estructura" in response.lower():
            suggestions = """

💡 **Próximos pasos:**
• Usa los **filtros de tabla** para buscar datos específicos
• Ve a **Análisis** para estadísticas automáticas
• Exporta en **CSV o Excel** cuando lo necesites
"""
            return response + suggestions
        return response
    
    def _add_statistical_suggestions(self, response: str) -> str:
        """Agrega sugerencias para estadísticas"""
        suggestions = """

💡 **Sugerencia:** Para ver estadísticas reales calculadas, ve a la sección **Análisis** de la aplicación donde encontrarás:
• Promedios, medianas y desviaciones estándar
• Distribuciones y frecuencias
• Gráficos interactivos
"""
        return response + suggestions
    
    def _add_filtering_suggestions(self, response: str) -> str:
        """Agrega sugerencias para filtrado"""
        suggestions = """

💡 **Sugerencia:** Usa la **barra de búsqueda** y los **filtros de columna** en la tabla de datos.
"""
        return response + suggestions


# Instancia global
response_processor = ResponseProcessor()
