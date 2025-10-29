# controllers/aux_ai_controller/response_processor.py
from typing import Dict, Any
import re


class ResponseProcessor:
    """Procesa y mejora respuestas del AI con validación inteligente"""
    
    def process(self, ai_response: str, query_analysis: Dict[str, Any], file_context: str = None) -> str:
        """Post-procesamiento inteligente de respuestas"""
        
        query_type = query_analysis['type']
        confidence = query_analysis.get('confidence', 0.7)
        
        # Limpiar respuesta
        processed = self._clean_response(ai_response)
        
        # Validar y corregir placeholders
        processed = self._validate_and_fix_placeholders(processed, query_type)
        
        # Agregar sugerencias contextuales
        processed = self._add_contextual_suggestions(processed, query_type, confidence)
        
        # Mejorar formato Markdown
        processed = self._enhance_markdown(processed)
        
        return processed
    
    def _clean_response(self, response: str) -> str:
        """Limpia respuesta de frases redundantes"""
        remove_phrases = [
            r"Permíteme un momento.*?\.",
            r"Voy a cargar.*?\.",
            r"Necesito cargar.*?\.",
            r"Para proporcionarte.*?\.",
            r"Con gusto te ayudo.*?\.",
            r"te mostraré.*?\.",
            r"voy a procesar.*?\."
        ]
        
        cleaned = response
        for pattern in remove_phrases:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
        
        # Eliminar múltiples líneas en blanco
        cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
        
        return cleaned.strip()
    
    def _validate_and_fix_placeholders(self, response: str, query_type: str) -> str:
        """Valida y corrige placeholders inventados"""
        
        # Detectar placeholders entre corchetes
        placeholder_patterns = [
            r'\[Valor.*?\]',
            r'\[.*?[Pp]romedio.*?\]',
            r'\[.*?[Mm][íi]nimo.*?\]',
            r'\[.*?[Mm][áa]ximo.*?\]',
            r'\[.*?[Mm]ediana.*?\]',
            r'\[.*?[Dd]esviaci[óo]n.*?\]',
            r'\[.*?[Tt]otal.*?\]',
            r'\[.*?[Cc]antidad.*?\]'
        ]
        
        placeholder_count = 0
        for pattern in placeholder_patterns:
            matches = re.findall(pattern, response, flags=re.IGNORECASE)
            placeholder_count += len(matches)
        
        # Si hay muchos placeholders, agregar advertencia
        if placeholder_count > 3:
            warning = "\n\n⚠️ **Nota:** Los valores mostrados son ejemplos. Para estadísticas reales calculadas, usa la sección **Análisis** de la aplicación."
            response = re.sub(r'\[.*?\]', '**[valor no calculado]**', response)
            response += warning
        
        return response
    
    def _add_contextual_suggestions(self, response: str, query_type: str, confidence: float) -> str:
        """Agrega sugerencias basadas en tipo de consulta y confianza"""
        
        suggestions = {
            'structure_analysis': "\n\n💡 **Próximos pasos:** Usa los filtros de tabla para explorar o ve a **Análisis** para estadísticas.",
            'statistical': "\n\n💡 **Tip:** Para análisis más profundo, prueba la sección **Visualización** para gráficos interactivos.",
            'filtering': "\n\n💡 **Sugerencia:** Usa la **barra de búsqueda** y los **filtros de columna** en la tabla.",
            'temporal': "\n\n💡 **Tip:** Para análisis temporal avanzado, considera exportar y usar herramientas de series de tiempo.",
            'export': "\n\n💡 **Recuerda:** Puedes exportar en **CSV** o **Excel** desde cualquier vista de datos."
        }
        
        # Solo agregar si la confianza es alta
        if confidence > 0.75 and query_type in suggestions:
            return response + suggestions[query_type]
        
        return response
    
    def _enhance_markdown(self, response: str) -> str:
        """Mejora formato Markdown para mejor legibilidad"""
        
        # Asegurar espacios antes de headers
        response = re.sub(r'([^\n])\n(#{1,3} )', r'\1\n\n\2', response)
        
        # Mejorar listas
        response = re.sub(r'\n([•\-\*]) ', r'\n\n\1 ', response)
        
        # Asegurar negrita correcta
        response = re.sub(r'\*\*\s+', r'**', response)
        response = re.sub(r'\s+\*\*', r'**', response)
        
        return response

response_processor = ResponseProcessor()
