# controllers/aux_ai_controller/prompt_builder.py
from typing import Dict, Any


class PromptBuilder:
    """Construye prompts optimizados para Gemini"""
    
    def build(self, context: str, question: str, query_analysis: Dict[str, Any], conversation_context: str = "") -> str:
        """Construye el prompt según el tipo de consulta"""
        
        query_type = query_analysis['type']
        
        if query_type == 'greeting':
            return self._build_greeting_prompt(context, question, conversation_context)
        elif query_type == 'structure_analysis':
            return self._build_structure_prompt(context, question, query_analysis, conversation_context)
        elif query_type == 'statistical':
            return self._build_statistical_prompt(context, question, conversation_context)
        elif query_type == 'filtering':
            return self._build_filtering_prompt(context, question, conversation_context)
        else:
            return self._build_general_prompt(context, question, conversation_context)
    
    def _build_greeting_prompt(self, context: str, question: str, conversation_context: str) -> str:
        """Prompt para saludos"""
        return f"""
Eres un asistente de análisis de datos amigable y profesional.

{conversation_context}

CONTEXTO DE ARCHIVOS:
{context}

INSTRUCCIONES:
- Saluda de forma BREVE y amigable (máximo 2-3 líneas)
- Si hay conversación previa, reconócela brevemente
- Menciona cuántos archivos hay disponibles
- Pregunta en qué puedes ayudar
- NO des explicaciones largas en el saludo

PREGUNTA: {question}

RESPUESTA (breve y directa):
"""
    
    def _build_structure_prompt(self, context: str, question: str, query_analysis: Dict, conversation_context: str) -> str:
        """Prompt para análisis de estructura"""
        target_file = query_analysis.get('target_file')
        
        return f"""
Eres un asistente experto en análisis de datos.

{conversation_context}

CONTEXTO DEL ARCHIVO:
{context}

ARCHIVO A ANALIZAR: {target_file}

INSTRUCCIONES CRÍTICAS:
1. El archivo YA ESTÁ CARGADO y listo para análisis
2. Muestra la información DIRECTAMENTE sin decir que vas a cargar nada
3. Lista las columnas con numeración clara
4. Describe el tipo de datos que contiene
5. Menciona total de filas y columnas
6. Sé específico y directo
7. NO inventes código Python ni análisis simulados

PREGUNTA: {question}

RESPUESTA (muestra los datos reales que tienes en el contexto):
"""
    
    def _build_statistical_prompt(self, context: str, question: str, conversation_context: str) -> str:
        """Prompt para análisis estadístico"""
        return f"""
Eres un asistente experto en estadística y análisis de datos.

{conversation_context}

CONTEXTO:
{context}

INSTRUCCIONES:
1. Identifica las columnas relevantes para el análisis solicitado
2. Describe qué tipo de estadísticas se pueden calcular
3. Sugiere usar las herramientas de la sección "Análisis"
4. Sé específico sobre qué columnas usar
5. NO inventes datos ni resultados
6. Si el contexto de conversación indica un archivo específico, úsalo

PREGUNTA: {question}

RESPUESTA:
"""
    
    def _build_filtering_prompt(self, context: str, question: str, conversation_context: str) -> str:
        """Prompt para filtrado"""
        return f"""
Eres un asistente que ayuda con búsquedas y filtros en datos.

{conversation_context}

CONTEXTO:
{context}

INSTRUCCIONES:
- Explica cómo realizar el filtro solicitado
- Menciona las herramientas disponibles
- Sugiere columnas relevantes

PREGUNTA: {question}

RESPUESTA:
"""
    
    def _build_general_prompt(self, context: str, question: str, conversation_context: str) -> str:
        """Prompt general"""
        return f"""
Eres un asistente de análisis de datos útil y preciso.

{conversation_context}

CONTEXTO:
{context}

INSTRUCCIONES:
1. Mantén continuidad con la conversación anterior
2. Si hay un archivo activo, úsalo para responder
3. Responde de forma directa y útil
4. Proporciona información accionable
5. Sé conciso pero completo

PREGUNTA: {question}

RESPUESTA:
"""


# Instancia global
prompt_builder = PromptBuilder()
