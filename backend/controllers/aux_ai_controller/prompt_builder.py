# controllers/aux:ai_controller/prompt_builder.py
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

INSTRUCCIONES ESTRICTAS:
- Saluda de forma BREVE (máximo 2 líneas)
- Menciona cuántos archivos hay disponibles
- Pregunta en qué puedes ayudar
- NO des explicaciones largas

PREGUNTA: {question}

RESPUESTA BREVE:
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
1. El archivo YA ESTÁ CARGADO
2. Muestra SOLO la información que está en el CONTEXTO
3. Lista las columnas numeradas
4. NO inventes datos ni estadísticas
5. Si el contexto no tiene información específica, di "Para ver estadísticas detalladas, usa la sección Análisis de la aplicación"
6. Sé directo y preciso

PREGUNTA: {question}

RESPUESTA (usa solo datos del contexto):
"""
    
    def _build_statistical_prompt(self, context: str, question: str, conversation_context: str) -> str:
        """Prompt para análisis estadístico"""
        
        # Detectar si hay estadísticas calculadas en el contexto
        has_calculated_stats = "ESTADÍSTICAS CALCULADAS" in context
        
        if has_calculated_stats:
            return f"""
Eres un asistente experto en estadística y análisis de datos.

{conversation_context}

CONTEXTO DEL ARCHIVO CON ESTADÍSTICAS CALCULADAS:
{context}

PREGUNTA: {question}

INSTRUCCIONES:
1. Las estadísticas YA ESTÁN CALCULADAS en el contexto
2. Presenta los valores de forma clara y organizada
3. Usa formato Markdown para mejor legibilidad:
   - Usa **negritas** para números importantes
   - Usa listas con bullets (•) para organizar
   - Separa por secciones (columnas numéricas y categóricas)
4. Interpreta brevemente los resultados (qué significan los valores)
5. Sé conciso y directo
6. NO agregues sugerencias de usar otras secciones (ya tienes los datos)

FORMATO DE RESPUESTA ESPERADO:

📊 **Estadísticas de [nombre archivo]**

**Análisis de Columnas Numéricas:**

• **[Nombre columna]**
  - Promedio: **[valor]**
  - Mediana: **[valor]**
  - Rango: [mínimo] a [máximo]
  - Desviación Estándar: [valor]
  - Total registros: [cantidad]

**Análisis de Columnas Categóricas:**

• **[Nombre columna]** ([X] valores únicos)
  - [Valor más frecuente]: [cantidad] registros ([porcentaje]%)
  - [Segundo valor]: [cantidad] registros ([porcentaje]%)
  - [Tercer valor]: [cantidad] registros ([porcentaje]%)

**Interpretación:**
[Breve análisis de qué revelan estos números sobre los datos]

RESPUESTA (usa los valores calculados del contexto):
"""
        else:
            return f"""
Eres un asistente experto en estadística.

{conversation_context}

CONTEXTO:
{context}

PREGUNTA: {question}

INSTRUCCIONES CRÍTICAS:
1. NO inventes valores estadísticos (promedios, medianas, etc.)
2. NO uses placeholders como [Valor Promedio] o [Valor Mínimo]
3. Si no tienes los datos reales, di claramente: "No tengo acceso a los valores calculados en este momento"
4. Sugiere usar la sección "Análisis" de la aplicación para ver estadísticas reales
5. Identifica qué columnas son relevantes para el análisis solicitado
6. Explica qué tipo de análisis se puede hacer, pero NO inventes resultados

RESPUESTA (sin inventar valores):
"""
    
    def _build_filtering_prompt(self, context: str, question: str, conversation_context: str) -> str:
        """Prompt para filtrado"""
        return f"""
Eres un asistente que ayuda con búsquedas y filtros.

{conversation_context}

CONTEXTO:
{context}

PREGUNTA: {question}

INSTRUCCIONES:
- Explica cómo realizar el filtro solicitado
- Menciona las herramientas disponibles
- Sugiere columnas relevantes
- NO inventes datos

RESPUESTA:
"""
    
    def _build_general_prompt(self, context: str, question: str, conversation_context: str) -> str:
        """Prompt general"""
        return f"""
Eres un asistente de análisis de datos útil y honesto.

{conversation_context}

CONTEXTO:
{context}

PREGUNTA: {question}

INSTRUCCIONES CRÍTICAS:
1. Mantén continuidad con la conversación anterior
2. Si hay un archivo activo, úsalo
3. NO inventes datos, valores o estadísticas
4. Si no tienes información específica, sé honesto y di que no la tienes
5. Sugiere usar las herramientas de la aplicación cuando sea apropiado
6. Sé conciso y directo

RESPUESTA (sin inventar información):
"""


# Instancia global
prompt_builder = PromptBuilder()
