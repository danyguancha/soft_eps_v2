# controllers/aux_ai_controller/prompt_builder.py
from typing import Dict, Any


class PromptBuilder:
    """Construye prompts con instrucciones específicas de formato"""
    
    def build(
        self, 
        context: str, 
        question: str, 
        query_analysis: Dict[str, Any], 
        conversation_context: str = ""
    ) -> str:
        """Construcción dinámica de prompts"""
        
        intent = query_analysis['type']
        
        # Instrucciones de formato ESTRICTAS para Gemini
        format_instructions = """
REGLAS DE FORMATO MARKDOWN OBLIGATORIAS:
1. Usa headers con ## o ### (sin números)
2. Para negritas: usa **texto** (dos asteriscos, NO tres ***)
3. Para listas: usa - o • (NO *)
4. Para listas numeradas: usa 1. 2. 3.
5. NO uses *** (triple asterisco)
6. Separa secciones con líneas en blanco
7. Para resaltar: usa **negrita** o `código`

EJEMPLO DE FORMATO CORRECTO:

## Análisis Temporal

**Columnas de fecha:** Las siguientes columnas contienen fechas:

- Fecha de Nacimiento
- Fecha de Ingreso
- Fecha de Seguimiento

**Granularidad disponible:**
1. Análisis diario
2. Análisis mensual
3. Análisis anual

**Tendencias:** El análisis puede mostrar...
"""
        
        nlp_insights = self._build_nlp_insights(query_analysis)
        
        # Seleccionar template
        prompt_templates = {
            'greeting': self._build_greeting_prompt,
            'structure_analysis': self._build_structure_prompt,
            'statistical': self._build_statistical_prompt,
            'filtering': self._build_filtering_prompt,
            'temporal': self._build_temporal_prompt,
            'help': self._build_help_prompt,
            'export': self._build_export_prompt,
            'comparison': self._build_comparison_prompt,
            'general': self._build_general_prompt
        }
        
        builder = prompt_templates.get(intent, self._build_general_prompt)
        
        return builder(context, question, query_analysis, conversation_context, nlp_insights, format_instructions)
    
    def _build_nlp_insights(self, analysis: Dict[str, Any]) -> str:
        """Construye resumen de insights NLP"""
        parts = []
        
        parts.append(f"**Intención detectada:** {analysis['type']} (Confianza: {analysis.get('confidence', 0)*100:.1f}%)")
        
        if analysis.get('keywords'):
            parts.append(f"**Palabras clave:** {', '.join(analysis['keywords'][:5])}")
        
        if analysis.get('numerical_operations'):
            parts.append(f"**Operaciones solicitadas:** {', '.join(analysis['numerical_operations'])}")
        
        if analysis.get('specific_columns'):
            parts.append(f"**Columnas mencionadas:** {', '.join(analysis['specific_columns'])}")
        
        return "\n".join(parts)
    
    def _build_greeting_prompt(self, context, question, analysis, conv_context, nlp_insights, format_instructions) -> str:
        return f"""
Eres EvalNoteBot, un asistente de análisis de datos profesional.

{format_instructions}

{conv_context}

ARCHIVOS DISPONIBLES:
{context}

INSTRUCCIONES:
- Saluda BREVEMENTE (máximo 2 líneas)
- Lista los archivos con bullets (-)
- Pregunta en qué puedes ayudar

PREGUNTA: {question}

RESPUESTA EN MARKDOWN CORRECTO:
"""
    
    def _build_temporal_prompt(self, context, question, analysis, conv_context, nlp_insights, format_instructions) -> str:
        return f"""
Eres EvalNoteBot, experto en análisis temporal y series de tiempo.

{format_instructions}

{conv_context}

ANÁLISIS:
{nlp_insights}

DATOS:
{context}

INSTRUCCIONES CRÍTICAS:
1. Usa formato Markdown CORRECTO (ver reglas arriba)
2. Identifica columnas de fecha/tiempo
3. Describe análisis temporal posible
4. Usa headers con ##
5. Usa listas con - (guión)
6. Usa **negrita** para destacar (dos asteriscos, NO tres)
7. NO uses *** (triple asterisco)

FORMATO DE RESPUESTA:

## Análisis Temporal Disponible

**Columnas de fecha identificadas:**

- [Nombre columna 1]
- [Nombre columna 2]

**Análisis posibles:**

1. **Tendencias:** [Descripción]
2. **Estacionalidad:** [Descripción]
3. **Granularidad:** Diaria, mensual o anual

**Visualizaciones sugeridas:**

- Gráficos de líneas para tendencias
- Histogramas para distribución temporal
- Mapas de calor para estacionalidad

PREGUNTA: {question}

RESPUESTA (USA MARKDOWN CORRECTO):
"""
    
    def _build_structure_prompt(self, context, question, analysis, conv_context, nlp_insights, format_instructions) -> str:
        return f"""
Eres EvalNoteBot, especialista en análisis de estructuras de datos.

{format_instructions}

{conv_context}

ANÁLISIS NLP:
{nlp_insights}

CONTEXTO:
{context}

INSTRUCCIONES:
1. Usa ## para headers
2. Usa - para listas
3. Usa **negrita** (dos asteriscos)
4. NO uses *** (triple asterisco)
5. Numera columnas con formato: 1. 2. 3.

FORMATO:

## Estructura del Archivo

**Total de columnas:** [número]
**Total de registros:** [número]

**Columnas Numéricas:**

1. [Nombre] - [tipo]
2. [Nombre] - [tipo]

**Columnas Categóricas:**

1. [Nombre] - [valores únicos]
2. [Nombre] - [valores únicos]

PREGUNTA: {question}

RESPUESTA:
"""
    
    def _build_statistical_prompt(self, context, question, analysis, conv_context, nlp_insights, format_instructions) -> str:
        operations = analysis.get('numerical_operations', [])
        operations_str = ", ".join(operations) if operations else "estadísticas generales"
        
        has_calculated_stats = "ESTADÍSTICAS CALCULADAS" in context
        
        if has_calculated_stats:
            return f"""
    Eres EvalNoteBot, experto en estadística y análisis de datos.

    {format_instructions}

    {conv_context}

    ANÁLISIS DE LA CONSULTA:
    {nlp_insights}
    **Operaciones solicitadas:** {operations_str}

    DATOS CON ESTADÍSTICAS CALCULADAS:
    {context}

    INSTRUCCIONES CRÍTICAS:
    1. El usuario pide ESTADÍSTICAS, NO la estructura
    2. Las estadísticas YA están CALCULADAS en el contexto
    3. Presenta los valores de forma CLARA con Markdown
    4. Usa ## para headers
    5. Usa **negrita** para valores importantes
    6. Organiza por tipo de columna (Numéricas y Categóricas)
    7. Agrega una INTERPRETACIÓN breve de los resultados
    8. NO hables de columnas sin dar sus estadísticas

    FORMATO OBLIGATORIO:

    ## Estadísticas Descriptivas del Archivo

    ### Columnas Numéricas

    **[Nombre Columna 1]:**
    - Promedio: **[valor]**
    - Mediana: **[valor]**
    - Mínimo: **[valor]**
    - Máximo: **[valor]**
    - Desviación Estándar: **[valor]**
    - Total de registros: **[valor]**

    **[Nombre Columna 2]:**
    [Misma estructura...]

    ### Columnas Categóricas

    **[Nombre Columna]** ([X] valores únicos)
    - **Valor más frecuente:** [valor] ([X] registros, [X]%)
    - **Segundo valor:** [valor] ([X] registros, [X]%)
    - **Tercer valor:** [valor] ([X] registros, [X]%)

    ### Interpretación

    [Análisis breve de qué revelan estas estadísticas sobre los datos]

    PREGUNTA: {question}

    RESPUESTA (USA SOLO LOS VALORES CALCULADOS DEL CONTEXTO):
    """
        else:
            return f"""
    Eres EvalNoteBot, experto en estadística.

    {format_instructions}

    CONTEXTO:
    {context}

    PREGUNTA: {question}

    INSTRUCCIONES:
    1. El usuario pide ESTADÍSTICAS CALCULADAS
    2. El sistema NO tiene estadísticas calculadas aún
    3. Explica que para ver estadísticas con valores reales debe ir a la sección "Análisis"
    4. Indica qué tipo de estadísticas se pueden calcular
    5. Lista las columnas numéricas que se pueden analizar

    FORMATO:

    ## Estadísticas Solicitadas

    Para obtener estadísticas calculadas con valores reales, debes:

    **Opción 1:** Ve a la sección **Análisis** en la aplicación donde encontrarás:
    - Promedios, medianas y desviaciones calculadas automáticamente
    - Distribuciones y frecuencias
    - Gráficos interactivos

    **Columnas disponibles para análisis:**

    **Numéricas:**
    - [Lista de columnas numéricas]

    **Categóricas:**
    - [Lista de columnas categóricas]

    RESPUESTA:
    """

    
    def _build_filtering_prompt(self, context, question, analysis, conv_context, nlp_insights, format_instructions) -> str:
        return f"""
Eres EvalNoteBot, especialista en filtrado de datos.

{format_instructions}

{conv_context}

ANÁLISIS:
{nlp_insights}

CONTEXTO:
{context}

FORMATO:

## Filtrado Solicitado

**Columna a filtrar:** [nombre]
**Tipo de filtro:** [descripción]

**Pasos para aplicar:**

1. [Paso específico]
2. [Paso específico]
3. [Paso específico]

PREGUNTA: {question}

RESPUESTA:
"""
    
    def _build_help_prompt(self, context, question, analysis, conv_context, nlp_insights, format_instructions) -> str:
        return f"""
Eres EvalNoteBot, asistente educativo.

{format_instructions}

CONTEXTO:
{context}

FORMATO:

## Guía: [Tema]

**Qué necesitas:**

- [Requisito 1]
- [Requisito 2]

**Pasos:**

1. [Paso detallado]
2. [Paso detallado]

**Tips:**

- [Consejo práctico]

PREGUNTA: {question}

RESPUESTA:
"""
    
    def _build_export_prompt(self, context, question, analysis, conv_context, nlp_insights, format_instructions) -> str:
        return f"""
Eres EvalNoteBot, especialista en exportación.

{format_instructions}

CONTEXTO:
{context}

PREGUNTA: {question}

RESPUESTA:
"""
    
    def _build_comparison_prompt(self, context, question, analysis, conv_context, nlp_insights, format_instructions) -> str:
        return f"""
Eres EvalNoteBot, experto en comparaciones.

{format_instructions}

CONTEXTO:
{context}

PREGUNTA: {question}

RESPUESTA:
"""
    
    def _build_general_prompt(self, context, question, analysis, conv_context, nlp_insights, format_instructions) -> str:
        return f"""
Eres EvalNoteBot, asistente inteligente de análisis de datos.

{format_instructions}

{conv_context}

ANÁLISIS:
{nlp_insights}

CONTEXTO:
{context}

INSTRUCCIONES:
1. Usa formato Markdown correcto
2. NO inventes información
3. Sé específico y útil
4. Sugiere próximos pasos

PREGUNTA: {question}

RESPUESTA INTELIGENTE:
"""


# Instancia global
prompt_builder = PromptBuilder()
