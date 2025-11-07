# controllers/ai_controller.py - ORQUESTADOR CON NLP AUTOMÁTICO
import google.generativeai as genai
import asyncio
from typing import Dict, Any
from config import GEMINI_API_KEY

# Importar componentes NLP automatizados
from controllers.aux_ai_controller.context_builder import context_builder
from controllers.aux_ai_controller.query_analyzer import query_analyzer
from controllers.aux_ai_controller.prompt_builder import prompt_builder
from controllers.aux_ai_controller.response_processor import response_processor
from controllers.aux_ai_controller.conversation_manager import conversation_manager
from controllers.aux_ai_controller.sql_executor import sql_executor
from controllers.aux_ai_controller.intent_classifier import intent_classifier

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-2.0-flash-exp')


class AIController:
    """
    Orquestador principal del asistente AI con NLP automático
    
    Características:
    - Detección automática de intenciones con spaCy
    - Análisis semántico avanzado
    - Cálculo de estadísticas en tiempo real
    - Gestión de conversaciones con contexto
    - Respuestas inteligentes y contextuales
    """
    
    def __init__(self):
        print("Inicializando AIController con NLP automático...")
        self.context_builder = context_builder
        self.query_analyzer = query_analyzer
        self.prompt_builder = prompt_builder
        self.response_processor = response_processor
        self.conversation_manager = conversation_manager
        self.sql_executor = sql_executor
        self.intent_classifier = intent_classifier
        print("AIController listo")
    
    async def ask_ai(self, request) -> Dict[str, Any]:
        """
        Procesa consulta del usuario con análisis NLP automático
        
        Flujo:
        1. Análisis NLP automático (intención, entidades, keywords)
        2. Detección automática de archivo objetivo
        3. Cálculo de estadísticas si es necesario
        4. Construcción de contexto enriquecido
        5. Generación de respuesta con Gemini
        6. Post-procesamiento y mejora de respuesta
        """
        try:
            session_id = getattr(request, 'session_id', 'default_session')
            print(f"Session: {session_id}")
            
            # PASO 1: Guardar pregunta del usuario en el historial
            self.conversation_manager.add_message(session_id, 'user', request.question)
            
            # PASO 2: Obtener archivos disponibles
            available_files = await self.context_builder.get_available_files()
            print(f"Archivos disponibles: {len(available_files)}")
            for f in available_files:
                print(f"   • {f['original_name']} ({f['total_rows']:,} filas)")
            
            # PASO 3: ANÁLISIS NLP AUTOMÁTICO
            print(f"\nAnalizando consulta con NLP...")
            query_analysis = self.query_analyzer.analyze(request.question, available_files)
            
            print(f"Intención detectada: {query_analysis['type']} (confianza: {query_analysis.get('confidence', 0)*100:.1f}%)")
            print(f"   Acción: {query_analysis['intent']}")
            print(f"   Requiere archivo: {query_analysis['requires_file']}")
            
            if query_analysis.get('keywords'):
                print(f"   Palabras clave: {', '.join(query_analysis['keywords'][:5])}")
            
            if query_analysis.get('numerical_operations'):
                print(f"   Operaciones: {', '.join(query_analysis['numerical_operations'])}")
            
            # PASO 4: DETERMINACIÓN INTELIGENTE DE ARCHIVO OBJETIVO
            print(f"\nDeterminando archivo objetivo...")
            
            # Prioridad 1: Archivo detectado automáticamente por NLP
            target_file = query_analysis.get('target_file')
            
            # Prioridad 2: Archivo del contexto conversacional
            if not target_file:
                target_file = self.conversation_manager.extract_file_context_nlp(
                    session_id, 
                    request.question, 
                    available_files,
                    query_analysis
                )
            
            # Prioridad 3: Archivo explícito del request
            if not target_file:
                target_file = request.file_context
            
            # Prioridad 4: Si solo hay un archivo, usarlo
            if not target_file and len(available_files) == 1:
                target_file = available_files[0]['file_id']
                print(f"   Auto-seleccionando único archivo disponible")
            
            if target_file:
                file_name = next((f['original_name'] for f in available_files if f['file_id'] == target_file), target_file)
                print(f"Archivo objetivo: {file_name}")
            else:
                print(f"No se requiere archivo específico")
            
            # PASO 5: CÁLCULO AUTOMÁTICO DE ESTADÍSTICAS
            calculated_stats = None
            
            if query_analysis['type'] == 'statistical' and target_file:
                print(f"\nCalculando estadísticas en tiempo real...")
                
                # Buscar información del archivo
                file_info = next((f for f in available_files if f['file_id'] == target_file or target_file in f['original_name']), None)
                
                if file_info and file_info.get('columns'):
                    parquet_path = file_info.get('parquet_path')
                    
                    if parquet_path:
                        print(f"   Usando: {parquet_path}")
                        print(f"   Columnas a analizar: {len(file_info['columns'])}")
                        
                        calculated_stats = await self.sql_executor.calculate_statistics(
                            file_info['file_id'],
                            file_info['columns'],
                            parquet_path
                        )
                        
                        if calculated_stats:
                            num_numeric = len(calculated_stats.get('numeric', {}))
                            num_categorical = len(calculated_stats.get('categorical', {}))
                            print(f"   Estadísticas calculadas:")
                            print(f"      • {num_numeric} columnas numéricas analizadas")
                            print(f"      • {num_categorical} columnas categóricas analizadas")
                        else:
                            print(f"   No se pudieron calcular estadísticas")
                    else:
                        print(f"   Parquet no disponible para {target_file}")
                else:
                    print(f"   Información del archivo no disponible")
            
            # PASO 6: CONSTRUCCIÓN DE CONTEXTO ENRIQUECIDO
            print(f"\nConstruyendo contexto...")
            context = await self.context_builder.build_context(target_file)
            
            # Agregar estadísticas calculadas al contexto
            if calculated_stats:
                context += "\n\n" + "="*60
                context += "\nESTADÍSTICAS CALCULADAS EN TIEMPO REAL\n"
                context += "="*60 + "\n"
                context += self._format_statistics(calculated_stats)
                print(f"   Contexto enriquecido con estadísticas reales")
            
            # PASO 7: CONTEXTO CONVERSACIONAL
            conversation_context = self.conversation_manager.build_conversation_context(session_id)
            
            if conversation_context:
                intent_pattern = self.conversation_manager.get_intent_pattern(session_id)
                print(f"   Contexto conversacional disponible")
                print(f"   Patrón de intenciones: {' → '.join(intent_pattern[-3:])}")
            
            # PASO 8: CONSTRUCCIÓN DE PROMPT DINÁMICO
            print(f"\nGenerando prompt optimizado...")
            prompt = self.prompt_builder.build(
                context, 
                request.question, 
                query_analysis,
                conversation_context
            )
            
            prompt_preview = prompt[:200].replace('\n', ' ')
            print(f"   Preview: {prompt_preview}...")
            
            # PASO 9: GENERACIÓN DE RESPUESTA CON GEMINI
            print(f"\nConsultando a Gemini...")
            ai_response = await self._generate_response(prompt)
            
            response_preview = ai_response[:150].replace('\n', ' ')
            print(f"   Respuesta recibida: {response_preview}...")
            
            # PASO 10: POST-PROCESAMIENTO DE RESPUESTA
            print(f"\nPost-procesando respuesta...")
            final_response = self.response_processor.process(
                ai_response,
                query_analysis,
                target_file
            )
            
            # PASO 11: GUARDAR EN HISTORIAL
            self.conversation_manager.add_message(
                session_id, 
                'assistant', 
                final_response,
                {
                    'query_type': query_analysis['type'],
                    'target_file': target_file,
                    'confidence': query_analysis.get('confidence'),
                    'has_stats': bool(calculated_stats)
                }
            )
            
            print(f"\n{'='*60}")
            print(f"CONSULTA COMPLETADA EXITOSAMENTE")
            print(f"{'='*60}\n")
            
            return {
                "success": True,
                "response": final_response,
                "query_type": query_analysis['type'],
                "confidence": query_analysis.get('confidence'),
                "target_file": target_file,
                "session_id": session_id,
                "has_calculated_stats": bool(calculated_stats),
                "intent_action": query_analysis['intent'],
                "keywords_detected": query_analysis.get('keywords', []),
                "entities_detected": query_analysis.get('entities', [])
            }
            
        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            
            print(f"\n{'='*60}")
            print(f"ERROR EN CONSULTA")
            print(f"{'='*60}")
            print(f"Error: {str(e)}")
            print(f"Traceback:\n{error_trace}")
            print(f"{'='*60}\n")
            
            return {
                "success": False,
                "response": f"Lo siento, ocurrió un error al procesar tu consulta. Por favor, intenta reformularla o contacta al administrador.",
                "error": str(e),
                "traceback": error_trace
            }
    
    def _format_statistics(self, stats: Dict[str, Any]) -> str:
        """
        Formatea estadísticas calculadas para el contexto del AI
        
        Formato optimizado para que Gemini pueda interpretar y presentar
        """
        formatted = []
        
        # Estadísticas numéricas
        if 'numeric' in stats and stats['numeric']:
            formatted.append("\nCOLUMNAS NUMÉRICAS:\n")
            
            for col, values in stats['numeric'].items():
                formatted.append(f"**{col}**")
                formatted.append(f"  • Promedio: {values['promedio']:.2f}")
                formatted.append(f"  • Mediana: {values['mediana']:.2f}")
                formatted.append(f"  • Mínimo: {values['minimo']:.2f}")
                formatted.append(f"  • Máximo: {values['maximo']:.2f}")
                formatted.append(f"  • Desviación Estándar: {values['desviacion_std']:.2f}")
                formatted.append(f"  • Total de registros válidos: {values['count']:,}")
                formatted.append("")  # Línea en blanco
        
        # Estadísticas categóricas
        if 'categorical' in stats and stats['categorical']:
            formatted.append("\nCOLUMNAS CATEGÓRICAS:\n")
            
            for col, values in stats['categorical'].items():
                formatted.append(f"**{col}** ({values['valores_unicos']} valores únicos)")
                formatted.append(f"  Top 5 más frecuentes:")
                
                for i, item in enumerate(values['top_5'], 1):
                    formatted.append(f"  {i}. {item['value']}: {item['frequency']:,} registros ({item['percentage']:.1f}%)")
                
                formatted.append("")  # Línea en blanco
        
        # Resumen
        total_numeric = len(stats.get('numeric', {}))
        total_categorical = len(stats.get('categorical', {}))
        
        formatted.append(f"\nRESUMEN:")
        formatted.append(f"  • {total_numeric} columnas numéricas analizadas")
        formatted.append(f"  • {total_categorical} columnas categóricas analizadas")
        formatted.append(f"  • Estadísticas calculadas en tiempo real con SQL")
        
        return "\n".join(formatted)
    
    async def _generate_response(self, prompt: str) -> str:
        """
        Genera respuesta con Gemini 2.0 Flash
        
        Configuración optimizada para respuestas rápidas y precisas
        """
        try:
            loop = asyncio.get_event_loop()
            
            # Configuración de generación
            generation_config = {
                'temperature': 0.7,  # Balance entre creatividad y precisión
                'top_p': 0.95,
                'top_k': 40,
                'max_output_tokens': 2048,
            }
            
            response = await loop.run_in_executor(
                None,
                lambda: model.generate_content(
                    prompt,
                    generation_config=generation_config
                )
            )
            
            return response.text
            
        except Exception as e:
            print(f"Error en generación con Gemini: {e}")
            return f"Error generando respuesta con el modelo AI: {str(e)}"
    
    def get_conversation_summary(self, session_id: str) -> Dict[str, Any]:
        """Obtiene resumen de la conversación"""
        history = self.conversation_manager.get_conversation_history(session_id)
        intent_pattern = self.conversation_manager.get_intent_pattern(session_id)
        active_file = self.conversation_manager.get_active_file(session_id)
        
        return {
            'total_messages': len(history),
            'intent_pattern': intent_pattern,
            'active_file': active_file,
            'recent_intents': intent_pattern[-5:] if intent_pattern else []
        }
    
    def reset_conversation(self, session_id: str) -> Dict[str, str]:
        """Reinicia una conversación"""
        self.conversation_manager.clear_conversation(session_id)
        return {
            "success": True,
            "message": f"Conversación {session_id} reiniciada"
        }


# Instancia global
ai_controller = AIController()
