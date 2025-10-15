# controllers/ai_controller.py
import google.generativeai as genai
import asyncio
from typing import Dict, Any
from config import GEMINI_API_KEY
from controllers.aux_ai_controller.context_builder import ContextBuilder
from controllers.aux_ai_controller.query_analyzer import QueryAnalyzer
from controllers.aux_ai_controller.prompt_builder import PromptBuilder
from controllers.aux_ai_controller.response_processor import ResponseProcessor
from controllers.aux_ai_controller.data_retriever import DataRetriever
from controllers.aux_ai_controller.conversation_manager import ConversationManager

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-2.5-flash')


class AIController:
    """Orquestador principal del asistente AI"""
    
    def __init__(self):
        self.context_builder = ContextBuilder()
        self.query_analyzer = QueryAnalyzer()
        self.prompt_builder = PromptBuilder()
        self.response_processor = ResponseProcessor()
        self.data_retriever = DataRetriever()
        self.conversation_manager = ConversationManager()
    
    async def ask_ai(self, request) -> Dict[str, Any]:
        """Procesa consulta del usuario"""
        try:
            # Generar session_id si no existe
            session_id = getattr(request, 'session_id', 'default_session')
            
            print(f"🤖 Pregunta: {request.question}")
            print(f"📝 Session ID: {session_id}")
            
            # 1. Guardar pregunta del usuario
            self.conversation_manager.add_message(session_id, 'user', request.question)
            
            # 2. Obtener archivos disponibles
            available_files = await self.context_builder.get_available_files()
            print(f"📁 Archivos encontrados: {len(available_files)}")
            
            # 3. Analizar la pregunta
            query_analysis = self.query_analyzer.analyze(request.question, available_files)
            print(f"📊 Análisis: {query_analysis}")
            
            # 4. Determinar archivo objetivo (considerando contexto conversacional)
            target_file = self.conversation_manager.extract_file_context(
                session_id, 
                request.question, 
                available_files
            ) or query_analysis.get('target_file') or request.file_context
            
            print(f"🎯 Archivo objetivo: {target_file}")
            
            # 5. Construir contexto del archivo
            context = await self.context_builder.build_context(target_file)
            
            # 6. Construir contexto conversacional
            conversation_context = self.conversation_manager.build_conversation_context(session_id)
            
            # 7. Construir prompt con contexto conversacional
            prompt = self.prompt_builder.build(
                context, 
                request.question, 
                query_analysis,
                conversation_context
            )
            
            # 8. Generar respuesta
            ai_response = await self._generate_response(prompt)
            
            # 9. Procesar respuesta
            final_response = self.response_processor.process(
                ai_response,
                query_analysis,
                target_file
            )
            
            # 10. Guardar respuesta del asistente
            self.conversation_manager.add_message(
                session_id, 
                'assistant', 
                final_response,
                {'query_type': query_analysis['type'], 'target_file': target_file}
            )
            
            return {
                "success": True,
                "response": final_response,
                "query_type": query_analysis['type'],
                "target_file": target_file,
                "session_id": session_id
            }
            
        except Exception as e:
            import traceback
            print(f"❌ Error: {e}")
            print(traceback.format_exc())
            return {
                "success": False,
                "response": f"Lo siento, ocurrió un error: {str(e)}",
                "error": str(e)
            }
    
    async def _generate_response(self, prompt: str) -> str:
        """Genera respuesta con Gemini"""
        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: model.generate_content(prompt)
            )
            return response.text
        except Exception as e:
            return f"Error generando respuesta: {str(e)}"


# Instancia global
ai_controller = AIController()
