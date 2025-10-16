# controllers/ai_controller.py - Agregar ejecución de SQL
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
from controllers.aux_ai_controller.sql_executor import SQLExecutor 

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-2.0-flash-exp')


class AIController:
    """Orquestador principal del asistente AI"""
    
    def __init__(self):
        self.context_builder = ContextBuilder()
        self.query_analyzer = QueryAnalyzer()
        self.prompt_builder = PromptBuilder()
        self.response_processor = ResponseProcessor()
        self.data_retriever = DataRetriever()
        self.conversation_manager = ConversationManager()
        self.sql_executor = SQLExecutor() 
    
    async def ask_ai(self, request) -> Dict[str, Any]:
        """Procesa consulta del usuario"""
        try:
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
            
            # 4. Determinar archivo objetivo
            target_file = self.conversation_manager.extract_file_context(
                session_id, 
                request.question, 
                available_files
            ) or query_analysis.get('target_file') or request.file_context
            
            print(f"🎯 Archivo objetivo: {target_file}")
            
            # 5. ✅ SI ES CONSULTA ESTADÍSTICA Y HAY ARCHIVO, CALCULAR VALORES REALES
            calculated_stats = None
            if query_analysis['type'] == 'statistical' and target_file:
                print(f"🔢 Calculando estadísticas para {target_file}...")
                
                # Obtener información del archivo incluyendo la ruta del parquet
                file_info = None
                for f in available_files:
                    if f['file_id'] == target_file or target_file in f['original_name']:
                        file_info = f
                        break
                
                if file_info and file_info.get('columns'):
                    # ✅ Obtener la ruta del parquet
                    parquet_path = file_info.get('parquet_path')
                    
                    if not parquet_path:
                        print(f"⚠️ No se encontró parquet_path para {target_file}")
                    else:
                        print(f"📂 Ruta del parquet: {parquet_path}")
                        
                        calculated_stats = await self.sql_executor.calculate_statistics(
                            file_info['file_id'],
                            file_info['columns'],
                            parquet_path  # ✅ Pasar la ruta del parquet
                        )
                        print(f"✅ Estadísticas calculadas: {len(calculated_stats)} tipos")
            
            # 6. Construir contexto del archivo
            context = await self.context_builder.build_context(target_file)
            
            # 7. ✅ SI HAY ESTADÍSTICAS CALCULADAS, AGREGARLAS AL CONTEXTO
            if calculated_stats:
                context += "\n\n=== ESTADÍSTICAS CALCULADAS ===\n"
                context += self._format_statistics(calculated_stats)
            
            # 8. Construir contexto conversacional
            conversation_context = self.conversation_manager.build_conversation_context(session_id)
            
            # 9. Construir prompt
            prompt = self.prompt_builder.build(
                context, 
                request.question, 
                query_analysis,
                conversation_context
            )
            
            # 10. Generar respuesta
            ai_response = await self._generate_response(prompt)
            
            # 11. Procesar respuesta
            final_response = self.response_processor.process(
                ai_response,
                query_analysis,
                target_file
            )
            
            # 12. Guardar respuesta del asistente
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
                "session_id": session_id,
                "has_calculated_stats": bool(calculated_stats)
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
    
    def _format_statistics(self, stats: Dict[str, Any]) -> str:
        """Formatea estadísticas calculadas para el contexto"""
        formatted = []
        
        # Estadísticas numéricas
        if 'numeric' in stats and stats['numeric']:
            formatted.append("\n**COLUMNAS NUMÉRICAS:**")
            for col, values in stats['numeric'].items():
                formatted.append(f"\n📊 **{col}:**")
                formatted.append(f"  - Promedio: {values['promedio']}")
                formatted.append(f"  - Mediana: {values['mediana']}")
                formatted.append(f"  - Mínimo: {values['minimo']}")
                formatted.append(f"  - Máximo: {values['maximo']}")
                formatted.append(f"  - Desviación Estándar: {values['desviacion_std']}")
                formatted.append(f"  - Conteo: {values['count']}")
        
        # Estadísticas categóricas
        if 'categorical' in stats and stats['categorical']:
            formatted.append("\n\n**COLUMNAS CATEGÓRICAS:**")
            for col, values in stats['categorical'].items():
                formatted.append(f"\n📋 **{col}:**")
                formatted.append(f"  - Valores únicos: {values['valores_unicos']}")
                formatted.append(f"  - Top 5 más frecuentes:")
                for item in values['top_5']:
                    formatted.append(f"    • {item['value']}: {item['frequency']} ({item['percentage']}%)")
        
        return "\n".join(formatted)
    
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
