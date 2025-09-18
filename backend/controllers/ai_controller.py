# controllers/ai_controller.py - CORRECCIÓN PARA LEER METADATA_CACHE
import google.generativeai as genai
import asyncio
import os
import json
import glob
from typing import Dict, Any, Optional, List
from config import GEMINI_API_KEY
from controllers.files_controllers.storage_manager import FileStorageManager
from services.duckdb_service.duckdb_service import duckdb_service

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-1.5-flash')

class AIController:
    def __init__(self):
        self.storage_manager = FileStorageManager()
        self.max_sample_rows = 5
        self.max_context_length = 8000
        
        # ✅ RUTAS A LAS CARPETAS DE METADATOS
        self.metadata_cache_path = "metadata_cache"
        self.parquet_cache_path = "parquet_cache"
    
    async def ask_ai(self, request) -> Dict[str, Any]:
        """Procesa consulta al asistente IA - VERSIÓN CORREGIDA"""
        try:
            print(f"🤖 Procesando consulta AI: {request.question[:100]}...")
            
            # ✅ CONSTRUIR CONTEXTO DESDE METADATA_CACHE
            context = await self._build_comprehensive_context_async(request.file_context)
            
            # Determinar tipo de consulta
            query_type = self._analyze_query_type(request.question)
            
            # Construir prompt
            prompt = self._build_smart_prompt(context, request.question, query_type)
            
            print(f"📝 Prompt generado ({len(prompt)} chars)")
            
            # Generar respuesta
            response = await self._generate_ai_response_async(prompt)
            
            # Procesar respuesta
            processed_response = self._process_ai_response(
                response, 
                request.file_context, 
                query_type
            )
            
            return {
                "success": True,
                "response": processed_response,
                "context_type": "file_specific" if request.file_context else "general",
                "query_type": query_type,
                "file_context": request.file_context
            }
            
        except Exception as e:
            print(f"❌ Error en AI Controller: {e}")
            return {
                "success": False,
                "response": f"Lo siento, ocurrió un error al procesar tu consulta: {str(e)}",
                "error": str(e)
            }
    
    async def _build_comprehensive_context_async(self, file_id: Optional[str] = None) -> str:
        """Construye contexto desde metadata_cache"""
        try:
            context_parts = []
            
            # ✅ LEER METADATOS DESDE METADATA_CACHE
            available_files = await self._get_available_files_from_metadata()
            
            if not available_files:
                return """
No hay archivos cargados actualmente. 

📁 **Para empezar:**
1. Ve a la sección "Subir Archivos" (/technical-note/upload)
2. Selecciona un archivo CSV o Excel
3. Una vez cargado, podrás hacer consultas específicas sobre tus datos

💡 **Tipos de análisis disponibles:**
• Análisis estadístico básico
• Filtrado y búsqueda de datos
• Análisis temporal
• Reportes de nota técnica (inasistentes por edad)
• Cruce de archivos (VLOOKUP)
"""
            
            if file_id:
                # ✅ CONTEXTO ESPECÍFICO DEL ARCHIVO
                context_parts.append("=== ARCHIVO SELECCIONADO ===")
                file_context = await self._build_file_specific_context_from_metadata(file_id, available_files)
                context_parts.append(file_context)
            else:
                # ✅ CONTEXTO GENERAL DE TODOS LOS ARCHIVOS
                context_parts.append("=== ARCHIVOS DISPONIBLES ===")
                general_context = self._build_general_context_from_metadata(available_files)
                context_parts.append(general_context)
            
            # Capacidades del sistema
            context_parts.append("\n=== CAPACIDADES DEL SISTEMA ===")
            context_parts.append(self._get_system_capabilities())
            
            full_context = "\n".join(context_parts)
            
            # Limitar longitud
            if len(full_context) > self.max_context_length:
                full_context = full_context[:self.max_context_length] + "...\n[Contexto truncado]"
            
            return full_context
            
        except Exception as e:
            print(f"❌ Error construyendo contexto: {e}")
            return "Contexto no disponible debido a un error interno."
    
    async def _get_available_files_from_metadata(self) -> List[Dict[str, Any]]:
        """Lee todos los archivos de metadatos disponibles"""
        try:
            available_files = []
            
            if not os.path.exists(self.metadata_cache_path):
                print(f"⚠️ Carpeta {self.metadata_cache_path} no encontrada")
                return []
            
            # ✅ BUSCAR TODOS LOS JSON EN METADATA_CACHE
            metadata_files = glob.glob(os.path.join(self.metadata_cache_path, "*.json"))
            
            for metadata_file in metadata_files:
                try:
                    with open(metadata_file, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)
                    
                    # Agregar información del archivo
                    file_info = {
                        'file_id': metadata.get('file_id', os.path.basename(metadata_file).replace('.json', '')),
                        'original_name': metadata.get('original_name', 'Desconocido'),
                        'extension': metadata.get('extension', 'csv'),
                        'columns': metadata.get('columns', []),
                        'total_rows': metadata.get('total_rows', 0),
                        'file_size_mb': metadata.get('original_size_mb', 0),
                        'cached_at': metadata.get('cached_at', ''),
                        'parquet_path': metadata.get('parquet_path', ''),
                        'compression_ratio': metadata.get('compression_ratio', 0)
                    }
                    
                    available_files.append(file_info)
                    
                except Exception as file_error:
                    print(f"⚠️ Error leyendo {metadata_file}: {file_error}")
                    continue
            
            print(f"📁 Encontrados {len(available_files)} archivos en metadata_cache")
            return available_files
            
        except Exception as e:
            print(f"❌ Error obteniendo archivos: {e}")
            return []
    
    async def _build_file_specific_context_from_metadata(self, file_id: str, available_files: List[Dict]) -> str:
        """Construye contexto específico usando metadatos"""
        try:
            # Buscar archivo específico
            file_info = None
            for f in available_files:
                if f['file_id'] == file_id or f['original_name'] == file_id:
                    file_info = f
                    break
            
            if not file_info:
                return f"❌ Archivo {file_id} no encontrado en los metadatos."
            
            context_parts = []
            
            # ✅ INFORMACIÓN DETALLADA DEL ARCHIVO
            context_parts.append(f"📄 **Archivo:** {file_info['original_name']}")
            context_parts.append(f"📊 **Tipo:** {file_info['extension'].upper()}")
            context_parts.append(f"📈 **Filas:** {file_info['total_rows']:,} registros")
            context_parts.append(f"📋 **Columnas:** {len(file_info['columns'])} campos")
            
            if file_info['file_size_mb'] > 0:
                context_parts.append(f"💾 **Tamaño:** {file_info['file_size_mb']:.2f} MB")
            
            if file_info['compression_ratio'] > 0:
                context_parts.append(f"🗜️ **Compresión:** {file_info['compression_ratio']:.1f}%")
            
            # ✅ ESTRUCTURA DE COLUMNAS
            columns = file_info['columns']
            if columns:
                context_parts.append(f"\n**📋 ESTRUCTURA DE DATOS:**")
                for i, col in enumerate(columns[:25], 1):  # Máximo 25 columnas
                    context_parts.append(f"  {i:2}. {col}")
                
                if len(columns) > 25:
                    context_parts.append(f"  ... y {len(columns)-25} columnas más")
            
            # ✅ ESTADÍSTICAS ADICIONALES SI ESTÁN DISPONIBLES
            if file_info['cached_at']:
                try:
                    from datetime import datetime
                    cached_time = datetime.fromisoformat(file_info['cached_at'].replace('Z', '+00:00'))
                    context_parts.append(f"\n⏰ **Procesado:** {cached_time.strftime('%Y-%m-%d %H:%M')}")
                except:
                    pass
            
            # ✅ INTENTAR OBTENER MUESTRA DE DATOS
            try:
                sample_data = await self._get_sample_data_async(file_info['file_id'])
                if sample_data:
                    context_parts.append(f"\n**🔍 MUESTRA DE DATOS:**")
                    context_parts.append(sample_data)
            except Exception as sample_error:
                print(f"⚠️ Error obteniendo muestra: {sample_error}")
            
            return "\n".join(context_parts)
            
        except Exception as e:
            print(f"❌ Error en contexto específico: {e}")
            return f"Error obteniendo información del archivo {file_id}"
    
    def _build_general_context_from_metadata(self, available_files: List[Dict]) -> str:
        """Construye contexto general desde metadatos"""
        try:
            if not available_files:
                return "No hay archivos cargados actualmente."
            
            context_parts = []
            context_parts.append(f"📁 **Total de archivos:** {len(available_files)}")
            
            # ✅ ESTADÍSTICAS GENERALES
            total_rows = sum(f['total_rows'] for f in available_files)
            total_size = sum(f['file_size_mb'] for f in available_files)
            
            context_parts.append(f"📊 **Registros totales:** {total_rows:,}")
            if total_size > 0:
                context_parts.append(f"💾 **Tamaño total:** {total_size:.1f} MB")
            
            # ✅ LISTA DE ARCHIVOS DISPONIBLES
            context_parts.append(f"\n**📋 ARCHIVOS DISPONIBLES:**")
            for i, file_info in enumerate(available_files, 1):
                file_summary = (
                    f"{i:2}. **{file_info['original_name']}** "
                    f"({file_info['extension'].upper()}) - "
                    f"{len(file_info['columns'])} columnas, "
                    f"{file_info['total_rows']:,} filas"
                )
                context_parts.append(f"   {file_summary}")
            
            return "\n".join(context_parts)
            
        except Exception as e:
            print(f"❌ Error en contexto general: {e}")
            return "Error obteniendo información general de archivos."
    
    # ✅ MANTENER OTROS MÉTODOS EXISTENTES
    async def _generate_ai_response_async(self, prompt: str) -> str:
        """Genera respuesta AI de forma async"""
        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: model.generate_content(prompt)
            )
            return response.text
        except Exception as e:
            return f"Error generando respuesta: {str(e)}"
    
    async def _get_sample_data_async(self, file_id: str) -> str:
        """Obtiene muestra de datos usando DuckDB"""
        try:
            loop = asyncio.get_event_loop()
            
            def get_sample():
                sample_query = f"SELECT * FROM '{file_id}' LIMIT {self.max_sample_rows}"
                return duckdb_service.conn.execute(sample_query).fetchdf()
            
            result = await loop.run_in_executor(None, get_sample)
            
            if not result.empty:
                markdown_table = result.to_markdown(index=False, tablefmt="grid")
                return f"``````"
            
            return "No se pudo obtener muestra de datos"
            
        except Exception as e:
            print(f"⚠️ Error obteniendo muestra: {e}")
            return "Muestra no disponible"
    
    def _analyze_query_type(self, question: str) -> str:
        """Analiza tipo de consulta"""
        question_lower = question.lower()
        
        if any(word in question_lower for word in ['hola', 'hello', 'hi', 'buenos días', 'buenas tardes']):
            return 'greeting'
        elif any(word in question_lower for word in ['estadística', 'promedio', 'suma', 'contar']):
            return 'statistical'
        elif any(word in question_lower for word in ['filtrar', 'buscar', 'encontrar']):
            return 'filtering'
        elif any(word in question_lower for word in ['columna', 'campo', 'estructura']):
            return 'schema'
        elif any(word in question_lower for word in ['temporal', 'tiempo', 'fecha']):
            return 'temporal'
        elif any(word in question_lower for word in ['nota técnica', 'inasistentes']):
            return 'technical_note'
        elif any(word in question_lower for word in ['ayuda', 'cómo', 'tutorial']):
            return 'help'
        else:
            return 'general'
    
    def _build_smart_prompt(self, context: str, question: str, query_type: str) -> str:
        """Construye prompt inteligente"""
        
        greeting_context = ""
        if query_type == 'greeting':
            greeting_context = """
INSTRUCCIONES PARA SALUDO:
- Responde de manera muy Breve
- Responde de manera amigable y profesional
- Menciona brevemente los archivos disponibles si los hay
- Invita al usuario a hacer preguntas específicas sobre los datos
- Sugiere tipos de análisis que puede realizar
"""
        
        base_prompt = f"""
Eres un asistente especializado en análisis de datos que ayuda a usuarios a entender y analizar sus archivos de datos.

CONTEXTO DE DATOS DISPONIBLES:
{context}

{greeting_context}

PREGUNTA DEL USUARIO: {question}

RESPUESTA:
Proporciona una respuesta útil, específica y accionable. Si sugieres análisis, explica cómo usar las herramientas disponibles.
"""
        
        return base_prompt
    
    def _process_ai_response(self, ai_response: str, file_context: Optional[str], query_type: str) -> str:
        """Procesa respuesta AI"""
        processed_response = ai_response
        
        if query_type == 'greeting':
            processed_response += "\n\n💡 **¿Qué puedo hacer por ti?**\n• Analizar estructura de datos\n• Generar estadísticas\n• Ayudar con filtros y búsquedas\n• Explicar cómo usar las herramientas"
        elif query_type == 'statistical' and file_context:
            processed_response += "\n\n💡 **Sugerencia:** Usa la sección 'Análisis' para estadísticas detalladas."
        elif query_type == 'technical_note':
            processed_response += "\n\n💡 **Sugerencia:** Usa 'Nota Técnica' para reportes de inasistentes."
        elif not file_context:
            processed_response += "\n\n📁 **Tip:** Selecciona un archivo específico para análisis más detallados."
        
        return processed_response
    
    def _get_system_capabilities(self) -> str:
        """Describe capacidades del sistema"""
        return """
🔧 **Análisis disponibles:**
• Filtrado y búsqueda de datos
• Estadísticas y agregaciones  
• Análisis temporal
• Análisis de nota técnica (inasistentes)
• Cruce de archivos (VLOOKUP)
• Exportación de resultados

💡 **Puedes preguntar sobre:**
• Estructura y contenido de datos
• Patrones y tendencias
• Estadísticas específicas
• Cómo usar las herramientas disponibles
"""

# ✅ INSTANCIA GLOBAL
ai_controller = AIController()
