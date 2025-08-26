import google.generativeai as genai
from config import GEMINI_API_KEY
from controllers.files_controllers.storage_manager import FileStorageManager

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-1.5-flash')

storage = FileStorageManager().storage
data_cache = FileStorageManager().data_cache

def ask_ai(request):
    """Procesa consulta al asistente IA con contexto de archivos"""
    try:
        # Construir contexto basado en archivos cargados
        context = build_context(request.file_context)
        
        prompt = f"""
        Contexto de datos disponibles:
        {context}
        
        Pregunta del usuario: {request.question}
        
        Por favor, proporciona una respuesta útil basada en los datos disponibles.
        Si la pregunta requiere análisis específico de los datos, sugiere operaciones
        o filtros que podrían ser útiles.
        """
        
        response = model.generate_content(prompt)
        return {"response": response.text}
    
    except Exception as e:
        return {"response": f"Error al procesar la consulta: {str(e)}"}

def build_context(file_id=None):
    """Construye contexto basado en archivos disponibles"""
    if not storage:
        return "No hay archivos cargados actualmente."
    
    context_parts = []
    
    if file_id and file_id in storage:
        # Contexto específico del archivo
        file_info = storage[file_id]
        cache_key = f"{file_id}_{file_info.get('default_sheet')}"
        
        context_parts.append(f"Archivo: {file_info['original_name']}")
        context_parts.append(f"Columnas: {', '.join(file_info['columns'])}")
        context_parts.append(f"Total de filas: {file_info['total_rows']}")
        
        if cache_key in data_cache:
            df = data_cache[cache_key]
            # Agregar estadísticas básicas
            context_parts.append(f"Muestra de datos:\n{df.head(3).to_string()}")
            
            # Estadísticas de columnas numéricas
            numeric_cols = df.select_dtypes(include=['number']).columns
            if len(numeric_cols) > 0:
                context_parts.append(f"Estadísticas básicas:\n{df[numeric_cols].describe()}")
    
    else:
        # Contexto general de todos los archivos
        context_parts.append(f"Total de archivos cargados: {len(storage)}")
        for file_id, info in storage.items():
            context_parts.append(f"- {info['original_name']}: {len(info['columns'])} columnas, {info['total_rows']} filas")
    
    return "\n".join(context_parts)
