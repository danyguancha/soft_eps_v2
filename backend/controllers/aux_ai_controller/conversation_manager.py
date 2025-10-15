# controllers/aux_ai_controller/conversation_manager.py
from typing import List, Dict, Any, Optional
from datetime import datetime


class ConversationManager:
    """Gestiona el historial y contexto de la conversación"""
    
    def __init__(self, max_history: int = 10):
        self.conversations: Dict[str, List[Dict]] = {}
        self.max_history = max_history
        self.active_files: Dict[str, str] = {}  # session_id -> file_id
    
    def add_message(self, session_id: str, role: str, content: str, metadata: Dict = None):
        """Agrega un mensaje al historial"""
        if session_id not in self.conversations:
            self.conversations[session_id] = []
        
        message = {
            'role': role,
            'content': content,
            'timestamp': datetime.now().isoformat(),
            'metadata': metadata or {}
        }
        
        self.conversations[session_id].append(message)
        
        # Limitar historial
        if len(self.conversations[session_id]) > self.max_history * 2:
            self.conversations[session_id] = self.conversations[session_id][-self.max_history * 2:]
    
    def get_conversation_history(self, session_id: str) -> List[Dict]:
        """Obtiene el historial de conversación"""
        return self.conversations.get(session_id, [])
    
    def get_active_file(self, session_id: str) -> Optional[str]:
        """Obtiene el archivo activo en la conversación"""
        return self.active_files.get(session_id)
    
    def set_active_file(self, session_id: str, file_id: str):
        """Establece el archivo activo"""
        self.active_files[session_id] = file_id
        print(f"📌 Archivo activo establecido: {file_id}")
    
    def clear_active_file(self, session_id: str):
        """Limpia el archivo activo"""
        if session_id in self.active_files:
            del self.active_files[session_id]
    
    def build_conversation_context(self, session_id: str) -> str:
        """Construye contexto de la conversación"""
        history = self.get_conversation_history(session_id)
        
        if not history:
            return ""
        
        # Solo las últimas 5 interacciones
        recent_history = history[-10:]
        
        context_parts = ["\n=== CONTEXTO DE CONVERSACIÓN ANTERIOR ==="]
        
        for msg in recent_history:
            role = "Usuario" if msg['role'] == 'user' else "Asistente"
            content = msg['content'][:200]  # Limitar longitud
            context_parts.append(f"{role}: {content}")
        
        active_file = self.get_active_file(session_id)
        if active_file:
            context_parts.append(f"\n📌 **ARCHIVO ACTIVO EN CONVERSACIÓN:** {active_file}")
            context_parts.append("⚠️ **IMPORTANTE:** Usa este archivo para todas las consultas siguientes a menos que el usuario especifique otro.")
        
        return "\n".join(context_parts)
    
    def extract_file_context(self, session_id: str, question: str, available_files: List[Dict]) -> Optional[str]:
        """Extrae el archivo relevante considerando el contexto"""
        question_lower = question.lower()
        
        # Palabras que indican cambio de archivo
        change_keywords = ['cambia', 'otro archivo', 'ahora analiza', 'usa este', 'este archivo']
        is_changing_file = any(keyword in question_lower for keyword in change_keywords)
        
        # Buscar mención explícita de archivo en la pregunta
        mentioned_file = None
        for file_info in available_files:
            file_name = file_info['original_name'].lower()
            name_without_ext = file_name.rsplit('.', 1)[0]
            
            # Buscar nombre completo o partes significativas
            if file_name in question_lower or name_without_ext in question_lower:
                mentioned_file = file_info['file_id']
                break
            
            # Buscar palabras clave del nombre (mínimo 5 caracteres)
            name_parts = name_without_ext.split()
            for part in name_parts:
                if len(part) >= 5 and part in question_lower:
                    mentioned_file = file_info['file_id']
                    break
        
        # Si menciona archivo explícitamente, usarlo
        if mentioned_file:
            self.set_active_file(session_id, mentioned_file)
            return mentioned_file
        
        # Si no menciona archivo pero NO está cambiando, usar el activo
        if not is_changing_file:
            active = self.get_active_file(session_id)
            if active:
                print(f"💡 Usando archivo activo del contexto: {active}")
                return active
        
        return None
    
    def clear_conversation(self, session_id: str):
        """Limpia la conversación"""
        if session_id in self.conversations:
            del self.conversations[session_id]
        self.clear_active_file(session_id)


# Instancia global
conversation_manager = ConversationManager()
