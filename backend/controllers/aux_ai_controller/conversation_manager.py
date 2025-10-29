# controllers/aux_ai_controller/conversation_manager.py
from typing import List, Dict, Any, Optional
from datetime import datetime
from controllers.aux_ai_controller.intent_classifier import intent_classifier


class ConversationManager:
    """Gestiona conversaciones con contexto NLP automático"""
    
    def __init__(self, max_history: int = 10):
        self.conversations: Dict[str, List[Dict]] = {}
        self.max_history = max_history
        self.active_files: Dict[str, str] = {}
        self.intent_history: Dict[str, List[str]] = {}
    
    def add_message(
        self, 
        session_id: str, 
        role: str, 
        content: str, 
        metadata: Dict = None
    ):
        """Agrega mensaje con análisis NLP automático"""
        if session_id not in self.conversations:
            self.conversations[session_id] = []
            self.intent_history[session_id] = []
        
        # Detectar intención automáticamente
        if role == 'user':
            intent, confidence = intent_classifier.classify(content)
            self.intent_history[session_id].append(intent)
        else:
            intent, confidence = None, None
        
        message = {
            'role': role,
            'content': content,
            'timestamp': datetime.now().isoformat(),
            'metadata': metadata or {},
            'intent': intent,
            'confidence': confidence
        }
        
        self.conversations[session_id].append(message)
        
        # Limitar historial
        if len(self.conversations[session_id]) > self.max_history * 2:
            self.conversations[session_id] = self.conversations[session_id][-self.max_history * 2:]
            self.intent_history[session_id] = self.intent_history[session_id][-self.max_history:]
    
    def get_conversation_history(self, session_id: str) -> List[Dict]:
        """Obtiene historial con intenciones"""
        return self.conversations.get(session_id, [])
    
    def get_intent_pattern(self, session_id: str) -> List[str]:
        """Obtiene patrón de intenciones del usuario"""
        return self.intent_history.get(session_id, [])[-5:]  # Últimas 5
    
    def get_active_file(self, session_id: str) -> Optional[str]:
        """Obtiene archivo activo"""
        return self.active_files.get(session_id)
    
    def set_active_file(self, session_id: str, file_id: str):
        """Establece archivo activo"""
        self.active_files[session_id] = file_id
        print(f"📌 Archivo activo: {file_id}")
    
    def build_conversation_context(self, session_id: str) -> str:
        """Construye contexto enriquecido con intenciones"""
        history = self.get_conversation_history(session_id)
        
        if not history:
            return ""
        
        recent_history = history[-10:]
        intent_pattern = self.get_intent_pattern(session_id)
        
        context_parts = ["\n=== CONTEXTO DE CONVERSACIÓN ==="]
        
        # Patrón de intenciones
        if intent_pattern:
            context_parts.append(f"**Patrón de consultas:** {' → '.join(intent_pattern)}")
        
        # Historial reciente
        for msg in recent_history[-5:]:
            role = "Usuario" if msg['role'] == 'user' else "Asistente"
            content = msg['content'][:150]
            intent_info = f" [{msg.get('intent')}]" if msg.get('intent') else ""
            context_parts.append(f"{role}{intent_info}: {content}")
        
        active_file = self.get_active_file(session_id)
        if active_file:
            context_parts.append(f"\n📌 **ARCHIVO ACTIVO:** {active_file}")
        
        return "\n".join(context_parts)
    
    def extract_file_context_nlp(
        self, 
        session_id: str, 
        question: str, 
        available_files: List[Dict],
        query_analysis: Dict[str, Any]
    ) -> Optional[str]:
        """Extrae archivo usando análisis NLP"""
        
        # Si el análisis NLP ya identificó un archivo
        if query_analysis.get('target_file'):
            self.set_active_file(session_id, query_analysis['target_file'])
            return query_analysis['target_file']
        
        # Usar archivo activo si la consulta lo requiere
        if query_analysis.get('requires_file'):
            active = self.get_active_file(session_id)
            if active:
                print(f"💡 Usando archivo activo: {active}")
                return active
        
        return None
    
    def clear_conversation(self, session_id: str):
        """Limpia conversación"""
        if session_id in self.conversations:
            del self.conversations[session_id]
        if session_id in self.intent_history:
            del self.intent_history[session_id]
        self.clear_active_file(session_id)
    
    def clear_active_file(self, session_id: str):
        """Limpia archivo activo"""
        if session_id in self.active_files:
            del self.active_files[session_id]


# Instancia global
conversation_manager = ConversationManager()
