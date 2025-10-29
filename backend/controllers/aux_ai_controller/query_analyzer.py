# controllers/aux_ai_controller/query_analyzer.py
from typing import Dict, Any, Optional, List
import spacy
from controllers.aux_ai_controller.intent_classifier import intent_classifier


class QueryAnalyzer:
    """Analiza consultas usando NLP automático con spaCy"""
    
    def __init__(self):
        try:
            self.nlp = spacy.load("es_core_news_lg")
            print("✅ QueryAnalyzer inicializado con spaCy")
        except OSError:
            print("⚠️ Instalando modelo...")
            import subprocess
            subprocess.run(["python", "-m", "spacy", "download", "es_core_news_lg"])
            self.nlp = spacy.load("es_core_news_lg")
    
    def analyze(self, question: str, available_files: List[Dict] = None) -> Dict[str, Any]:
        """Análisis completo automatizado de la consulta"""
        
        # Clasificación automática de intención
        intent_details = intent_classifier.get_intent_details(question)
        
        # Análisis lingüístico
        doc = self.nlp(question.lower())
        
        analysis = {
            'type': intent_details['intent'],
            'confidence': intent_details['confidence'],
            'requires_file': self._requires_file_automatic(doc, intent_details),
            'target_file': self._extract_target_file_nlp(question, available_files or [], doc),
            'intent': self._map_intent_to_action(intent_details['intent']),
            'entities': intent_details['entities'],
            'keywords': intent_details['key_tokens'],
            'is_question': intent_details['is_question'],
            'has_negation': intent_details['has_negation'],
            'complexity': self._assess_complexity(doc),
            'specific_columns': self._extract_column_references(doc),
            'numerical_operations': self._detect_numerical_operations(doc)
        }
        
        return analysis
    
    def _requires_file_automatic(self, doc, intent_details: Dict) -> bool:
        """Determina automáticamente si necesita archivo"""
        intent = intent_details['intent']
        
        # Intenciones que siempre necesitan archivo
        file_required_intents = ['structure_analysis', 'statistical', 'filtering', 'temporal', 'export']
        
        if intent in file_required_intents:
            return True
        
        # Detectar si menciona archivo explícitamente
        file_keywords = ['archivo', 'datos', 'tabla', 'csv', 'excel', 'dataset']
        if any(token.text in file_keywords for token in doc):
            return True
        
        # Si hay verbos de análisis
        analysis_verbs = ['analizar', 'mostrar', 'calcular', 'buscar', 'filtrar', 'extraer']
        if any(token.lemma_ in analysis_verbs for token in doc):
            return True
        
        return False
    
    def _extract_target_file_nlp(self, question: str, available_files: List[Dict], doc) -> Optional[str]:
        """Extrae archivo objetivo usando NLP avanzado"""
        if not available_files:
            return None
        
        best_match = None
        best_score = 0.0
        
        for file_info in available_files:
            file_name = file_info['original_name'].lower()
            score = 0.0
            
            # Método 1: Mención directa
            if file_name in question.lower():
                return file_info['file_id']
            
            # Método 2: Similaridad semántica
            file_doc = self.nlp(file_name)
            similarity = doc.similarity(file_doc)
            score += similarity * 0.5
            
            # Método 3: Coincidencia de tokens
            file_tokens = set(file_name.replace('_', ' ').replace('.', ' ').split())
            question_tokens = set(question.lower().split())
            
            common_tokens = file_tokens.intersection(question_tokens)
            if common_tokens:
                score += len(common_tokens) * 0.3
            
            # Método 4: Extensión mencionada
            if file_info['extension'] in question.lower():
                score += 0.2
            
            if score > best_score:
                best_score = score
                best_match = file_info['file_id']
        
        # Solo devolver si confianza es alta
        return best_match if best_score > 0.4 else None
    
    def _map_intent_to_action(self, intent: str) -> str:
        """Mapea intención a acción específica"""
        intent_action_map = {
            'structure_analysis': 'show_columns',
            'statistical': 'calculate_statistics',
            'filtering': 'apply_filter',
            'temporal': 'temporal_analysis',
            'help': 'provide_help',
            'export': 'export_data',
            'comparison': 'compare_data',
            'greeting': 'greet_user',
            'general': 'general_query'
        }
        return intent_action_map.get(intent, 'general_query')
    
    def _assess_complexity(self, doc) -> str:
        """Evalúa complejidad automáticamente"""
        num_tokens = len(doc)
        num_entities = len(doc.ents)
        num_clauses = len([sent for sent in doc.sents])
        
        score = 0
        if num_tokens > 15: score += 1
        if num_entities > 2: score += 1
        if num_clauses > 1: score += 1
        
        if score == 0:
            return 'simple'
        elif score <= 1:
            return 'moderate'
        else:
            return 'complex'
    
    def _extract_column_references(self, doc) -> List[str]:
        """Extrae referencias a columnas del texto"""
        column_candidates = []
        
        # Buscar sustantivos propios o sustantivos después de "columna"
        for i, token in enumerate(doc):
            if token.text in ['columna', 'campo']:
                if i + 1 < len(doc):
                    column_candidates.append(doc[i + 1].text)
            elif token.pos_ == 'PROPN':
                column_candidates.append(token.text)
        
        return column_candidates[:5]  # Máximo 5 columnas
    
    def _detect_numerical_operations(self, doc) -> List[str]:
        """Detecta operaciones numéricas mencionadas"""
        operations = []
        
        operation_keywords = {
            'promedio': 'average',
            'media': 'mean',
            'suma': 'sum',
            'total': 'total',
            'contar': 'count',
            'máximo': 'max',
            'mínimo': 'min',
            'mediana': 'median',
            'desviación': 'stddev',
            'varianza': 'variance'
        }
        
        for token in doc:
            if token.lemma_ in operation_keywords:
                operations.append(operation_keywords[token.lemma_])
        
        return list(set(operations))


# Instancia global
query_analyzer = QueryAnalyzer()
