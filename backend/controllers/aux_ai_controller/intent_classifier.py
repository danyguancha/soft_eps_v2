# controllers/aux_ai_controller/intent_classifier.py
import spacy
from spacy.matcher import Matcher
from typing import Dict, Tuple
import numpy as np
import re


class IntentClassifier:
    """Clasificador mejorado con detección robusta de estadísticas"""
    
    def __init__(self):
        try:
            self.nlp = spacy.load("es_core_news_lg")
            print("Modelo spaCy large cargado")
        except OSError:
            import subprocess
            subprocess.run(["python", "-m", "spacy", "download", "es_core_news_lg"])
            self.nlp = spacy.load("es_core_news_lg")
        
        self.intent_examples = {
            'greeting': [
                "hola", "buenos días", "buenas tardes", "hey", "qué tal"
            ],
            'structure_analysis': [
                "qué columnas tiene", "muestra las columnas", 
                "cuáles son los campos", "estructura del archivo",
                "columnas disponibles", "listar columnas"
            ],
            'statistical': [
                # MÁS VARIACIONES ESPECÍFICAS
                "realiza estadísticas", "genera estadísticas", "generame estadísticas",
                "calcula estadísticas", "dame estadísticas", "estadísticas del archivo",
                "hacer estadísticas", "obtener estadísticas", "mostrar estadísticas",
                "análisis estadístico", "resumen estadístico", "estadísticas descriptivas",
                "cuál es el promedio", "calcula la media", "dame la mediana",
                "suma total", "contar registros", "máximo y mínimo",
                "desviación estándar", "frecuencias", "distribución",
                "métricas", "análisis numérico", "valores estadísticos"
            ],
            'filtering': [
                "filtrar por", "buscar donde", "encontrar registros"
            ],
            'temporal': [
                "análisis temporal", "tendencia", "serie de tiempo"
            ],
            'help': [
                "cómo puedo", "ayúdame", "explica cómo"
            ],
            'export': [
                "exportar datos", "descargar archivo"
            ],
            'comparison': [
                "comparar", "diferencia entre"
            ]
        }
        
        self.intent_vectors = self._create_intent_vectors()
        self._setup_patterns()
    
    def _create_intent_vectors(self) -> Dict[str, np.ndarray]:
        """Crea vectores para cada intención"""
        intent_vectors = {}
        
        for intent, examples in self.intent_examples.items():
            vectors = []
            for example in examples:
                doc = self.nlp(example)
                if doc.has_vector:
                    vectors.append(doc.vector)
            
            if vectors:
                intent_vectors[intent] = np.mean(vectors, axis=0)
        
        return intent_vectors
    
    def _setup_patterns(self):
        """Configura patrones mejorados"""
        self.matcher = Matcher(self.nlp.vocab)
        
        # PATRÓN 1: [VERBO] + estadísticas
        pattern_stats_verb = [
            {"LEMMA": {"IN": [
                "realizar", "generar", "calcular", "obtener", 
                "hacer", "dame", "mostrar", "crear", "producir"
            ]}},
            {"OP": "*", "IS_PUNCT": False},  # Palabras intermedias
            {"LOWER": {"IN": ["estadísticas", "estadística", "estadistico", "estadísticos"]}}
        ]
        self.matcher.add("STATS_VERB", [pattern_stats_verb])
        
        # PATRÓN 2: estadísticas + de/del + [archivo/datos]
        pattern_stats_of = [
            {"LOWER": {"IN": ["estadísticas", "estadística"]}},
            {"LOWER": {"IN": ["de", "del", "para"]}},
            {"OP": "*"}
        ]
        self.matcher.add("STATS_OF", [pattern_stats_of])
        
        # PATRÓN 3: Solo "estadísticas" en consulta corta
        pattern_stats_simple = [
            {"LOWER": {"IN": ["estadísticas", "estadística", "estadisticos"]}}
        ]
        self.matcher.add("STATS_SIMPLE", [pattern_stats_simple])
        
        # Patrón para estructura (solo si menciona "columnas" o "campos")
        pattern_structure = [
            {"LEMMA": {"IN": ["mostrar", "listar", "ver", "enseñar"]}},
            {"OP": "*"},
            {"LOWER": {"IN": ["columnas", "campos", "estructura"]}}
        ]
        self.matcher.add("STRUCTURE_PATTERN", [pattern_structure])
    
    def classify(self, text: str) -> Tuple[str, float]:
        """Clasifica con prioridad en patrones"""
        doc = self.nlp(text.lower())
        text_lower = text.lower()
        
        # PASO 1: DETECCIÓN DIRECTA CON REGEX (MÁS RÁPIDO Y PRECISO)
        stats_patterns = [
            r'\b(genera|realiza|calcula|dame|obtener|hacer|muestra|crea)\s+(?:las?\s+)?estad[ií]sticas?\b',
            r'\bestad[ií]sticas?\s+(?:de|del|para)\b',
            r'\bestad[ií]sticas?\s+(?:descriptivas?|generales?)\b',
            r'\bestad[ií]sticas?\b.*\barchivo\b',
            r'\barchivo\b.*\bestad[ií]sticas?\b'
        ]
        
        for pattern in stats_patterns:
            if re.search(pattern, text_lower, re.IGNORECASE):
                print("ESTADÍSTICAS detectadas por regex")
                return ('statistical', 0.98)
        
        # PASO 2: VERIFICAR PATRONES SINTÁCTICOS
        matches = self.matcher(doc)
        if matches:
            match_names = [self.nlp.vocab.strings[match_id] for match_id, start, end in matches]
            
            # Prioridad máxima: patrones de estadísticas
            if any(name in match_names for name in ["STATS_VERB", "STATS_OF", "STATS_SIMPLE"]):
                print("ESTADÍSTICAS detectadas por patrón sintáctico")
                return ('statistical', 0.97)
            
            # Solo estructura si menciona columnas Y NO estadísticas
            if "STRUCTURE_PATTERN" in match_names and "estad" not in text_lower:
                print("ESTRUCTURA detectada")
                return ('structure_analysis', 0.95)
        
        # PASO 3: PALABRAS CLAVE DIRECTAS
        stats_keywords = [
            'estadísticas', 'estadística', 'estadistico', 'estadísticos',
            'promedio', 'media', 'mediana', 'moda',
            'suma', 'total', 'conteo',
            'máximo', 'mínimo', 'rango',
            'desviación', 'varianza', 'frecuencia',
            'distribución', 'percentil'
        ]
        
        # Contar cuántas palabras clave aparecen
        stats_count = sum(1 for keyword in stats_keywords if keyword in text_lower)
        
        if stats_count >= 1:
            # Verificar que NO sea solo consulta de estructura
            structure_keywords = ['columnas', 'campos', 'estructura']
            structure_count = sum(1 for keyword in structure_keywords if keyword in text_lower)
            
            if structure_count == 0 or stats_count > structure_count:
                print(f"ESTADÍSTICAS detectadas ({stats_count} palabras clave)")
                return ('statistical', 0.90)
        
        # PASO 4: Similaridad vectorial
        if not doc.has_vector:
            return ('general', 0.5)
        
        question_vector = doc.vector
        similarities = {}
        
        for intent, intent_vector in self.intent_vectors.items():
            similarity = self._cosine_similarity(question_vector, intent_vector)
            similarities[intent] = similarity
        
        best_intent = max(similarities, key=similarities.get)
        confidence = similarities[best_intent]
        
        # PASO 5: Ajustes contextuales
        best_intent, confidence = self._contextual_adjustments(doc, best_intent, confidence, text_lower)
        
        if confidence < 0.6:
            return ('general', confidence)
        
        print(f"Intención: {best_intent} (confianza: {confidence:.2f})")
        return (best_intent, confidence)
    
    def _cosine_similarity(self, vec1: np.ndarray, vec2: np.ndarray) -> float:
        """Similaridad coseno"""
        dot_product = np.dot(vec1, vec2)
        norm1 = np.linalg.norm(vec1)
        norm2 = np.linalg.norm(vec2)
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        return dot_product / (norm1 * norm2)
    
    def _contextual_adjustments(self, doc, intent: str, confidence: float, text_lower: str) -> Tuple[str, float]:
        """Ajustes finales"""
        
        # Saludo corto
        if len(doc) <= 3:
            greeting_words = ['hola', 'hey', 'buenas', 'buenos']
            if any(token.text in greeting_words for token in doc):
                return ('greeting', 0.95)
        
        # FORZAR ESTADÍSTICAS SI MENCIONA PALABRAS CLAVE
        stats_words = [
            'estadísticas', 'estadística', 'promedio', 'media', 'mediana',
            'suma', 'total', 'máximo', 'mínimo', 'desviación', 'frecuencia'
        ]
        
        if any(word in text_lower for word in stats_words):
            # Solo si NO es claramente estructura
            if 'columnas' not in text_lower or 'estadísticas' in text_lower:
                print("Ajuste: Forzando STATISTICAL")
                return ('statistical', 0.95)
        
        # Solo estructura si menciona columnas sin estadísticas
        structure_words = ['columnas', 'campos', 'estructura']
        if any(word in text_lower for word in structure_words):
            if not any(word in text_lower for word in stats_words):
                return ('structure_analysis', min(confidence + 0.1, 0.95))
        
        return (intent, confidence)
    
    def get_intent_details(self, text: str) -> Dict:
        """Análisis completo"""
        intent, confidence = self.classify(text)
        doc = self.nlp(text)
        
        return {
            'intent': intent,
            'confidence': confidence,
            'entities': [{'text': ent.text, 'label': ent.label_} for ent in doc.ents],
            'key_tokens': [token.text for token in doc if token.pos_ in ['NOUN', 'VERB'] and not token.is_stop][:5],
            'has_negation': any(token.dep_ == 'neg' for token in doc),
            'is_question': doc[0].text.lower() in ['qué', 'cuál', 'cómo', 'dónde', 'cuándo', 'quién']
        }


# Instancia global
intent_classifier = IntentClassifier()
