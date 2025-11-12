# controllers/aux_ai_controller/intent_classifier.py
import spacy
from spacy.matcher import Matcher
from typing import Dict, Tuple
import numpy as np
import re


# Constantes para literales duplicados - Intenciones
INTENT_STATISTICAL = 'statistical'
INTENT_STRUCTURE = 'structure_analysis'
INTENT_GREETING = 'greeting'
INTENT_GENERAL = 'general'

# Constantes para variaciones de "estadística" (elimina duplicación)
LITERAL_ESTADISTICAS = 'estadísticas'
LITERAL_ESTADISTICA = 'estadística'
LITERAL_ESTADISTICO = 'estadistico'
LITERAL_ESTADISTICOS = 'estadísticos'

# Lista de variaciones para uso en patrones
ESTADISTICA_VARIANTS = [
    LITERAL_ESTADISTICAS,
    LITERAL_ESTADISTICA,
    LITERAL_ESTADISTICO,
    LITERAL_ESTADISTICOS
]

# Palabras clave de estadísticas
STATS_KEYWORDS = [
    LITERAL_ESTADISTICAS, LITERAL_ESTADISTICA, LITERAL_ESTADISTICO, LITERAL_ESTADISTICOS,
    'promedio', 'media', 'mediana', 'moda',
    'suma', 'total', 'conteo',
    'máximo', 'mínimo', 'rango',
    'desviación', 'varianza', 'frecuencia',
    'distribución', 'percentil'
]

STRUCTURE_KEYWORDS = ['columnas', 'campos', 'estructura']


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
            INTENT_GREETING: [
                "hola", "buenos días", "buenas tardes", "hey", "qué tal"
            ],
            INTENT_STRUCTURE: [
                "qué columnas tiene", "muestra las columnas", 
                "cuáles son los campos", "estructura del archivo",
                "columnas disponibles", "listar columnas"
            ],
            INTENT_STATISTICAL: [
                f"realiza {LITERAL_ESTADISTICAS}", f"genera {LITERAL_ESTADISTICAS}", 
                f"generame {LITERAL_ESTADISTICAS}",
                f"calcula {LITERAL_ESTADISTICAS}", f"dame {LITERAL_ESTADISTICAS}", 
                f"{LITERAL_ESTADISTICAS} del archivo",
                f"hacer {LITERAL_ESTADISTICAS}", f"obtener {LITERAL_ESTADISTICAS}", 
                f"mostrar {LITERAL_ESTADISTICAS}",
                f"análisis {LITERAL_ESTADISTICO}", f"resumen {LITERAL_ESTADISTICO}", 
                f"{LITERAL_ESTADISTICAS} descriptivas",
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
            {"OP": "*", "IS_PUNCT": False},
            {"LOWER": {"IN": ESTADISTICA_VARIANTS}}  # Usando constante
        ]
        self.matcher.add("STATS_VERB", [pattern_stats_verb])
        
        # PATRÓN 2: estadísticas + de/del + [archivo/datos]
        pattern_stats_of = [
            {"LOWER": {"IN": [LITERAL_ESTADISTICAS, LITERAL_ESTADISTICA]}},  # Usando constantes
            {"LOWER": {"IN": ["de", "del", "para"]}},
            {"OP": "*"}
        ]
        self.matcher.add("STATS_OF", [pattern_stats_of])
        
        # PATRÓN 3: Solo "estadísticas" en consulta corta
        pattern_stats_simple = [
            {"LOWER": {"IN": [LITERAL_ESTADISTICAS, LITERAL_ESTADISTICA, LITERAL_ESTADISTICOS]}}  # Usando constantes
        ]
        self.matcher.add("STATS_SIMPLE", [pattern_stats_simple])
        
        # Patrón para estructura
        pattern_structure = [
            {"LEMMA": {"IN": ["mostrar", "listar", "ver", "enseñar"]}},
            {"OP": "*"},
            {"LOWER": {"IN": STRUCTURE_KEYWORDS}}
        ]
        self.matcher.add("STRUCTURE_PATTERN", [pattern_structure])
    
    def _check_regex_patterns(self, text_lower: str) -> Tuple[bool, float]:
        """Verifica patrones regex para estadísticas"""
        stats_patterns = [
            r'\b(genera|realiza|calcula|dame|obtener|hacer|muestra|crea)\s+(?:las?\s+)?estad[ií]sticas?\b',
            r'\bestad[ií]sticas?\s+(?:de|del|para)\b',
            r'\bestad[ií]sticas?\s+(?:descriptivas?|generales?)\b',
            r'\bestad[ií]sticas?\b.*\barchivo\b',
            r'\barchivo\b.*\bestad[ií]sticas?\b'
        ]
        
        for pattern in stats_patterns:
            if re.search(pattern, text_lower, re.IGNORECASE):
                print(f"{LITERAL_ESTADISTICAS.upper()} detectadas por regex")  # Usando constante
                return (True, 0.98)
        
        return (False, 0.0)
    
    def _check_syntactic_patterns(self, doc, text_lower: str) -> Tuple[str, float]:
        """Verifica patrones sintácticos con spaCy"""
        matches = self.matcher(doc)
        if not matches:
            return (None, 0.0)
        
        match_names = [self.nlp.vocab.strings[match_id] for match_id, start, end in matches]
        
        # Prioridad: patrones de estadísticas
        if any(name in match_names for name in ["STATS_VERB", "STATS_OF", "STATS_SIMPLE"]):
            print(f"{LITERAL_ESTADISTICAS.upper()} detectadas por patrón sintáctico")  # Usando constante
            return (INTENT_STATISTICAL, 0.97)
        
        # Solo estructura si menciona columnas Y NO estadísticas
        if "STRUCTURE_PATTERN" in match_names and "estad" not in text_lower:
            print("🔍 ESTRUCTURA detectada")
            return (INTENT_STRUCTURE, 0.95)
        
        return (None, 0.0)
    
    def _check_keyword_match(self, text_lower: str) -> Tuple[str, float]:
        """Verifica coincidencias de palabras clave"""
        stats_count = sum(1 for keyword in STATS_KEYWORDS if keyword in text_lower)
        
        if stats_count >= 1:
            structure_count = sum(1 for keyword in STRUCTURE_KEYWORDS if keyword in text_lower)
            
            if structure_count == 0 or stats_count > structure_count:
                print(f"{LITERAL_ESTADISTICAS.upper()} detectadas ({stats_count} palabras clave)")  # Usando constante
                return (INTENT_STATISTICAL, 0.90)
        
        return (None, 0.0)
    
    def _calculate_vector_similarity(self, doc) -> Tuple[str, float]:
        """Calcula similaridad vectorial con intenciones"""
        if not doc.has_vector:
            return (INTENT_GENERAL, 0.5)
        
        question_vector = doc.vector
        similarities = {}
        
        for intent, intent_vector in self.intent_vectors.items():
            similarity = self._cosine_similarity(question_vector, intent_vector)
            similarities[intent] = similarity
        
        best_intent = max(similarities, key=similarities.get)
        confidence = similarities[best_intent]
        
        return (best_intent, confidence)
    
    def classify(self, text: str) -> Tuple[str, float]:
        """Clasifica con prioridad en patrones"""
        doc = self.nlp(text.lower())
        text_lower = text.lower()
        
        # PASO 1: Detección directa con regex
        is_stats, confidence = self._check_regex_patterns(text_lower)
        if is_stats:
            return (INTENT_STATISTICAL, confidence)
        
        # PASO 2: Verificar patrones sintácticos
        intent, confidence = self._check_syntactic_patterns(doc, text_lower)
        if intent:
            return (intent, confidence)
        
        # PASO 3: Palabras clave directas
        intent, confidence = self._check_keyword_match(text_lower)
        if intent:
            return (intent, confidence)
        
        # PASO 4: Similaridad vectorial
        intent, confidence = self._calculate_vector_similarity(doc)
        
        # PASO 5: Ajustes contextuales
        intent, confidence = self._contextual_adjustments(doc, intent, confidence, text_lower)
        
        if confidence < 0.6:
            return (INTENT_GENERAL, confidence)
        
        print(f"🎯 Intención: {intent} (confianza: {confidence:.2f})")
        return (intent, confidence)
    
    def _cosine_similarity(self, vec1: np.ndarray, vec2: np.ndarray) -> float:
        """Similaridad coseno"""
        dot_product = np.dot(vec1, vec2)
        norm1 = np.linalg.norm(vec1)
        norm2 = np.linalg.norm(vec2)
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        return dot_product / (norm1 * norm2)
    
    def _contextual_adjustments(self, doc, intent: str, confidence: float, text_lower: str) -> Tuple[str, float]:
        """Ajustes finales según contexto"""
        
        # Saludo corto
        if len(doc) <= 3:
            greeting_words = ['hola', 'hey', 'buenas', 'buenos']
            if any(token.text in greeting_words for token in doc):
                return (INTENT_GREETING, 0.95)
        
        # Forzar estadísticas si menciona palabras clave
        if any(word in text_lower for word in STATS_KEYWORDS):
            # Solo si NO es claramente estructura
            if 'columnas' not in text_lower or LITERAL_ESTADISTICAS in text_lower:  # Usando constante
                print("🔧 Ajuste: Forzando STATISTICAL")
                return (INTENT_STATISTICAL, 0.95)
        
        # Solo estructura si menciona columnas sin estadísticas
        if any(word in text_lower for word in STRUCTURE_KEYWORDS):
            if not any(word in text_lower for word in STATS_KEYWORDS):
                return (INTENT_STRUCTURE, min(confidence + 0.1, 0.95))
        
        return (intent, confidence)
    
    def get_intent_details(self, text: str) -> Dict:
        """Análisis completo de la intención"""
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
