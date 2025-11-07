import unittest
import re

def extract_keywords(query):
    # Demo: extraer palabras relevantes
    matches = re.findall(r'\b[a-zA-ZáéíóúñÉÁÍÓÚÑ]+\b', query.lower())
    palabras = [p for p in matches if len(p) > 3]
    return list(set(palabras))

def detect_intent(query):
    q = query.lower()
    if "estadística" in q or "estadísticas" in q:
        return "statistical"
    if "registros" in q or "muéstrame" in q:
        return "data_query"
    return "general"

class TestNLP(unittest.TestCase):
    def test_NL_01_extract_keywords(self):
        keywords = extract_keywords("mostrar estadísticas de Medicina en Caldas")
        for kw in ["estadísticas", "medicina", "caldas"]:
            self.assertIn(kw, keywords)

    def test_NL_02_detect_intent_statistics(self):
        intent = detect_intent("dame las estadísticas del archivo")
        self.assertEqual(intent, "statistical")

    def test_NL_03_detect_intent_data_query(self):
        intent = detect_intent("muéstrame los registros de Medicina")
        self.assertEqual(intent, "data_query")

if __name__ == "__main__":
    unittest.main()
