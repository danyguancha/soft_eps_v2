"""
Tests para Consulta al Chatbot (IA)
"""
import unittest
import sys
import os
from fastapi.testclient import TestClient
from io import BytesIO

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from main import app

client = TestClient(app)

class TestChatbotIA(unittest.TestCase):
    """Tests para endpoints de IA/Chatbot"""
    
    @classmethod
    def setUpClass(cls):
        """Configuración inicial - cargar archivo para contexto"""
        csv_content = b"documento,nombre,fecha_nacimiento,departamento\n"
        csv_content += b"1234,Juan Perez,1990-01-01,CALDAS\n"
        
        files = {'file': ('datos_vacunacion.csv', BytesIO(csv_content), 'text/csv')}
        response = client.post("/api/v1/upload", files=files)
        
        if response.status_code == 200:
            cls.file_id = response.json()["file_id"]
        else:
            raise Exception("No se pudo cargar el archivo de prueba")
    
    def test_IA_01(self):
        """IA-01: Consulta al asistente IA sin contexto de archivo"""
        payload = {
            "question": "¿Cómo puedo calcular la edad a partir de fecha de nacimiento?",
            "file_context": None,
            "session_id": "user_session_001"
        }
        
        response = client.post("/api/v1/ai", json=payload)
        
        # Validaciones estrictas
        self.assertEqual(
            response.status_code, 
            200, 
            f"Error en endpoint AI: {response.text if response.status_code != 200 else ''}"
        )
        
        data = response.json()
        
        # Validar estructura de respuesta
        self.assertTrue(data.get("success", False), "success debe ser True")
        self.assertIn("response", data, "Falta campo 'response'")
        self.assertIsInstance(data["response"], str, "response debe ser string")
        self.assertGreater(len(data["response"]), 0, "response no puede estar vacía")
        
        # Validar campos adicionales
        self.assertIn("context_type", data, "Falta campo 'context_type'")
        self.assertIn("query_type", data, "Falta campo 'query_type'")
        
        print(f"✅ IA-01 PASSED")
    
    def test_IA_02(self):
        """IA-02: Consulta al asistente IA con contexto de archivo específico"""
        payload = {
            "question": "¿Cuáles son las estadísticas principales de este archivo?",
            "file_context": self.file_id,
            "session_id": "user_session_001"
        }
        
        response = client.post("/api/v1/ai", json=payload)
        
        # Validaciones estrictas
        self.assertEqual(
            response.status_code, 
            200, 
            f"Error en endpoint AI: {response.text if response.status_code != 200 else ''}"
        )
        
        data = response.json()
        
        # Validar estructura de respuesta
        self.assertTrue(data.get("success", False), "success debe ser True")
        self.assertIn("response", data, "Falta campo 'response'")
        self.assertIsInstance(data["response"], str, "response debe ser string")
        self.assertGreater(len(data["response"]), 0, "response no puede estar vacía")
        
        # Validar contexto de archivo
        self.assertIn("file_context", data, "Falta campo 'file_context'")
        self.assertEqual(
            data.get("file_context"), 
            self.file_id, 
            f"file_context incorrecto: esperado {self.file_id}, recibido {data.get('file_context')}"
        )
        
        print(f"✅ IA-02 PASSED")
    
    @classmethod
    def tearDownClass(cls):
        """Limpieza"""
        try:
            client.delete(f"/api/v1/file/{cls.file_id}")
        except:
            pass


if __name__ == "__main__":
    unittest.main(verbosity=2)
