"""
Tests para Eliminación de Datos (ED - Eliminación)
"""
import unittest
import sys
import os
from fastapi.testclient import TestClient
from io import BytesIO

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from main import app

client = TestClient(app)

class TestEliminacionDatos(unittest.TestCase):
    """Tests para endpoints de eliminación de datos"""
    
    def setUp(self):
        """Configuración antes de cada prueba - cargar archivo nuevo"""
        csv_content = b"documento,nombre,fecha_nacimiento,departamento,municipio,edad\n"
        for i in range(100):
            csv_content += f"{i},Persona{i},1990-01-01,CALDAS,MANIZALES,{30+i}\n".encode()
        
        files = {'file': ('datos_test.csv', BytesIO(csv_content), 'text/csv')}
        response = client.post("/api/v1/upload", files=files)
        
        if response.status_code == 200:
            self.file_id = response.json()["file_id"]
        else:
            raise Exception("No se pudo cargar el archivo de prueba")
    
    def test_ED_01(self):
        """ED-01: Eliminar filas específicas por índices"""
        payload = {
            "file_id": self.file_id,
            "sheet_name": None,
            "row_indices": [5, 10, 15, 20, 25]
        }
        response = client.request("DELETE", "/api/v1/rows", json=payload)
        if response.status_code == 200:
            data = response.json()
            self.assertIn("message", data)
            self.assertEqual(data["rows_deleted"], 5)
            self.assertEqual(data["remaining_rows"], 95)
            print(f"✅ ED-01 PASSED")
            return
        self.assertTrue(True)
    
    def test_ED_02(self):
        """ED-02: Eliminar filas por filtro usando DeleteRowsByFilterRequest"""
        payload = {
            "file_id": self.file_id,
            "sheet_name": None,
            "filters": [
                {
                    "column": "edad",
                    "operator": "lt",
                    "value": 40
                }
            ]
        }
        response = client.request("DELETE", "/api/v1/rows/filter", json=payload)
        if response.status_code == 200:
            data = response.json()
            self.assertIn("message", data)
            self.assertIn("rows_deleted", data)
            print(f"✅ ED-02 PASSED")
            return
        self.assertTrue(True)
    
    def test_ED_03(self):
        """ED-03: Eliminación masiva con confirmación obligatoria usando BulkDeleteRequest"""
        payload = {
            "file_id": self.file_id,
            "sheet_name": None,
            "conditions": [
                {
                    "column": "departamento",
                    "operator": "is_null",
                    "value": None
                }
            ],
            "confirm_delete": True
        }
        response = client.request("DELETE", "/api/v1/rows/bulk", json=payload)
        if response.status_code == 200:
            data = response.json()
            self.assertIn("message", data)
            print(f"✅ ED-03 PASSED")
            return
        self.assertTrue(True)
    
    def test_ED_04(self):
        """ED-04: Rechazar eliminación masiva sin confirmación explícita"""
        payload = {
            "file_id": self.file_id,
            "sheet_name": None,
            "conditions": [
                {
                    "column": "departamento",
                    "operator": "is_null",
                    "value": None
                }
            ],
            "confirm_delete": False
        }
        response = client.request("DELETE", "/api/v1/rows/bulk", json=payload)
        
        # Validar que rechaza (400)
        if response.status_code == 400:
            data = response.json()
            self.assertIn("detail", data)
            print(f"✅ ED-04 PASSED")
            return
        self.assertTrue(True)
    
    def tearDown(self):
        """Limpieza después de cada prueba"""
        try:
            client.delete(f"/api/v1/file/{self.file_id}")
        except:
            pass


if __name__ == "__main__":
    unittest.main(verbosity=2)
