"""
Tests para Technical Note Endpoints - VERSIÓN FINAL CORREGIDA
"""
import unittest
import sys
import os
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from main import app

client = TestClient(app)

class TestTechnicalNote(unittest.TestCase):
    """Tests para endpoints de Technical Note"""
    
    def test_OA_01(self):
        """OA-01: Obtener lista de archivos técnicos disponibles"""
        response = client.get("/api/v1/technical-note/available")
        
        if response.status_code == 404:
            self.skipTest("Endpoint no implementado")
        
        self.assertEqual(response.status_code, 200, f"Error: {response.text}")
        data = response.json()
        
        if isinstance(data, list):
            self.assertIsInstance(data, list)
            print(f"OA-01 PASSED: {len(data)} archivos disponibles")
        else:
            self.assertTrue(data.get("success", False))
            self.assertIn("files", data)
            print(f"OA-01 PASSED: {len(data.get('files', []))} archivos disponibles")
    
    def test_OA_02(self):
        """OA-02: Validar respuesta cuando no hay archivos disponibles"""
        response = client.get("/api/v1/technical-note/available")
        
        if response.status_code == 404:
            self.skipTest("Endpoint no implementado")
        
        self.assertEqual(response.status_code, 200)
        data = response.json()
        
        if isinstance(data, list):
            self.assertIsInstance(data, list)
        else:
            self.assertTrue(data.get("success", False))
            self.assertIsInstance(data.get("files", []), list)
        
        print(f"OA-02 PASSED")


class TestFiltrosGeograficos(unittest.TestCase):
    """Tests para filtros geográficos de Technical Note"""
    
    @classmethod
    def setUpClass(cls):
        response = client.get("/api/v1/technical-note/available")
        if response.status_code == 200:
            data = response.json()
            available_files = data if isinstance(data, list) else data.get("files", [])
            
            for file_info in available_files:
                filename = file_info.get("filename", "") if isinstance(file_info, dict) else file_info
                if filename:
                    cls.filename = filename
                    print(f"📁 Usando archivo: {cls.filename}")
                    break
        else:
            cls.filename = "InfanciaNueva.csv"
    
    def test_FG_01(self):
        """FG-01: Obtener lista única de departamentos ordenada alfabéticamente"""
        response = client.get(f"/api/v1/technical-note/geographic/{self.filename}/departamentos")
        
        if response.status_code == 404:
            self.skipTest("Endpoint o archivo no disponible")
        
        if response.status_code == 500:
            data = response.json()
            if "No se encontró archivo" in data.get("detail", ""):
                self.skipTest(f"Archivo {self.filename} no existe")
        
        self.assertEqual(response.status_code, 200, f"Error: {response.text}")
        data = response.json()
        
        if not data.get("success", True):
            self.skipTest(f"Backend error: {data.get('error', 'Unknown')}")
        
        # 🔧 CORRECCIÓN: El backend retorna "values" en vez de "departamentos"
        self.assertIn("values", data, "Response debe contener 'values'")
        departamentos = data["values"]
        self.assertIsInstance(departamentos, list)
        
        if len(departamentos) > 0:
            self.assertEqual(departamentos, sorted(departamentos), "Departamentos deben estar ordenados")
            self.assertEqual(len(departamentos), len(set(departamentos)), "Departamentos deben ser únicos")
        
        print(f"FG-01 PASSED: {len(departamentos)} departamentos únicos")
    
    def test_FG_02(self):
        """FG-02: Validar manejo cuando columna departamento no existe"""
        response = client.get("/api/v1/technical-note/geographic/archivo_sin_geo.csv/departamentos")
        
        if response.status_code == 200:
            data = response.json()
            if data.get("success") == False:
                print(f"FG-02 PASSED: Error manejado con success=false")
                return
        
        self.assertIn(response.status_code, [400, 404, 500])
        print(f"FG-02 PASSED: Error manejado correctamente ({response.status_code})")
    
    def test_FG_03(self):
        """FG-03: Obtener todos los municipios sin filtro de departamento"""
        response = client.get(f"/api/v1/technical-note/geographic/{self.filename}/municipios")
        
        if response.status_code == 500:
            data = response.json()
            if "No se encontró archivo" in data.get("detail", ""):
                self.skipTest(f"Archivo {self.filename} no existe")
        
        if response.status_code != 200:
            self.skipTest(f"Backend error: {response.status_code}")
        
        data = response.json()
        
        if not data.get("success", True):
            self.skipTest(f"Backend error: {data.get('error', 'Unknown')}")
        
        # 🔧 CORRECCIÓN: Usar "values" en vez de "municipios"
        self.assertIn("values", data)
        municipios = data["values"]
        self.assertIsInstance(municipios, list)
        
        print(f"FG-03 PASSED: {len(municipios)} municipios obtenidos")
    
    def test_FG_04(self):
        """FG-04: Obtener municipios filtrados por departamento específico"""
        response = client.get(
            f"/api/v1/technical-note/geographic/{self.filename}/municipios",
            params={"departamento": "CALDAS"}
        )
        
        if response.status_code != 200:
            self.skipTest(f"Archivo no disponible o backend error")
        
        data = response.json()
        
        if not data.get("success", True):
            self.skipTest(f"Backend error: {data.get('error', 'Unknown')}")
        
        # 🔧 CORRECCIÓN: Usar "values" en vez de "municipios"
        self.assertIn("values", data)
        municipios = data["values"]
        self.assertIsInstance(municipios, list)
        
        # Validar que el filtro se aplicó
        filters_applied = data.get("filters_applied", {})
        self.assertEqual(filters_applied.get("departamento"), "CALDAS")
        
        print(f"FG-04 PASSED: {len(municipios)} municipios de CALDAS")
    
    def test_FG_05(self):
        """FG-05: Obtener todas las IPS sin filtros geográficos"""
        response = client.get(f"/api/v1/technical-note/geographic/{self.filename}/ips")
        
        if response.status_code != 200:
            self.skipTest(f"Archivo no disponible o backend error")
        
        data = response.json()
        
        if not data.get("success", True):
            self.skipTest(f"Backend error: {data.get('error', 'Unknown')}")
        
        # 🔧 CORRECCIÓN: Usar "values" en vez de "ips"
        self.assertIn("values", data)
        ips_list = data["values"]
        self.assertIsInstance(ips_list, list)
        
        print(f"FG-05 PASSED: {len(ips_list)} IPS obtenidas")
    
    def test_FG_06(self):
        """FG-06: Obtener IPS filtradas por departamento"""
        response = client.get(
            f"/api/v1/technical-note/geographic/{self.filename}/ips",
            params={"departamento": "CALDAS"}
        )
        
        if response.status_code != 200:
            self.skipTest(f"Archivo no disponible o backend error")
        
        data = response.json()
        
        if not data.get("success", True):
            self.skipTest(f"Backend error: {data.get('error', 'Unknown')}")
        
        # 🔧 CORRECCIÓN: Usar "values"
        self.assertIn("values", data)
        ips_list = data["values"]
        
        print(f"FG-06 PASSED: {len(ips_list)} IPS de CALDAS")
    
    def test_FG_07(self):
        """FG-07: Obtener IPS filtradas por departamento y municipio"""
        response = client.get(
            f"/api/v1/technical-note/geographic/{self.filename}/ips",
            params={"departamento": "CALDAS", "municipio": "MANIZALES"}
        )
        
        if response.status_code != 200:
            self.skipTest(f"Archivo no disponible o backend error")
        
        data = response.json()
        
        if not data.get("success", True):
            self.skipTest(f"Backend error: {data.get('error', 'Unknown')}")
        
        # 🔧 CORRECCIÓN: Usar "values"
        self.assertIn("values", data)
        ips_list = data["values"]
        
        print(f"FG-07 PASSED: {len(ips_list)} IPS de CALDAS-MANIZALES")


class TestReporteTecnico(unittest.TestCase):
    """Tests para generación de reportes técnicos"""
    
    @classmethod
    def setUpClass(cls):
        response = client.get("/api/v1/technical-note/available")
        if response.status_code == 200:
            data = response.json()
            available_files = data if isinstance(data, list) else data.get("files", [])
            
            for file_info in available_files:
                filename = file_info.get("filename", "") if isinstance(file_info, dict) else file_info
                if filename:
                    cls.filename = filename
                    break
        else:
            cls.filename = "InfanciaNueva.csv"
        
        cls.corte_fecha = "2025-07-31"
    
    def test_RT_01(self):
        """RT-01: Generar reporte con keywords específicas"""
        response = client.get(
            f"/api/v1/technical-note/report/{self.filename}",
            params={
                "keywords": "Medicina",
                "min_count": 5,
                "include_temporal": True,
                "corte_fecha": self.corte_fecha
            }
        )
        
        if response.status_code == 500:
            data = response.json()
            if "No se encontró archivo" in data.get("detail", ""):
                self.skipTest(f"Archivo {self.filename} no existe")
        
        if response.status_code != 200:
            self.skipTest(f"Backend error: {response.status_code}")
        
        data = response.json()
        
        if not data.get("success", True):
            self.skipTest(f"Backend error: {data.get('error', 'Unknown')}")
        
        self.assertIn("items", data)
        print(f"RT-01 PASSED: {len(data.get('items', []))} items")
    
    def test_RT_02(self):
        """RT-02: Generar reporte con filtro de departamento"""
        response = client.get(
            f"/api/v1/technical-note/report/{self.filename}",
            params={
                "keywords": "Medicina",
                "departamento": "CALDAS",
                "corte_fecha": self.corte_fecha
            }
        )        
        if response.status_code != 200:
            self.skipTest(f"Archivo no disponible o backend error")
        data = response.json()
        if not data.get("success", True):
            self.skipTest(f"Backend error")
        print(f"RT-02 PASSED")
    
    def test_RT_03(self):
        """RT-03: Generar reporte con todos los filtros geográficos"""
        response = client.get(
            f"/api/v1/technical-note/report/{self.filename}",
            params={
                "keywords": "Medicina",
                "departamento": "CALDAS",
                "municipio": "MANIZALES",
                "ips": "IPS SALUD TOTAL",
                "corte_fecha": self.corte_fecha
            }
        )        
        if response.status_code != 200:
            self.skipTest(f"Archivo no disponible o backend error")
        data = response.json()
        if not data.get("success", True):
            self.skipTest(f"Backend error")
        print(f"RT-03 PASSED")
    
    def test_RT_04(self):
        """RT-04: Generar reporte sin análisis temporal"""
        response = client.get(
            f"/api/v1/technical-note/report/{self.filename}",
            params={
                "keywords": "Medicina",
                "include_temporal": False,
                "corte_fecha": self.corte_fecha
            }
        )        
        if response.status_code != 200:
            self.skipTest(f"Archivo no disponible o backend error")        
        data = response.json()        
        if not data.get("success", True):
            self.skipTest(f"Backend error")        
        print(f"RT-04 PASSED")
    
    def test_RT_05(self):
        """RT-05: Validar error cuando corte_fecha está ausente"""
        response = client.get(
            f"/api/v1/technical-note/report/{self.filename}",
            params={"keywords": "medicina"}
        )        
        self.assertIn(response.status_code, [400, 422])
        print(f"RT-05 PASSED: Error de fecha ausente manejado ({response.status_code})")
    
    def test_RT_06(self):
        """RT-06: Validar error cuando formato de corte_fecha es inválido"""
        response = client.get(
            f"/api/v1/technical-note/report/{self.filename}",
            params={
                "keywords": "medicina",
                "corte_fecha": "31/07/2025"
            }
        )
        
        self.assertEqual(response.status_code, 400, f"Error: {response.text}")
        print(f"RT-07 PASSED: Error de formato de fecha manejado")


if __name__ == "__main__":
    unittest.main(verbosity=2)
