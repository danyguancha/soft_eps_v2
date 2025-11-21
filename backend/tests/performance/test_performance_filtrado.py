"""
Tests de Rendimiento - PR-FD: Filtrado de Datos
"""
import unittest
import sys
import os
import time
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from main import app
from tests.performance.generator.data_generator import generate_csv_data

client = TestClient(app)


class TestPerformanceFiltrado(unittest.TestCase):
    """Pruebas de rendimiento para filtrado y consultas"""
    
    @classmethod
    def setUpClass(cls):
        """Cargar dataset de 100K registros para pruebas"""
        print("\n" + "="*70)
        print("CONFIGURACIÓN: Cargando dataset de 100,000 registros")
        print("="*70)
        
        csv_data = generate_csv_data(rows=100000, columns=10)
        files = {'file': ('test_100k.csv', csv_data, 'text/csv')}
        response = client.post("/api/v1/upload", files=files)
        
        if response.status_code == 200:
            cls.file_id = response.json()['file_id']
            print(f"Dataset cargado: {cls.file_id}")
            print("="*70)
        else:
            raise Exception("No se pudo cargar el dataset de prueba")
    
    def test_PR_FD_01_filtrar_100k_por_departamento(self):
        """
        PR-FD-01: Filtrar 100,000 registros por departamento
        Expectativa: < 500 ms (0.5 segundos)
        Manual: 120-300 segundos (2-5 min)
        """
        print("\n" + "="*70)
        print("PR-FD-01: Filtrar 100K registros por departamento")
        print("="*70)
        
        payload = {
            "file_id": self.file_id,
            "page": 1,
            "page_size": 1000,
            "filters": [
                {
                    "column": "departamento",
                    "operator": "equals",
                    "value": "CALDAS"
                }
            ]
        }
        
        # Medir tiempo
        start_time = time.time()
        response = client.post("/api/v1/data", json=payload)
        end_time = time.time()
        
        tiempo_software = end_time - start_time
        tiempo_manual_estimado = 210  # Promedio: 3.5 minutos
        mejora_porcentaje = ((tiempo_manual_estimado - tiempo_software) / tiempo_manual_estimado) * 100
        
        # Validaciones
        self.assertEqual(response.status_code, 200, f"Error: {response.text}")
        data = response.json()
        self.assertIn('data', data)
        
        # Resultados
        print(f"Tiempo software: {tiempo_software:.3f} segundos ({tiempo_software * 1000:.0f} ms)")
        print(f"Tiempo manual estimado: {tiempo_manual_estimado} segundos (3.5 min)")
        print(f"Mejora: {mejora_porcentaje:.1f}%")
        print(f"Registros filtrados: {data.get('total_rows', 0)}")
        print(f"Expectativa: < 0.5 seg | Resultado: {'PASS ✓' if tiempo_software < 0.5 else 'FAIL ✗'}")
        print("="*70)
        
        self.assertLess(tiempo_software, 0.5, f"Tiempo excedido: {tiempo_software:.3f}s > 0.5s")
    
    def test_PR_FD_02_busqueda_global_50k(self):
        """
        PR-FD-02: Búsqueda global en 50,000 registros
        Expectativa: < 2 segundos
        Manual: 300-900 segundos (5-15 min)
        """
        print("\n" + "="*70)
        print("PR-FD-02: Búsqueda global en dataset")
        print("="*70)
        
        payload = {
            "file_id": self.file_id,
            "page": 1,
            "page_size": 100,
            "search": "CALDAS"
        }
        
        # Medir tiempo
        start_time = time.time()
        response = client.post("/api/v1/data", json=payload)
        end_time = time.time()
        
        tiempo_software = end_time - start_time
        tiempo_manual_estimado = 600  # Promedio: 10 minutos
        mejora_porcentaje = ((tiempo_manual_estimado - tiempo_software) / tiempo_manual_estimado) * 100
        
        # Validaciones
        self.assertEqual(response.status_code, 200)
        data = response.json()
        
        # Resultados
        print(f"Tiempo software: {tiempo_software:.3f} segundos")
        print(f"Tiempo manual estimado: {tiempo_manual_estimado} segundos (10 min)")
        print(f"Mejora: {mejora_porcentaje:.1f}%")
        print(f"Coincidencias: {data.get('total_rows', 0)}")
        print(f"Expectativa: < 2 seg | Resultado: {'PASS ✓' if tiempo_software < 2 else 'FAIL ✗'}")
        print("="*70)
        
        self.assertLess(tiempo_software, 2, f"Tiempo excedido: {tiempo_software:.3f}s > 2s")
    
    @classmethod
    def tearDownClass(cls):
        """Limpiar dataset de prueba"""
        print("\n" + "="*70)
        print("LIMPIEZA: Eliminando dataset de prueba")
        print("="*70)
        client.delete(f"/api/v1/file/{cls.file_id}")
        print("Dataset eliminado")
        print("="*70)


if __name__ == "__main__":
    unittest.main(verbosity=2)
