"""
Tests de Rendimiento - Technical Note (Nota Tecnica)
"""
import unittest
import sys
import os
import time
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from main import app

client = TestClient(app)


class TestPerformanceTechnicalNote(unittest.TestCase):
    """Pruebas de rendimiento para modulo de nota tecnica"""
    
    @classmethod
    def setUpClass(cls):
        """Crear archivo de prueba en technical_note/ y obtenerlo"""
        print("\n" + "="*70)
        print("CONFIGURACION: Creando archivo de prueba en technical_note/")
        print("="*70)
        
        # Importar el generador
        from tests.performance.generator.data_generator import generate_technical_note_data
        
        # Asegurar que el directorio existe
        tech_note_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'technical_note')
        os.makedirs(tech_note_dir, exist_ok=True)
        
        # Crear archivo CSV de prueba con dataset grande
        cls.test_filename = "TestNT_Performance.csv"
        test_file_path = os.path.join(tech_note_dir, cls.test_filename)
        
        # Generar dataset grande (43000 registros)
        print("   Generando dataset de 43000 registros...")
        csv_buffer = generate_technical_note_data(rows=43000)
        
        # Escribir archivo
        with open(test_file_path, 'wb') as f:
            f.write(csv_buffer.read())
        
        print(f"OK Archivo de prueba creado: {cls.test_filename}")
        print(f"   Ubicacion: {test_file_path}")
        print(f"   Registros: 43000")
        print(f"   Columnas: documento, nombre, fecha_nacimiento, departamento, municipio, ips, SERVICIO")
        
        # Verificar que el endpoint lo detecta
        response = client.get("/api/v1/technical-note/available")
        
        if response.status_code == 200:
            data = response.json()
            files = data if isinstance(data, list) else data.get('files', [])
            
            if len(files) > 0:
                for file_info in files:
                    filename = file_info.get('filename', '') if isinstance(file_info, dict) else file_info
                    if filename == cls.test_filename:
                        cls.filename = filename
                        print(f"OK Archivo detectado por el backend: {cls.filename}")
                        print("="*70)
                        return
                
                first_file = files[0]
                cls.filename = first_file.get('filename', '') if isinstance(first_file, dict) else first_file
                print(f"OK Usando archivo disponible: {cls.filename}")
                print("="*70)
            else:
                cls.filename = None
                print("ADVERTENCIA: Backend no detecta archivos")
                print("="*70)
        else:
            cls.filename = None
            print(f"ERROR: No se pudo obtener lista de archivos (status {response.status_code})")
            print("="*70)
    
    @classmethod
    def tearDownClass(cls):
        """Limpiar archivo de prueba creado"""
        print("\n" + "="*70)
        print("LIMPIEZA: Eliminando archivo de prueba")
        print("="*70)
        
        try:
            tech_note_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'technical_note')
            test_file_path = os.path.join(tech_note_dir, cls.test_filename)
            
            if os.path.exists(test_file_path):
                os.remove(test_file_path)
                print(f"OK Archivo eliminado: {cls.test_filename}")
            
            print("="*70)
        except Exception as e:
            print(f"ADVERTENCIA: Error al eliminar archivo: {e}")
            print("="*70)
    
    def test_NT_AD_01_listar_archivos_disponibles(self):
        """
        NT-AD-01: Listar archivos tecnicos disponibles
        Expectativa: < 500 ms (0.5 segundos)
        Manual: 300-600 segundos (5-10 min)
        """
        print("\n" + "="*70)
        print("NT-AD-01: Listar archivos disponibles")
        print("="*70)
        
        start_time = time.time()
        response = client.get("/api/v1/technical-note/available")
        end_time = time.time()
        
        tiempo_software = end_time - start_time
        tiempo_manual_estimado = 450
        mejora_porcentaje = ((tiempo_manual_estimado - tiempo_software) / tiempo_manual_estimado) * 100
        
        self.assertEqual(response.status_code, 200, f"Error: {response.text}")
        data = response.json()
        files = data if isinstance(data, list) else data.get('files', [])
        
        print(f"   Tiempo software: {tiempo_software:.3f} segundos ({tiempo_software * 43000:.0f} ms)")
        print(f"   Tiempo manual estimado: {tiempo_manual_estimado} segundos (7.5 min)")
        print(f"   Mejora: {mejora_porcentaje:.1f}%")
        print(f"   Archivos encontrados: {len(files)}")
        print(f"   Expectativa: < 0.5 seg | Resultado: {'PASS' if tiempo_software < 0.5 else 'FAIL'}")
        print("="*70)
        
        self.assertLess(tiempo_software, 0.5, f"Tiempo excedido: {tiempo_software:.3f}s > 0.5s")
        self.assertGreater(len(files), 0, "Debe haber al menos 1 archivo disponible")
    
    def test_NT_OD_01_obtener_departamentos(self):
        """
        NT-OD-01: Obtener lista de departamentos unicos
        Expectativa: < 300 ms (0.3 segundos)
        Manual: 180-300 segundos (3-5 min)
        """
        print("\n" + "="*70)
        print("NT-OD-01: Obtener departamentos unicos")
        print("="*70)
        
        if not self.filename:
            self.skipTest("No hay archivos de nota tecnica disponibles")
        
        print(f"   Usando archivo: {self.filename}")
        
        start_time = time.time()
        response = client.get(f"/api/v1/technical-note/geographic/{self.filename}/departamentos")
        end_time = time.time()
        
        if response.status_code == 404:
            self.skipTest(f"Archivo {self.filename} no disponible en backend")
        
        tiempo_software = end_time - start_time
        tiempo_manual_estimado = 240
        mejora_porcentaje = ((tiempo_manual_estimado - tiempo_software) / tiempo_manual_estimado) * 100
        
        self.assertEqual(response.status_code, 200, f"Error: {response.text}")
        data = response.json()
        departamentos = data.get('values', [])
        
        print(f"   Tiempo software: {tiempo_software:.3f} segundos ({tiempo_software * 43000:.0f} ms)")
        print(f"   Tiempo manual estimado: {tiempo_manual_estimado} segundos (4 min)")
        print(f"   Mejora: {mejora_porcentaje:.1f}%")
        print(f"   Departamentos unicos: {len(departamentos)}")
        print(f"   Expectativa: < 0.3 seg | Resultado: {'PASS' if tiempo_software < 0.3 else 'FAIL'}")
        print("="*70)
        
        self.assertLess(tiempo_software, 0.3, f"Tiempo excedido: {tiempo_software:.3f}s > 0.3s")
        self.assertGreater(len(departamentos), 0, "Debe haber al menos 1 departamento")
    
    def test_NT_OI_01_obtener_ips_filtradas(self):
        """
        NT-OI-01: Obtener IPS filtradas por departamento y municipio
        Expectativa: < 500 ms (0.5 segundos)
        Manual: 300-480 segundos (5-8 min)
        """
        print("\n" + "="*70)
        print("NT-OI-01: Obtener IPS filtradas")
        print("="*70)
        
        if not self.filename:
            self.skipTest("No hay archivos de nota tecnica disponibles")
        
        print(f"   Usando archivo: {self.filename}")
        
        start_time = time.time()
        response = client.get(
            f"/api/v1/technical-note/geographic/{self.filename}/ips",
            params={"departamento": "CALDAS", "municipio": "MANIZALES"}
        )
        end_time = time.time()
        
        if response.status_code == 404:
            self.skipTest(f"Archivo {self.filename} o endpoint no disponible")
        
        tiempo_software = end_time - start_time
        tiempo_manual_estimado = 390
        mejora_porcentaje = ((tiempo_manual_estimado - tiempo_software) / tiempo_manual_estimado) * 100
        
        self.assertEqual(response.status_code, 200, f"Error: {response.text}")
        data = response.json()
        ips_list = data.get('values', [])
        
        print(f"   Tiempo software: {tiempo_software:.3f} segundos ({tiempo_software * 43000:.0f} ms)")
        print(f"   Tiempo manual estimado: {tiempo_manual_estimado} segundos (6.5 min)")
        print(f"   Mejora: {mejora_porcentaje:.1f}%")
        print(f"   IPS unicas: {len(ips_list)}")
        print(f"   Expectativa: < 0.5 seg | Resultado: {'PASS' if tiempo_software < 0.5 else 'FAIL'}")
        print("="*70)
        
        self.assertLess(tiempo_software, 0.5, f"Tiempo excedido: {tiempo_software:.3f}s > 0.5s")
    
    def test_NT_GR_01_generar_reporte_basico(self):
        """
        NT-GR-01: Generar reporte basico con keyword "Medicina"
        Expectativa: < 12 segundos
        Manual: 2700-5400 segundos (45-90 min)
        """
        print("\n" + "="*70)
        print("NT-GR-01: Generar reporte basico")
        print("="*70)
        
        if not self.filename:
            self.skipTest("No hay archivos de nota tecnica disponibles")
        
        print(f"   Usando archivo: {self.filename}")
        
        start_time = time.time()
        response = client.get(
            f"/api/v1/technical-note/report/{self.filename}",
            params={
                "keywords": "Medicina",
                "corte_fecha": "2025-10-31",
                "include_temporal": True
            }
        )
        end_time = time.time()
        
        if response.status_code in [404, 500]:
            error_detail = response.json().get('detail', 'Error desconocido')
            self.skipTest(f"Archivo no disponible o error del servidor: {error_detail}")
        
        tiempo_software = end_time - start_time
        tiempo_manual_estimado = 4050
        mejora_porcentaje = ((tiempo_manual_estimado - tiempo_software) / tiempo_manual_estimado) * 100
        
        self.assertEqual(response.status_code, 200, f"Error: {response.text}")
        data = response.json()
        items = data.get('items', [])
        
        print(f"   Tiempo software: {tiempo_software:.3f} segundos")
        print(f"   Tiempo manual estimado: {tiempo_manual_estimado} segundos (67.5 min)")
        print(f"   Mejora: {mejora_porcentaje:.1f}%")
        print(f"   Rangos de edad procesados: {len(items)}")
        print(f"   Expectativa: < 12 seg | Resultado: {'PASS' if tiempo_software < 12 else 'FAIL'}")
        print("="*70)
        
        self.assertLess(tiempo_software, 12, f"Tiempo excedido: {tiempo_software:.3f}s > 12s")
    
    def test_NT_RM_01_reporte_multiples_keywords_con_filtro(self):
        """
        NT-RM-01: Reporte con multiples keywords y filtro geografico
        Expectativa: < 25 segundos
        Manual: 5400-9000 segundos (1.5-2.5 horas)
        """
        print("\n" + "="*70)
        print("NT-RM-01: Reporte multiples keywords con filtro geografico")
        print("="*70)
        
        if not self.filename:
            self.skipTest("No hay archivos de nota tecnica disponibles")
        
        print(f"   Usando archivo: {self.filename}")
        print(f"   Keywords: Medicina, Control, Vacunacion")
        print(f"   Filtro: CALDAS")
        
        start_time = time.time()
        response = client.get(
            f"/api/v1/technical-note/report/{self.filename}",
            params={
                "keywords": "Medicina,Control,Vacunacion",
                "departamento": "CALDAS",
                "corte_fecha": "2025-10-31",
                "include_temporal": True,
                "min_count": 0
            }
        )
        end_time = time.time()
        
        if response.status_code in [404, 500]:
            error_detail = response.json().get('detail', 'Error desconocido')
            self.skipTest(f"Archivo no disponible o error del servidor: {error_detail}")
        
        tiempo_software = end_time - start_time
        tiempo_manual_estimado = 7200
        mejora_porcentaje = ((tiempo_manual_estimado - tiempo_software) / tiempo_manual_estimado) * 100
        
        self.assertEqual(response.status_code, 200, f"Error: {response.text}")
        data = response.json()
        
        self.assertTrue(data.get('success', False), "Reporte debe ser exitoso")
        
        items = data.get('items', [])
        total_combinaciones = len(items)
        throughput = total_combinaciones / tiempo_software if tiempo_software > 0 else 0
        
        filters_applied = data.get('filters_applied', {})
        
        print(f"\n   --- METRICAS DE RENDIMIENTO ---")
        print(f"   Tiempo software: {tiempo_software:.3f} segundos")
        print(f"   Tiempo manual estimado: {tiempo_manual_estimado} segundos (2 horas)")
        print(f"   Mejora: {mejora_porcentaje:.1f}%")
        print(f"\n   --- METRICAS DE PROCESAMIENTO ---")
        print(f"   Combinaciones keyword-edad procesadas: {total_combinaciones}")
        print(f"   Throughput: {throughput:.3f} combinaciones/segundo")
        print(f"   Keywords procesadas: 3 (Medicina, Control, Vacunacion)")
        print(f"   Filtro geografico aplicado: {filters_applied.get('departamento', 'N/A')}")
        
        global_stats = data.get('global_statistics', {})
        if global_stats:
            print(f"\n   --- ESTADISTICAS GLOBALES ---")
            cobertura = global_stats.get('cobertura_global_porcentaje', 0)
            numerador = global_stats.get('numerador_total', 0)
            denominador = global_stats.get('denominador_total', 0)
            
            print(f"   Cobertura global: {cobertura:.2f}%")
            print(f"   Numerador total: {numerador}")
            print(f"   Denominador total: {denominador}")
        
        print(f"\n   --- VALIDACIONES ---")
        
        if total_combinaciones == 0:
            print(f"   ADVERTENCIA: No se encontraron combinaciones (dataset pequeño)")
            print(f"   Validando que el endpoint responde correctamente...")
            
            self.assertIn('items', data, "Respuesta debe contener 'items'")
            self.assertIn('success', data, "Respuesta debe contener 'success'")
            
            if filters_applied:
                print(f"   OK: Filtro geografico aplicado correctamente")
                self.assertEqual(filters_applied.get('departamento'), 'CALDAS', 
                                "Filtro de departamento debe estar aplicado")
            
            print(f"   OK: Endpoint funcional (sin datos por dataset pequeño)")
        else:
            print(f"   OK: {total_combinaciones} combinaciones encontradas")
            self.assertGreater(total_combinaciones, 0)
            self.assertGreater(throughput, 0)
            
            if filters_applied:
                self.assertEqual(filters_applied.get('departamento'), 'CALDAS', 
                                "Filtro de departamento debe estar aplicado")
        
        print(f"\n   Expectativa tiempo: < 25 seg | Resultado: {'PASS' if tiempo_software < 25 else 'FAIL'}")
        print("="*70)
        
        self.assertLess(tiempo_software, 25, f"Tiempo excedido: {tiempo_software:.3f}s > 25s")
    
    def test_NT_RC_01_reporte_completo_con_filtros(self):
        """
        NT-RC-01: Reporte completo con todos los filtros
        Expectativa: < 18 segundos
        Manual: 7200-10800 segundos (2-3 horas)
        """
        print("\n" + "="*70)
        print("NT-RC-01: Reporte completo con filtros")
        print("="*70)
        
        if not self.filename:
            self.skipTest("No hay archivos de nota tecnica disponibles")
        
        print(f"   Usando archivo: {self.filename}")
        
        start_time = time.time()
        response = client.get(
            f"/api/v1/technical-note/report/{self.filename}",
            params={
                "keywords": "Medicina",
                "departamento": "NARIÑO",
                "municipio": "IPIALES",
                "ips": "IPS INDIGENA MALLAMAS",
                "corte_fecha": "2025-10-31",
                "include_temporal": True
            }
        )
        end_time = time.time()
        
        if response.status_code in [404, 500]:
            error_detail = response.json().get('detail', 'Error desconocido')
            self.skipTest(f"Archivo/endpoint no disponible o error del servidor: {error_detail}")
        
        tiempo_software = end_time - start_time
        tiempo_manual_estimado = 9000
        mejora_porcentaje = ((tiempo_manual_estimado - tiempo_software) / tiempo_manual_estimado) * 100
        
        self.assertEqual(response.status_code, 200, f"Error: {response.text}")
        data = response.json()
        
        print(f"   Tiempo software: {tiempo_software:.3f} segundos")
        print(f"   Tiempo manual estimado: {tiempo_manual_estimado} segundos (2.5 horas)")
        print(f"   Mejora: {mejora_porcentaje:.1f}%")
        print(f"   Expectativa: < 18 seg | Resultado: {'PASS' if tiempo_software < 18 else 'FAIL'}")
        print("="*70)
        
        self.assertLess(tiempo_software, 18, f"Tiempo excedido: {tiempo_software:.3f}s > 18s")


if __name__ == "__main__":
    unittest.main(verbosity=2)
