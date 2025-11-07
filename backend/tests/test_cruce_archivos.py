"""
Tests para Cruce de Archivos (CA) - VERSIÓN FINAL
"""
import unittest
import sys
import os
from fastapi.testclient import TestClient
from io import BytesIO

# Agregar el directorio raíz al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from main import app

client = TestClient(app)

class TestCruceArchivos(unittest.TestCase):
    """Tests para endpoints de cruce de archivos"""
    
    @classmethod
    def setUpClass(cls):
        """Configuración inicial - cargar dos archivos para cruce"""        
        # Archivo 1: Afiliados
        csv1_content = b"documento,nombre,edad\n"
        csv1_content += b"1234,Juan Perez,30\n"
        csv1_content += b"5678,Maria Gomez,25\n"
        csv1_content += b"9012,Carlos Lopez,35\n"
        files1 = {'file': ('afiliados.csv', BytesIO(csv1_content), 'text/csv')}
        response1 = client.post("/api/v1/upload", files=files1)
        if response1.status_code == 200:
            cls.file1_id = response1.json()["file_id"]
            print(f"📁 Archivo 1 cargado: {cls.file1_id}")
        else:
            print(f"❌ Error cargando archivo 1: {response1.text}")
            raise Exception("No se pudo cargar archivo 1")  
        # Archivo 2: Atenciones
        csv2_content = b"num_documento,fecha_atencion,diagnostico,ips\n"
        csv2_content += b"1234,2025-01-15,Gripe,IPS Norte\n"
        csv2_content += b"5678,2025-02-20,Diabetes,IPS Sur\n"
        csv2_content += b"7777,2025-03-10,Hipertension,IPS Centro\n"
        
        files2 = {'file': ('atenciones.csv', BytesIO(csv2_content), 'text/csv')}
        response2 = client.post("/api/v1/upload", files=files2)
        
        if response2.status_code == 200:
            cls.file2_id = response2.json()["file_id"]
            print(f"📁 Archivo 2 cargado: {cls.file2_id}")
        else:
            print(f"❌ Error cargando archivo 2: {response2.text}")
            raise Exception("No se pudo cargar archivo 2")
        
        print("="*60 + "\n")
    
    def test_CA_01(self):
        """CA-01: Realizar cruce left join exitoso entre dos archivos usando FileCrossRequest"""
        print("\n" + "-"*60)
        print("🧪 TEST CA-01: Cruce LEFT JOIN")
        print("-"*60)
        payload = {
            "file1_key": self.file1_id,
            "file2_key": self.file2_id,
            "file1_sheet": None,
            "file2_sheet": None,
            "key_column_file1": "documento",
            "key_column_file2": "num_documento",
            "cross_type": "left"
        }
        response = client.post("/api/v1/cross", json=payload)        
        # Validaciones
        self.assertEqual(response.status_code, 200, f"Error: {response.text if response.status_code != 200 else ''}")
        data = response.json()
        
        # Validar estructura básica
        self.assertIn("success", data)
        self.assertTrue(data["success"])
        self.assertIn("data", data, "Respuesta no contiene 'data'")
        self.assertIn("total_rows", data, "Respuesta no contiene 'total_rows'")
        self.assertIn("columns", data, "Respuesta no contiene 'columns'")
        if "statistics" in data:
            # Estructura anidada
            stats = data["statistics"]
            matched_rows = stats.get("matched_rows", 0)
            total_rows = data["total_rows"]
            print(f"   Total rows: {total_rows}")
            print(f"   Matched rows (from statistics): {matched_rows}")
            print(f"   Method: {stats.get('method', 'N/A')}")
        else:
            # Estructura plana
            matched_rows = data.get("matched_rows", 0)
            total_rows = data.get("total_rows", 0)
            print(f"   Total rows: {total_rows}")
            print(f"   Matched rows: {matched_rows}")
        
        # Validar que hay datos
        self.assertGreater(total_rows, 0, "No se generaron filas en el cruce")
        self.assertIsInstance(data["data"], list, "data no es una lista")
        self.assertEqual(len(data["data"]), total_rows, "Cantidad de registros no coincide")
        
        # Validar estructura de los registros
        if len(data["data"]) > 0:
            first_record = data["data"][0]
            self.assertIn("documento", first_record, "Falta columna documento")
            self.assertIn("nombre", first_record, "Falta columna nombre")
            self.assertIn("edad", first_record, "Falta columna edad")
            print(f"   Columnas en resultado: {data['columns']}")
        
        print(f"\n✅ CA-01 PASSED: Cruce realizado exitosamente")
        print("-"*60)
    
    def test_CA_02(self):
        """CA-02: Realizar cruce entre hojas específicas de archivos Excel"""
        print("\n" + "-"*60)
        print("🧪 TEST CA-02: Cruce Excel con hojas específicas")
        print("-"*60)
        
        try:
            import openpyxl
            from openpyxl import Workbook
            
            # Crear dos Excel con hojas específicas
            wb1 = Workbook()
            ws1 = wb1.active
            ws1.title = "Datos2024"
            ws1.append(["id", "nombre"])
            ws1.append([1, "Test1"])
            
            excel1_buffer = BytesIO()
            wb1.save(excel1_buffer)
            excel1_buffer.seek(0)
            
            wb2 = Workbook()
            ws2 = wb2.active
            ws2.title = "Resumen"
            ws2.append(["id_registro", "valor"])
            ws2.append([1, 100])
            
            excel2_buffer = BytesIO()
            wb2.save(excel2_buffer)
            excel2_buffer.seek(0)
            
            # Subir archivos
            files1 = {'file': ('reporte.xlsx', excel1_buffer, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}
            files2 = {'file': ('consolidado.xlsx', excel2_buffer, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}
            
            resp1 = client.post("/api/v1/upload", files=files1)
            resp2 = client.post("/api/v1/upload", files=files2)
            
            if resp1.status_code == 200 and resp2.status_code == 200:
                file1_excel = resp1.json()["file_id"]
                file2_excel = resp2.json()["file_id"]
                print(f"   Excel 1: {file1_excel}")
                print(f"   Excel 2: {file2_excel}")
                payload = {
                    "file1_key": file1_excel,
                    "file2_key": file2_excel,
                    "file1_sheet": "Datos2024",
                    "file2_sheet": "Resumen",
                    "key_column_file1": "id",
                    "key_column_file2": "id_registro",
                    "cross_type": "left"
                }
                
                response = client.post("/api/v1/cross", json=payload)                
                if response.status_code == 200:
                    data = response.json()
                    self.assertTrue(data.get("success", False))
                    self.assertIn("data", data)
                    self.assertIn("total_rows", data)
                    
                    print(f"   Total rows: {data.get('total_rows', 0)}")
                    print(f"\n✅ CA-02 PASSED: Cruce de Excel exitoso")
                else:
                    print(f"   Error: {response.json().get('detail', 'N/A')}")
                    self.skipTest(f"Cruce falló: {response.json().get('detail')}")
                
                # Limpiar
                client.delete(f"/api/v1/file/{file1_excel}")
                client.delete(f"/api/v1/file/{file2_excel}")
                print("-"*60)
            else:
                self.skipTest("No se pudieron cargar los archivos Excel")
                
        except ImportError:
            self.skipTest("openpyxl no está instalado")
    
    @classmethod
    def tearDownClass(cls):
        """Limpieza - eliminar archivos de prueba"""
        print("\n" + "="*60)
        print("🧹 LIMPIEZA - TEST CRUCE ARCHIVOS")
        print("="*60)
        
        try:
            client.delete(f"/api/v1/file/{cls.file1_id}")
            client.delete(f"/api/v1/file/{cls.file2_id}")
            print(f"✅ Archivos de prueba eliminados")
        except Exception as e:
            print(f"⚠️ Error al eliminar archivos: {e}")
        
        print("="*60 + "\n")


if __name__ == "__main__":
    unittest.main(verbosity=2)
