"""
Script para ejecutar todas las pruebas
"""
import unittest
import sys

# Importar todos los módulos de prueba
from test_gestion_archivos import TestGestionArchivos
from test_obtener_data import TestObtenerData
from test_cruce_archivos import TestCruceArchivos
from test_exportacion_datos import TestExportacionDatos
from test_eliminacion_datos import TestEliminacionDatos
from test_chatbot_ia import TestChatbotIA
from test_technical_note import (
    TestTechnicalNote,
    TestFiltrosGeograficos,
    TestReporteTecnico
)

def suite():
    """Crear suite de pruebas"""
    test_suite = unittest.TestSuite()
    
    # Agregar tests de gestión de archivos
    test_suite.addTest(unittest.makeSuite(TestGestionArchivos))
    
    # Agregar tests de obtención de datos
    test_suite.addTest(unittest.makeSuite(TestObtenerData))
    
    # Agregar tests de cruce
    test_suite.addTest(unittest.makeSuite(TestCruceArchivos))
    
    # Agregar tests de exportación
    test_suite.addTest(unittest.makeSuite(TestExportacionDatos))
    
    # Agregar tests de eliminación
    test_suite.addTest(unittest.makeSuite(TestEliminacionDatos))
    
    # Agregar tests de chatbot
    test_suite.addTest(unittest.makeSuite(TestChatbotIA))
    
    # Agregar tests de technical note
    test_suite.addTest(unittest.makeSuite(TestTechnicalNote))
    test_suite.addTest(unittest.makeSuite(TestFiltrosGeograficos))
    test_suite.addTest(unittest.makeSuite(TestReporteTecnico))
    
    return test_suite

if __name__ == "__main__":
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite())
    
    # Retornar código de salida apropiado
    sys.exit(0 if result.wasSuccessful() else 1)
