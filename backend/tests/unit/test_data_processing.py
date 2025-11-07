import unittest
from datetime import datetime
import re

def calculate_age(fecha_nacimiento, fecha_corte):
    fecha1 = datetime.strptime(fecha_nacimiento, "%Y-%m-%d")
    fecha2 = datetime.strptime(fecha_corte, "%Y-%m-%d")
    return fecha2.year - fecha1.year - ((fecha2.month, fecha2.day) < (fecha1.month, fecha1.day))

def age_to_months(edad_años, edad_meses):
    return edad_años * 12 + edad_meses

def normalize_column_name(col):
    return re.sub(r'\s+', '_', col.strip().lower())

def detect_column_type(values):
    if all(isinstance(v, (int, float)) for v in values):
        return "numeric"
    return "categorical"

class TestDataProcessing(unittest.TestCase):
    def test_DP_01_calculate_age(self):
        edad = calculate_age("1990-01-01", "2025-01-01")
        self.assertEqual(edad, 35)

    def test_DP_02_age_to_months(self):
        total_meses = age_to_months(2, 6)
        self.assertEqual(total_meses, 30)

    def test_DP_03_normalize_column_name(self):
        normalized = normalize_column_name("Fecha de Nacimiento")
        self.assertEqual(normalized, "fecha_de_nacimiento")

    def test_DP_04_detect_column_type_numeric(self):
        tipo = detect_column_type([1, 2, 3, 4, 5])
        self.assertEqual(tipo, "numeric")

    def test_DP_05_detect_column_type_categorical(self):
        tipo = detect_column_type(["CALDAS", "ANTIOQUIA", "CALDAS", "META"])
        self.assertEqual(tipo, "categorical")

if __name__ == "__main__":
    unittest.main()
