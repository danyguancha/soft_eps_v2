import unittest
from datetime import datetime

def validate_date_format(fecha):
    try:
        datetime.strptime(fecha, "%Y-%m-%d")
        return True
    except Exception:
        return False

def validate_file_extension(filename, allowed):
    return any(filename.endswith(x) for x in allowed)

def validate_age_range(edad, min_age, max_age):
    return min_age <= edad <= max_age

class TestValidators(unittest.TestCase):
    def test_VL_01_validate_date_format_ok(self):
        self.assertTrue(validate_date_format("2025-07-31"))

    def test_VL_02_validate_date_format_bad(self):
        self.assertFalse(validate_date_format("31/07/2025"))

    def test_VL_03_validate_file_extension_csv(self):
        self.assertTrue(validate_file_extension("datos.csv", [".csv", ".xlsx"]))

    def test_VL_04_validate_file_extension_not_allowed(self):
        self.assertFalse(validate_file_extension("script.exe", [".csv", ".xlsx"]))

    def test_VL_05_validate_age_range(self):
        self.assertTrue(validate_age_range(25, 18, 65))

if __name__ == "__main__":
    unittest.main()
