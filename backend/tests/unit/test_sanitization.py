import unittest
import hashlib
import re

def sanitize_table_name(nombre):
    # Reemplaza espacios/puntos por guion bajo
    return re.sub(r"[\s\.]+", "_", nombre)

def get_file_extension(filename):
    return '.' + filename.split('.')[-1] if '.' in filename else ''

def validate_file_size(size_bytes, max_size):
    return size_bytes <= max_size

def generate_file_hash(content):
    return hashlib.md5(content).hexdigest()[:16]

class TestSanitization(unittest.TestCase):
    def test_ST_01_sanitize_table_name(self):
        nombre = "tabla con espacios.csv"
        resultado = sanitize_table_name(nombre)
        self.assertEqual(resultado, "tabla_con_espacios_csv")

    def test_ST_02_get_file_extension(self):
        filename = "datos.csv"
        ext = get_file_extension(filename)
        self.assertEqual(ext, ".csv")

    def test_ST_03_validate_file_size(self):
        is_valid = validate_file_size(6_000_000_000, 5_000_000_000)
        self.assertFalse(is_valid)

    def test_ST_04_generate_file_hash(self):
        content = b"test data"
        hash1 = generate_file_hash(content)
        hash2 = generate_file_hash(content)
        self.assertEqual(hash1, hash2)
        self.assertEqual(len(hash1), 16)

if __name__ == "__main__":
    unittest.main()
