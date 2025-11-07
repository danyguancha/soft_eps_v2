import unittest

def escape_identifier(name):
    return f'"{name}"'

def build_where_clause(column, operator, value):
    col = escape_identifier(column)
    if operator == "equals":
        return f'{col} = {value}'
    if operator == "in":
        if isinstance(value, list):
            joined = ', '.join([repr(v) for v in value])
            return f'{col} IN ({joined})'
    raise ValueError('Operador no soportado')

def validate_operator(operator):
    ALLOWED = {"equals", "in"}
    if operator.lower() in ALLOWED:
        return True
    raise ValueError(f"Operador inválido: {operator}")

class TestSqlUtils(unittest.TestCase):
    def test_SQ_01_escape_identifier(self):
        name = "columna peligrosa"
        escaped = escape_identifier(name)
        self.assertEqual(escaped, '"columna peligrosa"')

    def test_SQ_02_build_where_equals(self):
        sql = build_where_clause("edad", "equals", 30)
        self.assertEqual(sql, '"edad" = 30')

    def test_SQ_03_build_where_in(self):
        sql = build_where_clause("departamento", "in", ["CALDAS", "ANTIOQUIA"])
        self.assertEqual(sql, '"departamento" IN (\'CALDAS\', \'ANTIOQUIA\')')

    def test_SQ_04_validate_operator(self):
        with self.assertRaises(ValueError):
            validate_operator("DROP TABLE")

if __name__ == "__main__":
    unittest.main()
