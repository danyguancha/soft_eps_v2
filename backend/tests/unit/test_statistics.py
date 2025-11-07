import unittest
import math


def calculate_mean(values):
    return sum(values) / len(values) if values else 0

def calculate_median(values):
    v = sorted(values)
    n = len(v)
    if n % 2:
        return v[n // 2]
    return (v[n // 2 - 1] + v[n // 2]) / 2

def calculate_percentile(values, percentile):
    v = sorted(values)
    k = (len(v) - 1) * (percentile / 100)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return v[int(k)]
    d0 = v[int(f)] * (c - k)
    d1 = v[int(c)] * (k - f)
    return d0 + d1

def calculate_std_dev(values):
    mean = calculate_mean(values)
    return (sum((x - mean)**2 for x in values) / (len(values)-1))**0.5

class TestStatistics(unittest.TestCase):
    def test_GE_01_calculate_mean(self):
        self.assertEqual(calculate_mean([10, 20, 30, 40, 50]), 30.0)

    def test_GE_02_calculate_median(self):
        self.assertEqual(calculate_median([1, 3, 5, 7, 9]), 5)

    def test_GE_03_calculate_percentile(self):
        # Se acepta con error flotante de dos decimales
        self.assertAlmostEqual(calculate_percentile(list(range(1, 101)), 95), 95.05, places=2)

    def test_GE_04_calculate_std_dev(self):
        result = calculate_std_dev([2, 4, 4, 4, 5, 5, 7, 9])
        self.assertAlmostEqual(result, 2.138089935299395, places=6)

if __name__ == "__main__":
    unittest.main()
