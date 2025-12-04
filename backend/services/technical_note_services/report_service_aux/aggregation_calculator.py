from typing import Dict, List


class AggregationCalculator:
    """Responsable de calcular agregaciones (trimestres, semestres, anual)"""
    
    @staticmethod
    def calculate_trimestres(
        mensual_data: Dict,
        meses_nombres: List[str],
        mes_limite: int
    ) -> Dict:
        """Calcula trimestres"""
        trimestres = {}
        trimestre_meses = {
            "T1": meses_nombres[0:3],
            "T2": meses_nombres[3:6],
            "T3": meses_nombres[6:9],
            "T4": meses_nombres[9:12]
        }
        
        for trim_key, meses in trimestre_meses.items():
            meses_validos = [m for m in meses if meses_nombres.index(m) + 1 <= mes_limite]
            
            if meses_validos:
                pob = sum(mensual_data[m]["poblacion_objeto"] for m in meses_validos)
                num = sum(mensual_data[m]["numerador"] for m in meses_validos)
                den = sum(mensual_data[m]["denominador"] for m in meses_validos)
                cob = (num / den * 100) if den > 0 else 0
                semaf, color = AggregationCalculator._calcular_semaforizacion(cob)
            else:
                pob = num = den = cob = 0
                semaf, color = "Sin datos", "#6c757d"
            
            trimestres[trim_key] = {
                "poblacion_objeto": pob,
                "numerador": num,
                "denominador": den,
                "cobertura": round(cob, 2),
                "semaforizacion": semaf,
                "color": color
            }
        
        return trimestres
    
    @staticmethod
    def calculate_semestres(trimestres: Dict) -> Dict:
        """Calcula semestres"""
        semestres = {}
        
        for sem_key, trims in [("S1", ["T1", "T2"]), ("S2", ["T3", "T4"])]:
            pob = sum(trimestres[t]["poblacion_objeto"] for t in trims)
            num = sum(trimestres[t]["numerador"] for t in trims)
            den = sum(trimestres[t]["denominador"] for t in trims)
            cob = (num / den * 100) if den > 0 else 0
            semaf, color = AggregationCalculator._calcular_semaforizacion(cob)
            
            semestres[sem_key] = {
                "poblacion_objeto": pob,
                "numerador": num,
                "denominador": den,
                "cobertura": round(cob, 2),
                "semaforizacion": semaf,
                "color": color
            }
        
        return semestres
    
    @staticmethod
    def calculate_anual(semestres: Dict) -> Dict:
        """Calcula anual"""
        pob = sum(semestres[s]["poblacion_objeto"] for s in ["S1", "S2"])
        num = sum(semestres[s]["numerador"] for s in ["S1", "S2"])
        den = sum(semestres[s]["denominador"] for s in ["S1", "S2"])
        cob = (num / den * 100) if den > 0 else 0
        semaf, color = AggregationCalculator._calcular_semaforizacion(cob)
        
        return {
            "poblacion_objeto": pob,
            "numerador": num,
            "denominador": den,
            "cobertura": round(cob, 2),
            "semaforizacion": semaf,
            "color": color
        }
    
    @staticmethod
    def _calcular_semaforizacion(cobertura: float) -> tuple:
        """Calcula semaforización"""
        if cobertura >= 95:
            return ('Óptimo', '#52c41a')
        elif cobertura >= 80:
            return ('Aceptable', '#faad14')
        elif cobertura >= 60:
            return ('Deficiente', '#fa8c16')
        else:
            return ('Muy Deficiente', '#ff4d4f')
