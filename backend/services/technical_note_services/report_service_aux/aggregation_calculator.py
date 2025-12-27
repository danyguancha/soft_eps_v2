# services/technical_note_services/report_service_aux/aggregation_calculator.py

from typing import Dict, List
from services.technical_note_services.report_service_aux.semaforization import Semaforization


class AggregationCalculator:
    """Responsable de calcular agregaciones (trimestres, semestres, anual) con semaforización"""
    
    def __init__(self):
        self.semaforization = Semaforization()
    
    def calculate_trimestres(
        self,
        mensual_data: Dict,
        meses_nombres: List[str],
        mes_limite: int
    ) -> Dict:
        """
        Calcula trimestres agregando datos mensuales.
        Si denominador = 0 → cobertura = 0.0, semaforizacion = "NA", color = gris
        """
        trimestres = {}
        trimestre_meses = {
            "trim1": meses_nombres[0:3],   # Enero, Febrero, Marzo
            "trim2": meses_nombres[3:6],   # Abril, Mayo, Junio
            "trim3": meses_nombres[6:9],   # Julio, Agosto, Septiembre
            "trim4": meses_nombres[9:12]   # Octubre, Noviembre, Diciembre
        }
        
        for trim_key, meses in trimestre_meses.items():
            # Filtrar meses válidos (dentro del límite)
            meses_validos = [m for m in meses if meses_nombres.index(m) + 1 <= mes_limite]
            
            if meses_validos:
                # Sumar datos de los meses válidos
                pob = sum(mensual_data[m].get("poblacion_objeto", 0) for m in meses_validos)
                num = sum(mensual_data[m].get("numerador", 0) for m in meses_validos)
                den = sum(mensual_data[m].get("denominador", 0) for m in meses_validos)
                
                # Calcular cobertura
                if den > 0:
                    cob = (num / den) * 100
                else:
                    cob = 0.0
                
                # Obtener semaforización
                semaf_result = self.semaforization.calculate_semaforizacion(num, den, cob)
                
            else:
                # Sin meses válidos → todo en 0
                pob = num = den = cob = 0
                semaf_result = {
                    "estado": "NA",
                    "color": "#808080",
                    "color_name": "gris",
                    "descripcion": "Sin datos"
                }
            
            trimestres[trim_key] = {
                "poblacion_objeto": pob,
                "numerador": num,
                "denominador": den,
                "cobertura": round(cob, 2),
                "semaforizacion": semaf_result["estado"],
                "color": semaf_result["color"]
            }
        
        return trimestres
    
    def calculate_semestres(self, trimestres: Dict) -> Dict:
        """
        Calcula semestres agregando trimestres.
        Si denominador = 0 → cobertura = 0.0, semaforizacion = "NA", color = gris
        """
        semestres = {}
        
        semestre_config = {
            "sem1": ["trim1", "trim2"],  # T1 + T2
            "sem2": ["trim3", "trim4"]   # T3 + T4
        }
        
        for sem_key, trims in semestre_config.items():
            # Sumar datos de los trimestres
            pob = sum(trimestres[t]["poblacion_objeto"] for t in trims)
            num = sum(trimestres[t]["numerador"] for t in trims)
            den = sum(trimestres[t]["denominador"] for t in trims)
            
            # Calcular cobertura
            if den > 0:
                cob = (num / den) * 100
            else:
                cob = 0.0
            
            # Obtener semaforización
            semaf_result = self.semaforization.calculate_semaforizacion(num, den, cob)
            
            semestres[sem_key] = {
                "poblacion_objeto": pob,
                "numerador": num,
                "denominador": den,
                "cobertura": round(cob, 2),
                "semaforizacion": semaf_result["estado"],
                "color": semaf_result["color"]
            }
        
        return semestres
    
    def calculate_anual(self, semestres: Dict) -> Dict:
        """
        Calcula anual agregando semestres.
        Si denominador = 0 → cobertura = 0.0, semaforizacion = "NA", color = gris
        """
        # Sumar datos de ambos semestres
        pob = sum(semestres[s]["poblacion_objeto"] for s in ["sem1", "sem2"])
        num = sum(semestres[s]["numerador"] for s in ["sem1", "sem2"])
        den = sum(semestres[s]["denominador"] for s in ["sem1", "sem2"])
        
        # Calcular cobertura
        if den > 0:
            cob = (num / den) * 100
        else:
            cob = 0.0
        
        # Obtener semaforización
        semaf_result = self.semaforization.calculate_semaforizacion(num, den, cob)
        
        return {
            "poblacion_objeto": pob,
            "numerador": num,
            "denominador": den,
            "cobertura": round(cob, 2),
            "semaforizacion": semaf_result["estado"],
            "color": semaf_result["color"]
        }
