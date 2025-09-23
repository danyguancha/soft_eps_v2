

from typing import Dict


class Semaforization:
    def calculate_semaforizacion(self, numerador: int, porcentaje: float) -> Dict[str, str]:
        """
        🚦 FUNCIÓN DE SEMAFORIZACIÓN: Calcula estado y color según porcentaje
        
        Reglas:
        - Numerador = 0 → NA (Gris)
        - 0% ≤ % < 60% → Muy Deficiente (Rojo)
        - 60% ≤ % ≤ 74.9% → Deficiente (Amarillo Oscuro)
        - 75% ≤ % < 90% → Aceptable (Amarillo Claro)
        - % ≥ 90% → Óptimo (Verde)
        """
        try:
            # Caso especial: Numerador 0 = NA
            if numerador == 0:
                return {
                    "estado": "NA",
                    "color": "#808080",  # Gris
                    "color_name": "gris",
                    "descripcion": "Sin datos"
                }
            
            # Semaforización por porcentaje
            if porcentaje >= 90:
                return {
                    "estado": "Óptimo",
                    "color": "#28a745",  # Verde
                    "color_name": "verde",
                    "descripcion": "Excelente desempeño"
                }
            elif porcentaje >= 75:
                return {
                    "estado": "Aceptable", 
                    "color": "#ffc107",  # Amarillo Claro
                    "color_name": "amarillo_claro",
                    "descripcion": "Buen desempeño"
                }
            elif porcentaje >= 60:
                return {
                    "estado": "Deficiente",
                    "color": "#fd7e14",  # Amarillo Oscuro/Naranja
                    "color_name": "amarillo_oscuro",
                    "descripcion": "Desempeño bajo"
                }
            else:  # porcentaje < 60
                return {
                    "estado": "Muy Deficiente",
                    "color": "#ef1e1e",  # Rojo
                    "color_name": "rojo",
                    "descripcion": "Desempeño muy bajo"
                }
                
        except Exception as e:
            print(f"Error en semaforización: {e}")
            return {
                "estado": "Error",
                "color": "#6c757d",  # Gris oscuro
                "color_name": "gris_oscuro",
                "descripcion": "Error en cálculo"
            }