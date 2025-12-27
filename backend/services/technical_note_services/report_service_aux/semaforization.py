# services/technical_note_services/report_service_aux/semaforization.py

from typing import Dict


class Semaforization:
    """Clase responsable de calcular semaforización (estado y color) según numerador, denominador y porcentaje"""
    
    def calculate_semaforizacion(
        self, 
        numerador: int, 
        denominador: int,
        porcentaje: float
    ) -> Dict[str, str]:
        """
        🚦 FUNCIÓN DE SEMAFORIZACIÓN: Calcula estado y color según numerador, denominador y porcentaje
        
        Reglas:
        - Denominador = 0 → NA (Gris) - Sin importar numerador
        - Numerador = 0 y Denominador > 0 → Aplicar regla de porcentaje (0% = Muy Deficiente)
        - 0% ≤ % < 60% → Muy Deficiente (Rojo)
        - 60% ≤ % < 75% → Deficiente (Naranja)
        - 75% ≤ % < 90% → Aceptable (Amarillo)
        - % ≥ 90% → Óptimo (Verde)
        
        Args:
            numerador: Cantidad de casos registrados
            denominador: Población susceptible/objetivo
            porcentaje: Cobertura calculada (numerador/denominador * 100)
        
        Returns:
            Dict con estado, color, color_name, descripcion
        """
        try:
            # 🔥 CASO 1: Denominador = 0 → NA (Sin datos)
            if denominador == 0:
                return {
                    "estado": "NA",
                    "color": "#808080",  # Gris
                    "color_name": "gris",
                    "descripcion": "Sin denominador"
                }
            
            # 🔥 CASO 2: Denominador > 0 → Semaforización por porcentaje
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
                    "color": "#ffc107",  # Amarillo
                    "color_name": "amarillo",
                    "descripcion": "Buen desempeño"
                }
            elif porcentaje >= 60:
                return {
                    "estado": "Deficiente",
                    "color": "#fd7e14",  # Naranja
                    "color_name": "naranja",
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
