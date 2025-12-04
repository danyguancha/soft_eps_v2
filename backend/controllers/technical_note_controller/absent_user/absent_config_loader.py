import json
import os
from typing import Dict


class AbsentConfigLoader:
    """Carga configuraciones de rangos de fecha para inasistentes"""
    
    @staticmethod
    def load_absent_date_ranges() -> Dict[str, Dict[int, tuple]]:
        """Carga rangos de fechas para cálculo de inasistentes"""
        try:
            config_dir = os.path.join(
                os.path.dirname(__file__), '..', '..', '..', 'config'
            )
            json_path = os.path.join(config_dir, 'birth_date_ranges_den.json')
            
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            absent_ranges = {}
            for edad_key, meses_dict in data.items():
                absent_ranges[edad_key] = {
                    int(mes): tuple(fechas) 
                    for mes, fechas in meses_dict.items()
                }
            
            print(f"Rangos de inasistentes cargados: {len(absent_ranges)} grupos de edad")
            return absent_ranges
        
        except FileNotFoundError:
            print("No se encontró birth_date_ranges_den.json, usando dict vacío")
            return {}
        
        except Exception as e:
            print(f"Error cargando birth_date_ranges_den.json: {e}")
            return {}
