import json
import os
from typing import Dict, Any
from utils.text_normalizer import normalize_text


class ConfigLoader:
    """Responsable de cargar configuraciones desde archivos JSON"""
    
    @staticmethod
    def load_birth_date_ranges_den() -> Dict[str, Dict[int, tuple]]:
        """Carga rangos de fechas de nacimiento desde archivo JSON"""
        try:
            config_dir = os.path.join(
                os.path.dirname(__file__), '..', '..', '..', 'config'
            )
            json_path = os.path.join(config_dir, 'birth_date_ranges_den.json')
            
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            birth_date_ranges_den = {}
            for edad_key, meses_dict in data.items():
                birth_date_ranges_den[edad_key] = {
                    int(mes): tuple(fechas) 
                    for mes, fechas in meses_dict.items()
                }
            
            print(f"Rangos de fechas cargados: {len(birth_date_ranges_den)} grupos de edad")
            return birth_date_ranges_den
        
        except FileNotFoundError:
            print("No se encontró birth_date_ranges_den.json, usando dict vacío")
            return {}
        
        except Exception as e:
            print(f"Error cargando birth_date_ranges_den.json: {e}")
            return {}
    
    @staticmethod
    def load_rangos_compuestos() -> Dict[str, Any]:
        """Carga rangos compuestos desde age_ranges.json"""
        try:
            config_dir = os.path.join(
                os.path.dirname(__file__), '..', '..', '..', 'config'
            )
            json_path = os.path.join(config_dir, 'age_ranges.json')
            
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            rangos_dict = {}
            for rango in data.get('rangos_compuestos', []):
                label_norm = normalize_text(rango['label'])
                rangos_dict[label_norm] = rango
                rangos_dict[rango['label']] = rango
            
            print(f"Rangos compuestos cargados: {len(data.get('rangos_compuestos', []))} rangos")
            return rangos_dict
        
        except Exception as e:
            print(f"Error cargando rangos compuestos: {e}")
            return {}
