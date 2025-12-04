from typing import Dict, Any, List
import re


class ReportSorter:
    """Responsable de ordenar items del reporte"""
    
    @staticmethod
    def sort_items_by_age(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Ordena items por edad siguiendo el orden natural:
        - Primero meses (0-11)
        - Luego años (1, 2, 3, ...)
        - Finalmente rangos compuestos
        """
        
        def extract_age_value(rango_edad: str) -> tuple:
            """
            Extrae valor numérico de edad para ordenamiento.
            Retorna: (tipo, valor_minimo)
            tipo: 0=meses, 1=años, 999=sin clasificar
            """
            if not rango_edad:
                return (999, 999)
            
            rango_lower = rango_edad.lower().strip()
            rango_sin_tildes = rango_lower.replace('ñ', 'n')
            
            if 'mes' in rango_sin_tildes:
                match = re.search(r'(\d+)\s*a?\s*(\d*)\s*mes', rango_sin_tildes)
                if match:
                    valor_min = int(match.group(1))
                    return (0, valor_min)
                
                match = re.search(r'(\d+)', rango_sin_tildes)
                if match:
                    return (0, int(match.group(1)))
            
            if 'ano' in rango_sin_tildes or 'año' in rango_lower:
                match = re.search(r'(\d+)\s*a?\s*(\d*)\s*a[nñ]o', rango_sin_tildes)
                if match:
                    valor_min = int(match.group(1))
                    return (1, valor_min)
                
                match = re.search(r'(\d+)', rango_sin_tildes)
                if match:
                    return (1, int(match.group(1)))
            
            match = re.search(r'(\d+)\s*a\s*(\d+)', rango_sin_tildes)
            if match:
                valor_min = int(match.group(1))
                if 'mes' in rango_sin_tildes:
                    return (0, valor_min)
                if 'ano' in rango_sin_tildes or 'año' in rango_lower:
                    return (1, valor_min)
                return (1, valor_min)
            
            return (999, 999)
        
        try:
            sorted_items = sorted(
                items,
                key=lambda item: extract_age_value(item.get('rango_edad', ''))
            )
            
            print("\nOrden de items por edad:")
            for idx, item in enumerate(sorted_items[:10], 1):
                rango = item.get('rango_edad', 'Sin edad')
                tipo, valor = extract_age_value(rango)
                tipo_str = {0: "meses", 1: "años", 999: "sin clasificar"}.get(tipo, "?")
                print(f"   [{idx}] {rango} -> ({tipo_str}, valor={valor})")
            
            if len(sorted_items) > 10:
                print(f"   ... ({len(sorted_items) - 10} items más)")
            
            return sorted_items
        
        except Exception as e:
            print(f"Error ordenando items: {e}")
            import traceback
            traceback.print_exc()
            return items
