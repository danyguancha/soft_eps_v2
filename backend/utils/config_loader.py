# utils/config_loader.py

import json
import os
from typing import Dict, Any, Optional
from utils.mapping_loader import mapping_loader  # 🔥 NUEVO IMPORT


class ConfigLoader:
    """Carga configuraciones desde archivos JSON - Singleton con soporte para mappings dinámicos"""
    
    _instance = None
    _column_mappings = None
    _current_filename = None  # 🔥 NUEVO: Trackear el filename actual
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._initialized = True
            self._load_configs()
    
    def _load_configs(self):
        """Carga archivo de mapeo de columnas (legacy por defecto)"""
        try:
            base_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                'config'
            )
            
            mappings_path = os.path.join(base_path, 'column_mappings.json')
            
            print(f"🔍 Buscando mapeo legacy en: {mappings_path}")
            
            if os.path.exists(mappings_path):
                with open(mappings_path, 'r', encoding='utf-8') as f:
                    self._column_mappings = json.load(f)
                    print(f"✅ Mapeo legacy cargado: {len(self._column_mappings.get('mappings', []))} mapeos")
            else:
                print(f"⚠️ No se encontró {mappings_path}")
                
                # Debug info
                print(f"   📂 Contenido del directorio config:")
                if os.path.exists(base_path):
                    for item in os.listdir(base_path):
                        print(f"      - {item}")
                else:
                    print(f"      ✗ El directorio {base_path} no existe")
                
                self._column_mappings = {"mappings": []}
            
        except Exception as e:
            print(f"⚠️ Error cargando mapeo: {e}")
            import traceback
            traceback.print_exc()
            self._column_mappings = {"mappings": []}
    
    def get_column_mappings(self, filename: Optional[str] = None) -> Dict[str, Any]:
        """
        🔥 MEJORADO: Retorna mappings de columnas con soporte dinámico
        
        Args:
            filename: Nombre del archivo del usuario (opcional)
                     Si se proporciona, carga mappings específicos del curso de vida
        
        Returns:
            Dict con mappings
        """
        if filename:
            # 🔥 NUEVO: Carga dinámica por curso de vida
            print(f"📂 Cargando mappings dinámicos para: {filename}")
            self._current_filename = filename
            return mapping_loader.load_mappings_for_filename(filename)
        else:
            # Legacy: Retorna mappings cargados en __init__
            if not self._column_mappings or len(self._column_mappings.get('mappings', [])) == 0:
                print("⚠️ Mappings legacy vacíos, recargando...")
                self._load_configs()
            return self._column_mappings
    
    def get_column_mappings_for_curso_vida(self, curso_vida: str) -> Dict[str, Any]:
        """
        🔥 NUEVO: Carga mappings para un curso de vida específico
        
        Args:
            curso_vida: Nombre del curso de vida (primera_infancia, infancia, etc.)
        
        Returns:
            Dict con mappings del curso de vida
        """
        return mapping_loader.load_mappings_for_curso_vida(curso_vida)
    
    def find_mapping(self, columna_usuario: str, filename: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        🔥 MEJORADO: Encuentra mapping completo con soporte dinámico
        
        Args:
            columna_usuario: Nombre de la columna a buscar
            filename: Nombre del archivo (opcional, para carga dinámica)
        
        Returns:
            {
                "columna_usuario": str,
                "keyword": str,
                "consolidado": {
                    "consulta_procedimiento": str,
                    "edad_aplicable": str,
                    "edad_NT_RPMS": str
                }
            }
        """
        # Obtener mappings (dinámicos o legacy)
        mappings_data = self.get_column_mappings(filename)
        
        for mapping in mappings_data.get("mappings", []):
            if mapping["columna_usuario"].lower() == columna_usuario.lower():
                # Validar que tenga los campos necesarios
                consolidado = mapping.get("consolidado", {})
                if not consolidado.get("edad_NT_RPMS"):
                    print(f"⚠️ Mapping sin edad_NT_RPMS para: {columna_usuario}")
                
                print(f"✅ Mapping encontrado para '{columna_usuario}':")
                print(f"   → Consulta: {consolidado.get('consulta_procedimiento')}")
                print(f"   → Edad aplicable: {consolidado.get('edad_aplicable')}")
                print(f"   → Edad NT_RPMS: {consolidado.get('edad_NT_RPMS')}")
                
                return mapping
        
        print(f"⚠️ No se encontró mapping para: {columna_usuario}")
        return None
    
    def reload_mappings(self):
        """🔥 NUEVO: Recarga mappings (útil para desarrollo)"""
        print("🔄 Recargando mappings...")
        self._load_configs()
        mapping_loader.clear_cache()
        print("✅ Mappings recargados")
    
    def get_report_structure(self) -> Dict[str, Any]:
        """Retorna estructura de reporte (trimestres, semestres, anual)"""
        return {
            "trimestres": {
                "T1": {
                    "meses": ["enero", "febrero", "marzo"],
                    "col_numerador": "T1_numerador",
                    "col_denominador": "T1_denominador",
                    "col_porcentaje": "T1_porcentaje",
                    "col_semaforo": "T1_semaforo"
                },
                "T2": {
                    "meses": ["abril", "mayo", "junio"],
                    "col_numerador": "T2_numerador",
                    "col_denominador": "T2_denominador",
                    "col_porcentaje": "T2_porcentaje",
                    "col_semaforo": "T2_semaforo"
                },
                "T3": {
                    "meses": ["julio", "agosto", "septiembre"],
                    "col_numerador": "T3_numerador",
                    "col_denominador": "T3_denominador",
                    "col_porcentaje": "T3_porcentaje",
                    "col_semaforo": "T3_semaforo"
                },
                "T4": {
                    "meses": ["octubre", "noviembre", "diciembre"],
                    "col_numerador": "T4_numerador",
                    "col_denominador": "T4_denominador",
                    "col_porcentaje": "T4_porcentaje",
                    "col_semaforo": "T4_semaforo"
                }
            },
            "semestres": {
                "S1": {
                    "trimestres": ["T1", "T2"],
                    "col_numerador": "S1_numerador",
                    "col_denominador": "S1_denominador",
                    "col_porcentaje": "S1_porcentaje",
                    "col_semaforo": "S1_semaforo"
                },
                "S2": {
                    "trimestres": ["T3", "T4"],
                    "col_numerador": "S2_numerador",
                    "col_denominador": "S2_denominador",
                    "col_porcentaje": "S2_porcentaje",
                    "col_semaforo": "S2_semaforo"
                }
            },
            "anual": {
                "semestres": ["S1", "S2"],
                "col_numerador": "anual_numerador",
                "col_denominador": "anual_denominador",
                "col_porcentaje": "anual_porcentaje",
                "col_semaforo": "anual_semaforo"
            }
        }


# Instancia global
config_loader = ConfigLoader()
