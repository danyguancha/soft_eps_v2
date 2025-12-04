import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd

class EdadRangeManager:
    """
    Gestor de rangos de edad para filtrado por años.
    """
    
    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = Path(__file__).parent.parent / 'config' / 'age_ranges.json'
        
        self.config_path = Path(config_path)
        self.rangos_meses = []
        self.rangos_anios = []
        self.rangos_compuestos = []
        self._load_config()
        self._normalize_labels()
    
    def _load_config(self):
        """Cargar configuración de rangos de edad."""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            self.rangos_meses = config.get('rangos_meses', [])
            self.rangos_anios = config.get('rangos_anios', [])
            self.rangos_compuestos = config.get('rangos_compuestos', [])
            
            print(f"✓ Rangos de edad cargados:")
            print(f"   - {len(self.rangos_meses)} rangos en meses")
            print(f"   - {len(self.rangos_anios)} rangos en años")
            print(f"   - {len(self.rangos_compuestos)} rangos compuestos")
            
        except Exception as e:
            print(f"❌ Error cargando rangos de edad: {e}")
            raise
    
    def _normalize_labels(self):
        """Crear versiones normalizadas de las etiquetas para matching."""
        self.normalized_map = {}
        
        # Normalizar rangos de meses
        for rango in self.rangos_meses:
            label = rango['label']
            normalized = self._normalize_label(label)
            self.normalized_map[normalized] = {
                'tipo': 'meses',
                'data': rango,
                'original_label': label
            }
        
        # Normalizar rangos de años
        for rango in self.rangos_anios:
            label = rango['label']
            normalized = self._normalize_label(label)
            self.normalized_map[normalized] = {
                'tipo': 'anios',
                'data': rango,
                'original_label': label
            }
        
        # Normalizar rangos compuestos
        for rango in self.rangos_compuestos:
            label = rango['label']
            normalized = self._normalize_label(label)
            self.normalized_map[normalized] = {
                'tipo': 'compuesto',
                'data': rango,
                'original_label': label
            }
    
    def _normalize_label(self, label: str) -> str:
        """Normalizar etiqueta para matching flexible."""
        import re
        # Convertir a mayúsculas
        normalized = label.upper()
        # Remover acentos
        normalized = normalized.replace('Á', 'A').replace('É', 'E').replace('Í', 'I')
        normalized = normalized.replace('Ó', 'O').replace('Ú', 'U').replace('Ñ', 'N')
        # Normalizar espacios
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        return normalized
    
    def find_range_by_label(self, label: str) -> Optional[Dict]:
        """
        Buscar un rango por su etiqueta (con normalización flexible).
        """
        normalized = self._normalize_label(label)
        return self.normalized_map.get(normalized)
    
    def get_edad_filter_for_range(self, label: str) -> Optional[Tuple[str, List[int]]]:
        """
        Obtener criterio de filtrado para un rango de edad específico.
        
        Returns:
            Tupla (tipo, valores) donde:
            - tipo: 'anios_exactos', 'anios_lista', 'meses_rango', etc.
            - valores: lista de valores a filtrar
        """
        rango_info = self.find_range_by_label(label)
        
        if not rango_info:
            print(f"⚠️  No se encontró rango para: {label}")
            return None
        
        tipo = rango_info['tipo']
        data = rango_info['data']
        
        if tipo == 'anios':
            # Rango simple de años (ej: "5 Años" -> edad == 5)
            return ('anios_exactos', [data['anios']])
        
        elif tipo == 'compuesto':
            # Rango compuesto (puede incluir meses y/o años)
            valores = []
            
            # Si incluye años
            if 'incluye_anios' in data:
                valores.extend(data['incluye_anios'])
            
            # Si incluye meses (convertir a edad 0 con condición especial)
            if 'incluye_meses' in data:
                # Para meses, necesitaremos filtro especial
                return ('meses_y_anios', {
                    'meses': data.get('incluye_meses', []),
                    'anios': data.get('incluye_anios', [])
                })
            
            return ('anios_lista', valores)
        
        elif tipo == 'meses':
            # Rango de meses (ej: "4 a 5 Meses")
            return ('meses_rango', {
                'min': data['min_meses'],
                'max': data['max_meses']
            })
        
        return None
    
    def filter_dataframe_by_edad(
        self, 
        df: pd.DataFrame, 
        label: str,
        edad_col: str = 'Edad'
    ) -> pd.DataFrame:
        """
        Filtrar DataFrame por rango de edad usando la columna 'Edad'.
        
        Args:
            df: DataFrame a filtrar
            label: Etiqueta del rango de edad (ej: "5 Años", "12 a 17 Años")
            edad_col: Nombre de la columna de edad
        
        Returns:
            DataFrame filtrado
        """
        filter_info = self.get_edad_filter_for_range(label)
        
        if not filter_info:
            print(f"❌ No se pudo obtener filtro para: {label}")
            return pd.DataFrame()
        
        tipo_filtro, valores = filter_info
        
        # Verificar que la columna existe
        if edad_col not in df.columns:
            print(f"❌ Columna '{edad_col}' no encontrada en el dataset")
            print(f"   Columnas disponibles: {df.columns.tolist()[:10]}...")
            return pd.DataFrame()
        
        try:
            if tipo_filtro == 'anios_exactos':
                # Filtrar por edad exacta
                edad = valores[0]
                mask = df[edad_col] == edad
                filtered_df = df[mask].copy()
                print(f"   ✓ Filtrado por edad exacta: {edad} años -> {len(filtered_df)} registros")
                return filtered_df
            
            elif tipo_filtro == 'anios_lista':
                # Filtrar por lista de edades
                mask = df[edad_col].isin(valores)
                filtered_df = df[mask].copy()
                print(f"   ✓ Filtrado por edades: {min(valores)}-{max(valores)} años -> {len(filtered_df)} registros")
                return filtered_df
            
            elif tipo_filtro == 'meses_rango':
                # Para meses, edad debe ser 0 (menor de 1 año)
                # Aquí necesitarías columna de meses o fecha de nacimiento
                print(f"   ⚠️  Filtro por meses requiere lógica especial (edad < 1)")
                mask = df[edad_col] == 0
                filtered_df = df[mask].copy()
                return filtered_df
            
            elif tipo_filtro == 'meses_y_anios':
                # Combinación de meses y años
                meses = valores.get('meses', [])
                anios = valores.get('anios', [])
                
                # Filtrar por años
                mask = df[edad_col].isin(anios)
                # TODO: Agregar lógica para meses si tienes columna de meses
                
                filtered_df = df[mask].copy()
                print(f"   ✓ Filtrado compuesto -> {len(filtered_df)} registros")
                return filtered_df
            
            else:
                print(f"❌ Tipo de filtro no soportado: {tipo_filtro}")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"❌ Error al filtrar: {e}")
            return pd.DataFrame()
    
    def get_all_labels(self) -> List[str]:
        """Obtener todas las etiquetas disponibles."""
        labels = []
        labels.extend([r['label'] for r in self.rangos_meses])
        labels.extend([r['label'] for r in self.rangos_anios])
        labels.extend([r['label'] for r in self.rangos_compuestos])
        return labels
    
    def get_labels_by_type(self, tipo: str) -> List[str]:
        """Obtener etiquetas de un tipo específico."""
        if tipo == 'meses':
            return [r['label'] for r in self.rangos_meses]
        elif tipo == 'anios':
            return [r['label'] for r in self.rangos_anios]
        elif tipo == 'compuesto':
            return [r['label'] for r in self.rangos_compuestos]
        return []


# Instancia global
_edad_manager = None

def get_edad_manager() -> EdadRangeManager:
    """Obtener instancia singleton del gestor de rangos de edad."""
    global _edad_manager
    if _edad_manager is None:
        _edad_manager = EdadRangeManager()
    return _edad_manager
