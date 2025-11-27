# utils/mapping_loader.py

import json
import os
from typing import Dict, Any, Optional
from utils.text_normalizer import normalize_text


class MappingLoader:
    """Cargador dinámico de mappings por curso de vida"""
    
    # Mapeo de nombres de archivo a curso de vida
    FILENAME_TO_CURSO_VIDA = {
        'primerainfancianueva': 'primera_infancia',
        'primera_infancia': 'primera_infancia',
        'infancianueva': 'infancia',
        'infancia': 'infancia',
        'adolescencianueva': 'adolescencia',
        'adolescencia': 'adolescencia',
        'juventudnueva': 'juventud',
        'juventud': 'juventud',
        'adulteznueva': 'adultez',
        'adultez': 'adultez',
        'vejeznueva': 'vejez',
        'vejez': 'vejez'
    }
    
    # Caché de mappings cargados
    _cache: Dict[str, Dict[str, Any]] = {}
    
    @classmethod
    def detect_curso_vida_from_filename(cls, filename: str) -> Optional[str]:
        """
        Detecta el curso de vida basado en el nombre del archivo del usuario
        
        Args:
            filename: Nombre del archivo (ej: 'PrimeraInfanciaNueva.csv')
        
        Returns:
            curso_vida detectado o None
        """
        if not filename:
            return None
        
        # Normalizar filename
        filename_norm = normalize_text(filename.replace('.csv', '').replace('.parquet', ''))
        
        # Buscar coincidencia
        for pattern, curso_vida in cls.FILENAME_TO_CURSO_VIDA.items():
            pattern_norm = normalize_text(pattern)
            if pattern_norm in filename_norm:
                print(f"   🎯 Curso de vida detectado: '{curso_vida}' (desde '{filename}')")
                return curso_vida
        
        print(f"   ⚠️ No se pudo detectar curso de vida desde: '{filename}'")
        return None
    
    @classmethod
    def load_mappings_for_curso_vida(cls, curso_vida: str) -> Dict[str, Any]:
        """
        Carga mappings para un curso de vida específico
        
        Args:
            curso_vida: Nombre del curso de vida (primera_infancia, infancia, etc.)
        
        Returns:
            Dict con mappings cargados
        """
        # Verificar caché
        if curso_vida in cls._cache:
            print(f"   📦 Mappings cargados desde caché: {curso_vida}")
            return cls._cache[curso_vida]
        
        # Construir ruta del archivo
        config_dir = os.path.join(os.path.dirname(__file__), '..', 'config', 'column_mappings')
        json_path = os.path.join(config_dir, f'{curso_vida}.json')
        
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            print(f"   ✅ Mappings cargados: {curso_vida}.json ({len(data.get('mappings', []))} mappings)")
            
            # Guardar en caché
            cls._cache[curso_vida] = data
            
            return data
        
        except FileNotFoundError:
            print(f"   ❌ No se encontró archivo: {json_path}")
            return {'mappings': []}
        
        except Exception as e:
            print(f"   ❌ Error cargando {curso_vida}.json: {e}")
            return {'mappings': []}
    
    @classmethod
    def load_mappings_for_filename(cls, filename: str) -> Dict[str, Any]:
        """
        Carga mappings detectando automáticamente el curso de vida desde el filename
        
        Args:
            filename: Nombre del archivo del usuario
        
        Returns:
            Dict con mappings o dict vacío si no se detecta curso de vida
        """
        curso_vida = cls.detect_curso_vida_from_filename(filename)
        
        if not curso_vida:
            print(f"   ⚠️ Intentando cargar mapping genérico (column_mappings.json)")
            return cls.load_legacy_mappings()
        
        return cls.load_mappings_for_curso_vida(curso_vida)
    
    @classmethod
    def load_legacy_mappings(cls) -> Dict[str, Any]:
        """
        Carga el archivo legacy column_mappings.json (fallback)
        
        Returns:
            Dict con mappings del archivo legacy
        """
        config_dir = os.path.join(os.path.dirname(__file__), '..', 'config')
        json_path = os.path.join(config_dir, 'column_mappings.json')
        
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            print(f"   ✅ Mappings legacy cargados: column_mappings.json ({len(data.get('mappings', []))} mappings)")
            return data
        
        except Exception as e:
            print(f"   ❌ Error cargando column_mappings.json: {e}")
            return {'mappings': []}
    
    @classmethod
    def load_all_mappings(cls) -> Dict[str, Any]:
        """
        Carga TODOS los mappings de todos los cursos de vida (útil para reportes generales)
        
        Returns:
            Dict con todos los mappings combinados
        """
        cursos_vida = [
            'primera_infancia',
            'infancia',
            'adolescencia',
            'juventud',
            'adultez',
            'vejez'
        ]
        
        all_mappings = []
        
        for curso_vida in cursos_vida:
            data = cls.load_mappings_for_curso_vida(curso_vida)
            all_mappings.extend(data.get('mappings', []))
        
        print(f"   ✅ Total mappings combinados: {len(all_mappings)}")
        
        return {'mappings': all_mappings}
    
    @classmethod
    def clear_cache(cls):
        """Limpia el caché de mappings"""
        cls._cache.clear()
        print("   🗑️ Caché de mappings limpiado")


# Instancia global
mapping_loader = MappingLoader()
