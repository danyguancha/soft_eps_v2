# services/technical_note_services/report_service_aux/sex_filter_loader.py

import json
import os
from typing import Dict, List, Optional
from utils.text_normalizer import normalize_text


class SexFilterConfig:
    """
    Carga y gestiona la configuración de filtros de sexo por keyword.
    
    Permite determinar dinámicamente qué keywords requieren filtro de sexo
    sin modificar el código fuente.
    """
    
    _instance = None
    _config = None
    
    def __new__(cls):
        """Singleton para cargar config una sola vez"""
        if cls._instance is None:
            cls._instance = super(SexFilterConfig, cls).__new__(cls)
            cls._instance._load_config()
        return cls._instance
    
    def _load_config(self):
        """Carga configuración desde archivo JSON"""
        config_path = os.path.join(
            os.path.dirname(__file__),
            '..',
            '..',
            '..',
            'config',
            'sex_filter_config.json'
        )
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self._config = json.load(f)
            
            # Normalizar todas las keywords al cargar
            self._normalize_keywords()
            
            print(f"✅ Configuración de filtros de sexo cargada:")
            print(f"   - Mujeres (F): {len(self._config['female_only']['keywords'])} keywords")
            print(f"   - Hombres (M): {len(self._config['male_only']['keywords'])} keywords")
            print(f"   - Sin filtro: {len(self._config['no_filter']['keywords'])} keywords")
        
        except FileNotFoundError:
            print(f"⚠️ No se encontró archivo de configuración: {config_path}")
            print("   Usando configuración por defecto")
            self._config = self._get_default_config()
        
        except Exception as e:
            print(f"⚠️ Error cargando configuración de filtros de sexo: {e}")
            self._config = self._get_default_config()
    
    def _normalize_keywords(self):
        """Normaliza todas las keywords para matching"""
        for category in ['female_only', 'male_only', 'no_filter']:
            if category in self._config:
                keywords = self._config[category]['keywords']
                self._config[category]['keywords_normalized'] = [
                    normalize_text(kw) for kw in keywords
                ]
    
    def _get_default_config(self) -> Dict:
        """Configuración por defecto si no existe el archivo"""
        return {
            "female_only": {
                "keywords": [
                    "tamizaje", "citologia", "adn-vph", "diu", "intrauterino",
                    "subdermico", "implante", "oral", "inyectable", "emergencia",
                    "valoracion_clinica_mama", "mama", "mamografia"
                ],
                "keywords_normalized": []
            },
            "male_only": {
                "keywords": [
                    "preservativo", "antigeno_prostatico", "prostata", "psa"
                ],
                "keywords_normalized": []
            },
            "no_filter": {
                "keywords": [
                    "esterilizacion", "no se suministra", "registro no evaluado"
                ],
                "keywords_normalized": []
            }
        }
    
    def get_sex_filter_for_keyword(self, keyword: str) -> Optional[str]:
        """
        Determina qué filtro de sexo aplicar para una keyword.
        
        Args:
            keyword: Keyword a evaluar
        
        Returns:
            'F' para mujeres, 'M' para hombres, None para sin filtro
        """
        if not keyword:
            return None
        
        keyword_norm = normalize_text(keyword)
        
        # Verificar si requiere filtro FEMENINO
        if self._matches_category(keyword_norm, 'female_only'):
            return 'F'
        
        # Verificar si requiere filtro MASCULINO
        if self._matches_category(keyword_norm, 'male_only'):
            return 'M'
        
        # Verificar si NO requiere filtro
        if self._matches_category(keyword_norm, 'no_filter'):
            return None
        
        # Por defecto: sin filtro
        return None
    
    def _matches_category(self, keyword_norm: str, category: str) -> bool:
        """
        Verifica si una keyword normalizada coincide con alguna de la categoría.
        
        Usa matching flexible:
        - Coincidencia exacta
        - Contención (uno dentro del otro)
        - Palabras compartidas
        """
        if category not in self._config:
            return False
        
        keywords_list = self._config[category].get('keywords_normalized', [])
        
        for config_kw in keywords_list:
            # Coincidencia exacta
            if keyword_norm == config_kw:
                return True
            
            # Contención
            if keyword_norm in config_kw or config_kw in keyword_norm:
                return True
            
            # Palabras compartidas (mínimo 1 palabra significativa)
            keyword_words = set(keyword_norm.split())
            config_words = set(config_kw.split())
            
            # Palabras genéricas a ignorar
            generic = {'DE', 'LA', 'EL', 'EN', 'Y', 'A', 'CON', 'POR'}
            
            common = (keyword_words & config_words) - generic
            
            if len(common) > 0:
                return True
        
        return False
    
    def is_female_only(self, keyword: str) -> bool:
        """Verifica si keyword requiere SOLO mujeres"""
        return self.get_sex_filter_for_keyword(keyword) == 'F'
    
    def is_male_only(self, keyword: str) -> bool:
        """Verifica si keyword requiere SOLO hombres"""
        return self.get_sex_filter_for_keyword(keyword) == 'M'
    
    def requires_sex_filter(self, keyword: str) -> bool:
        """Verifica si keyword requiere algún filtro de sexo"""
        return self.get_sex_filter_for_keyword(keyword) is not None
    
    def get_all_female_keywords(self) -> List[str]:
        """Obtiene todas las keywords para mujeres"""
        return self._config.get('female_only', {}).get('keywords', [])
    
    def get_all_male_keywords(self) -> List[str]:
        """Obtiene todas las keywords para hombres"""
        return self._config.get('male_only', {}).get('keywords', [])


# Instancia global (singleton)
sex_filter_config = SexFilterConfig()
