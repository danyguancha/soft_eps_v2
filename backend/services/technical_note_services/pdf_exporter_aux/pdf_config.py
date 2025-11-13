# services/technical_note_services/report_service_aux/pdf_config.py
from typing import Dict
from reportlab.lib.units import inch

# Constantes de semaforización
SEMAFORO_OPTIMO = 'Óptimo'
SEMAFORO_ACEPTABLE = 'Aceptable'
SEMAFORO_DEFICIENTE = 'Deficiente'
SEMAFORO_MUY_DEFICIENTE = 'Muy Deficiente'
SEMAFORO_NA = 'NA'

# Mapa de colores
COLOR_MAP: Dict[str, str] = {
    SEMAFORO_OPTIMO: '#4CAF50',
    SEMAFORO_ACEPTABLE: '#FF9800',
    SEMAFORO_DEFICIENTE: '#FF5722',
    SEMAFORO_MUY_DEFICIENTE: '#F44336',
    SEMAFORO_NA: '#9E9E9E'
}

# Dimensiones y espaciados
DEFAULT_IMAGE_WIDTH = 2 * inch
DEFAULT_IMAGE_HEIGHT = 2 * inch
DEFAULT_WATERMARK_OPACITY = 0.1

# Posiciones de imagen
IMAGE_POSITION_CENTER = 'center'
IMAGE_POSITION_TOP_RIGHT = 'top-right'
IMAGE_POSITION_TOP_LEFT = 'top-left'
IMAGE_POSITION_BOTTOM_RIGHT = 'bottom-right'
IMAGE_POSITION_BOTTOM_LEFT = 'bottom-left'

# Umbrales de cobertura
UMBRAL_OPTIMO = 90
UMBRAL_ACEPTABLE = 75
UMBRAL_DEFICIENTE = 60

# Configuración por defecto
DEFAULT_CONFIG = {
    'title': 'Reporte de Indicadores',
    'subtitle': 'Análisis de Cobertura',
    'organization': '',
    'description': '',
    'methodology': '',
    'interpretation': {},
    'footer_text': '',
    'contact_info': ''
}
