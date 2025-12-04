
from dataclasses import dataclass
from typing import Tuple, List

@dataclass(frozen=True)
class KeywordRule:
    name: str
    synonyms: Tuple[str, ...]

DEFAULT_KEYWORDS: List[KeywordRule] = [
    KeywordRule(name="medicina", synonyms=("medicina", "médica", "médico", "medical")),
    KeywordRule(name="enfermeria", synonyms=("enfermeria", "enfermería", "enfermero", "nurse")),
    KeywordRule(name="odontologia", synonyms=("odontologia","odontológica", "odontológico", "dental", "dentista", "odontolígica")),
    KeywordRule(name="fluor", synonyms=("fluor", "fluoruro", "flúor", "barniz")),
    KeywordRule(name="placa", synonyms=("placa", "bacteriana", "bacteriano")),
    KeywordRule(name="detartraje", synonyms=("detartraje", "detartrajear", "detartrajeo", "detartrajes","Detartraje")),
    KeywordRule(name="sellantes", synonyms=("sellante", "selladores", "sellador", "Sellantes")),
    KeywordRule(name="micronutrientes", synonyms=("micronutrientes", "micronu", "micronutrientes")),
    KeywordRule(name="vitamina_a", synonyms=("Vitamina A",  " Vit A")),
    KeywordRule(name="sulfato_ferroso", synonyms=("Sulfato ferroso", "sulfato_ferroso", "sulfato ferroso", "ferroso")),
    KeywordRule(name="diu", synonyms=("")),
    KeywordRule(name="subdermico", synonyms=(""))
]