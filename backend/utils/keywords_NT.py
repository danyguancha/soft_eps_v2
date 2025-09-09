
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
]