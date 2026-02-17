from dataclasses import dataclass
from typing import Tuple, List


@dataclass(frozen=True)
class KeywordRule:
    name: str
    synonyms: Tuple[str, ...]
    metodo_anticonceptivo: str = None


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
    
    # Keywords especiales - NO necesitan metodo_anticonceptivo aquí
    KeywordRule(name="diu", synonyms=("diu", "DIU", "dispositivo intrauterino")),
    KeywordRule(name="subdermico", synonyms=("subdermico", "subdérmico", "implante subdermico", "implante subdérmico")),
    KeywordRule(name="preservativo", synonyms=("preservativo", "condon", "condón", "preservativos", "condones")),
    KeywordRule(name="Asesoria Pre y Pos Test VIH", synonyms=("Asesoria Pre y Pos Test VIH", "Asesoría Pre y Pos Test VIH", "Asesoria Pre y Pos Test vih", "Asesoría Pre y Pos Test vih")),
    KeywordRule(name="Fecha de tamizaje para VIH", synonyms=("Fecha de tamizaje para VIH", "fecha de tamizaje para vih", "Fecha de tamizaje para vih", "fecha de tamizaje para VIH")),
    KeywordRule(name="Lactancia", synonyms=("Lactancia Materna", "lactancia", "Lactancia")),
    KeywordRule(name="Hepatitis_b", synonyms=("Hepatitis B", "hepatitis_b", "Hepatitis B")),
    KeywordRule(name="Hepatitis_c", synonyms=("Hepatitis C", "hepatitis_c", "Hepatitis C")),
    KeywordRule(name="Anemia", synonyms=("anemia", "anémica", "anémico", "anemia")),
    KeywordRule(name="Educacion individual", synonyms=("Educacion individual", "educación individual")),
    KeywordRule(name="Desparasitacion intestinal", synonyms=("Desparasitacion intestinal", "desparasitación intestinal", "desparasitacion", "desparasitación")),
    KeywordRule(name="citologia", synonyms=("citologia", "citología", "citología")),
    KeywordRule(name="ADN-VPH", synonyms=("ADN-VPH", "ADN VPH", "ADN-VPH", "ADN VPH")),


]


