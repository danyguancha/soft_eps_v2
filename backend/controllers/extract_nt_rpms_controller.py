import pandas as pd
import os
import re
from typing import List, Dict, Optional
from difflib import SequenceMatcher
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field


# ========== CONFIGURACIÓN DE HOJA ==========
@dataclass
class SheetConfig:
    """
    Configuración para la extracción de una hoja Excel específica.
    Permite definir el patrón de nombre de hoja y los números de fila
    donde se encuentran los encabezados y datos.
    """
    sheet_pattern: str
    header_row_general: int
    header_row_data: int
    data_start_row: int
    output_label: str


# Configuración por defecto para NT RPMS
NT_RPMS_CONFIG = SheetConfig(
    sheet_pattern=r"NT\s*RPMS",
    header_row_general=4,
    header_row_data=11,
    data_start_row=12,
    output_label="NT_RPMS"
)

# Configuración por defecto para NT RMPN
# Ajusta estos valores según la estructura real de la hoja NT_RMPN
NT_RMPN_CONFIG = SheetConfig(
    sheet_pattern=r"NT[_\s]*RMPN",
    header_row_general=8,
    header_row_data=19,
    data_start_row=20,
    output_label="NT_RMPN"
)


# ========== CACHÉ GLOBAL PARA DATOS GEOGRÁFICOS ==========
_GEOGRAPHIC_CACHE = {}


@lru_cache(maxsize=1)
def load_geographic_data_cached(file_path: str) -> pd.DataFrame:
    """Carga datos geográficos con caché en memoria (se ejecuta solo una vez)"""
    if file_path in _GEOGRAPHIC_CACHE:
        print(f"Usando datos geográficos desde caché")
        return _GEOGRAPHIC_CACHE[file_path].copy()

    if not os.path.exists(file_path):
        print(f"Archivo geográfico no encontrado: {file_path}")
        return pd.DataFrame()

    try:
        print(f"📍 Cargando datos geográficos: {file_path}")
        df = pd.read_excel(
            file_path,
            engine='openpyxl',
            dtype=str,
            na_filter=False
        )
        df.columns = df.columns.str.strip()
        _GEOGRAPHIC_CACHE[file_path] = df
        print(f"✓ Datos geográficos cargados y almacenados en caché")
        return df.copy()

    except Exception as e:
        print(f"Error cargando datos geográficos: {e}")
        return pd.DataFrame()


# ========== UTILIDADES BÁSICAS ==========
def find_target_sheet(sheet_names: List[str], pattern: str) -> Optional[str]:
    """Encuentra la hoja que coincide con el patrón regex dado."""
    for name in sheet_names:
        if re.search(pattern, name, re.IGNORECASE):
            return name
    return sheet_names[0] if sheet_names else None


def similarity_score(text1: str, text2: str) -> float:
    """Calcula similitud entre dos textos (0-1)."""
    if not text1 or not text2:
        return 0.0
    text1, text2 = str(text1).lower().strip(), str(text2).lower().strip()
    if text1 == text2:
        return 1.0
    if text1 in text2 or text2 in text1:
        return 0.95
    return SequenceMatcher(None, text1, text2).ratio()


# ========== NORMALIZADOR DE TEXTO ==========
class TextNormalizer:
    """Normaliza texto eliminando espacios múltiples y caracteres extraños."""

    @staticmethod
    def normalize(text: Optional[str]) -> Optional[str]:
        if pd.isna(text) or text is None:
            return None
        text_str = str(text).strip()
        if not text_str or text_str.lower() == 'nan':
            return None
        text_str = re.sub(r'\s+', ' ', text_str)
        text_str = re.sub(r'\s*-\s*', ' - ', text_str)
        text_str = re.sub(r'\s*\(\s*', '(', text_str)
        text_str = re.sub(r'\s*\)\s*', ')', text_str)
        text_str = re.sub(r'\s*,\s*', ', ', text_str)
        text_str = re.sub(r'\s*\.\s*', '. ', text_str)
        return text_str.strip()

    @classmethod
    def normalize_dataframe(cls, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        df = df.copy()
        for col in columns:
            if col in df.columns:
                df[col] = df[col].apply(cls.normalize)
                print(f"      ✓ Columna '{col}' normalizada")
        return df


# ========== MAPEO DE ENCABEZADOS ==========
class HeaderMapper:
    """Mapea nombres de encabezados a campos canónicos."""

    KEYWORDS = {
        'departamento': ['DPTO', 'dpto', 'departamento', 'depto'],
        'municipio': ['MUNICIPIO', 'municipio', 'mpio'],
        'nombre_ips': ['NOMBRE _ IPS', 'NOMBRE_IPS', 'nombre ips', 'ips', 'institucion'],
        'regimen': ['REGIMEN', 'regimen', 'régimen'],
        'proyeccion_tiempo': ['Proyeccion de Tiempo de NT', 'proyeccion', 'tiempo de nt'],
        'consultas_procedimientos': ['CONSULTAS / PROCEDIMIENTOS', 'consultas/procedimientos', 'consultas'],
        'servicios_habilitados': ['S_HABILITADOS', 's_habilitados', 'servicios habilitados', 'tipo de interven'],
        'frecuencia_edad': ['FRECUENCIA SEGÚN EDAD', 'frecuencia segun edad', 'frecuencia edad'],
        'cups': ['CUPS(AP,AC, MED, OTROS_S)', 'CUPS(AP.AC. MED. OTROS_S)', 'cups', 'código cups'],
        'frecuencia_indicada': ['FRECUENCIA INDICADA', 'frecuencia indicada'],
        'periodo': ['PERIODO', 'periodo', 'período'],
        'frecuencia_uso': ['FRECUENCIA  DE USO_IPS', 'FRECUENCIA DE USO_IPS', 'frecuencia de uso'],
        'frecuencia_ajustada': ['FRECUENCIA AJUSTADA', 'frecuencia ajustada'],
        'meta': ['%META', '%meta', 'meta'],
        'atenciones_realizar_anual': ['ATENCIONES A REALIZAR ANUAL', 'atenciones a realizar anual', 'atenciones a realizar'],
        'intervenciones_realizadas': ['INTERVENCIONES REALIZADAS HISTORICO', 'intervenciones realizadas', 'intervenciones_realizadas'],
    }

    @classmethod
    def find_column(cls, headers: List[str], field: str, used: set) -> Optional[int]:
        keywords = cls.KEYWORDS.get(field, [])
        best_idx, best_score = None, 0.5
        for idx, header in enumerate(headers):
            if idx in used or pd.isna(header) or not str(header).strip():
                continue
            header_str = str(header).strip()
            for keyword in keywords:
                if header_str.lower() == keyword.lower():
                    return idx
                score = similarity_score(keyword, header_str)
                if score > best_score:
                    best_score, best_idx = score, idx
        return best_idx

    @classmethod
    def find_all_columns(cls, headers: List[str], fields: List[str]) -> Dict[str, Optional[int]]:
        result, used = {}, set()
        for field in fields:
            idx = cls.find_column(headers, field, used)
            result[field] = idx
            if idx is not None:
                used.add(idx)
        return result


# ========== FORMATEO DE DATOS ==========
class DataFormatter:
    """Formatea valores según requisitos específicos."""

    @staticmethod
    def format_codigo(value, zfill_length: int = 3) -> str:
        if pd.isna(value):
            return ""
        str_val = str(value).strip().split('.')[0]
        return str_val.zfill(zfill_length) if str_val.isdigit() else str_val

    @staticmethod
    def to_numeric(value, decimals: int = 1):
        if pd.isna(value):
            return None
        try:
            return round(float(value), decimals)
        except (ValueError, TypeError):
            return value

    @classmethod
    def format_dataframe(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        if 'codigo_departamento' in df.columns:
            df['codigo_departamento'] = df['codigo_departamento'].apply(lambda x: cls.format_codigo(x, 2))
        if 'codigo_municipio' in df.columns:
            df['codigo_municipio'] = df['codigo_municipio'].apply(lambda x: cls.format_codigo(x, 3))
        for col in ['frecuencia_indicada', 'frecuencia_uso', 'frecuencia_ajustada', 'meta',
                    'atenciones_realizar_anual', 'intervenciones_realizadas']:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: cls.to_numeric(x, 1))
        print(f"   🔧 Normalizando campos de texto...")
        text_cols = ['nombre_ips', 'consultas_procedimientos', 'servicios_habilitados',
                     'frecuencia_edad', 'periodo', 'departamento', 'municipio']
        df = TextNormalizer.normalize_dataframe(df, text_cols)
        return df


# ========== ENRIQUECIMIENTO GEOGRÁFICO OPTIMIZADO ==========
class GeographicEnricher:
    """Enriquece códigos con nombres geográficos."""

    def __init__(self, departamentos_file: str):
        self.mapping = self._load_mapping(departamentos_file)

    def _load_mapping(self, file_path: str) -> pd.DataFrame:
        df = load_geographic_data_cached(file_path)
        if df.empty:
            return pd.DataFrame()
        try:
            col_map = {}
            for col in df.columns:
                col_lower = col.lower()
                if 'departamento' in col_lower and 'residencia' in col_lower:
                    col_map['dpto_nombre'] = col
                elif 'coddpto' in col_lower or ('cod' in col_lower and 'dpto' in col_lower):
                    col_map['dpto_codigo'] = col
                elif 'municipio' in col_lower and 'residencia' in col_lower:
                    col_map['mpio_nombre'] = col
                elif 'codmpio' in col_lower or ('cod' in col_lower and 'mpio' in col_lower):
                    col_map['mpio_codigo'] = col

            if len(col_map) < 4:
                print(f"Columnas insuficientes en archivo geográfico")
                return pd.DataFrame()

            mapping = pd.DataFrame({
                'codigo_departamento': df[col_map['dpto_codigo']].str.strip().str.zfill(2),
                'departamento': df[col_map['dpto_nombre']].str.strip(),
                'codigo_municipio': df[col_map['mpio_codigo']].str.strip().str.zfill(3),
                'municipio': df[col_map['mpio_nombre']].str.strip()
            })
            mapping['key'] = mapping['codigo_departamento'] + '_' + mapping['codigo_municipio']
            mapping = mapping.drop_duplicates(subset=['key'])
            print(f"✓ Preparados {len(mapping)} registros geográficos únicos")
            return mapping

        except Exception as e:
            print(f"Error procesando datos geográficos: {e}")
            return pd.DataFrame()

    def enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.mapping.empty:
            return df
        df = df.copy()
        df['codigo_municipio_normalizado'] = df.apply(
            lambda row: '001' if str(row.get('codigo_departamento', '')).strip().zfill(2) == '91'
                        and str(row.get('codigo_municipio', '')).strip().zfill(3) == '000'
                        else str(row.get('codigo_municipio', '')).strip().zfill(3),
            axis=1
        )
        df['key'] = (df['codigo_departamento'].astype(str).str.zfill(2) + '_' +
                     df['codigo_municipio_normalizado'].astype(str).str.zfill(3))
        df['codigo_municipio'] = df['codigo_municipio_normalizado']
        df = df.merge(self.mapping[['key', 'departamento', 'municipio']], on='key',
                      how='left', suffixes=('_codigo', '_nombre'))
        if 'departamento_nombre' in df.columns:
            df = df.rename(columns={
                'departamento_codigo': 'codigo_departamento',
                'departamento_nombre': 'departamento',
                'municipio_codigo': 'codigo_municipio',
                'municipio_nombre': 'municipio'
            })
        df = df.drop(columns=['key', 'codigo_municipio_normalizado'], errors='ignore')
        enriched = df['departamento'].notna().sum()
        print(f"✓ Enriquecidos: {enriched}/{len(df)} registros ({enriched/len(df)*100:.1f}%)")
        return df


# ========== EXTRACCIÓN DE DATOS ==========
class SheetExtractor:
    """Extrae datos de hoja Excel usando una SheetConfig configurable."""

    def __init__(self, file_path: str, config: SheetConfig):
        self.file_path = file_path
        self.config = config

    def extract(self) -> pd.DataFrame:
        """Extrae datos del archivo Excel según la configuración de hoja."""
        excel_file = pd.ExcelFile(self.file_path, engine='openpyxl')
        sheet_name = find_target_sheet(excel_file.sheet_names, self.config.sheet_pattern)

        if not sheet_name:
            raise ValueError(
                f"No se encontró hoja con patrón '{self.config.sheet_pattern}' en {self.file_path}"
            )

        print(f"      📄 Hoja encontrada: '{sheet_name}'")
        df_raw = excel_file.parse(sheet_name=sheet_name, header=None, dtype=str)

        general_data = self._extract_general_fields(df_raw)
        data_rows = self._extract_data_rows(df_raw, general_data)

        if not data_rows:
            raise ValueError(f"No se encontraron datos válidos en hoja '{sheet_name}'")

        return pd.DataFrame(data_rows)

    def _extract_general_fields(self, df: pd.DataFrame) -> Dict[str, str]:
        """Extrae campos generales usando la fila configurada."""
        row_idx = self.config.header_row_general - 1
        headers = df.iloc[row_idx].tolist()
        fields = ['departamento', 'municipio', 'nombre_ips', 'regimen', 'proyeccion_tiempo']
        columns = HeaderMapper.find_all_columns(headers, fields)
        value_row_idx = row_idx + 1

        return {
            'codigo_departamento': self._get_value(df, value_row_idx, columns.get('departamento')),
            'codigo_municipio': self._get_value(df, value_row_idx, columns.get('municipio')),
            'nombre_ips': self._get_value(df, value_row_idx, columns.get('nombre_ips')),
            'regimen': self._get_value(df, value_row_idx, columns.get('regimen')),
            'proyeccion_tiempo': self._get_value(df, value_row_idx, columns.get('proyeccion_tiempo')),
        }

    def _extract_data_rows(self, df: pd.DataFrame, general: Dict) -> List[Dict]:
        """Extrae filas de datos usando las filas configuradas."""
        row_idx = self.config.header_row_data - 1
        headers = df.iloc[row_idx].tolist()
        fields = [
            'consultas_procedimientos', 'servicios_habilitados', 'frecuencia_edad', 'cups',
            'frecuencia_indicada', 'periodo', 'frecuencia_uso', 'frecuencia_ajustada',
            'meta', 'atenciones_realizar_anual', 'intervenciones_realizadas'
        ]
        columns = HeaderMapper.find_all_columns(headers, fields)

        data_rows = []
        for r_idx in range(self.config.data_start_row - 1, len(df)):
            row = {
                **general,
                'consultas_procedimientos': self._get_value(df, r_idx, columns.get('consultas_procedimientos')),
                'servicios_habilitados': self._get_value(df, r_idx, columns.get('servicios_habilitados')),
                'frecuencia_edad': self._get_value(df, r_idx, columns.get('frecuencia_edad')),
                'cups': self._get_value(df, r_idx, columns.get('cups')),
                'frecuencia_indicada': self._get_numeric(df, r_idx, columns.get('frecuencia_indicada')),
                'periodo': self._get_value(df, r_idx, columns.get('periodo')),
                'frecuencia_uso': self._get_numeric(df, r_idx, columns.get('frecuencia_uso')),
                'frecuencia_ajustada': self._get_numeric(df, r_idx, columns.get('frecuencia_ajustada')),
                'meta': self._get_numeric(df, r_idx, columns.get('meta')),
                'atenciones_realizar_anual': self._get_numeric(df, r_idx, columns.get('atenciones_realizar_anual')),
                'intervenciones_realizadas': self._get_numeric(df, r_idx, columns.get('intervenciones_realizadas'))
            }
            if pd.notna(row['consultas_procedimientos']) or pd.notna(row['cups']):
                data_rows.append(row)
        return data_rows

    def _get_value(self, df: pd.DataFrame, row: int, col: Optional[int]) -> Optional[str]:
        if col is None or row >= len(df) or col >= len(df.columns):
            return None
        value = df.iloc[row, col]
        return None if pd.isna(value) or str(value).lower() == 'nan' else str(value).strip()

    def _get_numeric(self, df: pd.DataFrame, row: int, col: Optional[int]):
        value = self._get_value(df, row, col)
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return value


# ========== PROCESADOR PRINCIPAL ==========
class ExcelProcessor:
    """Orquesta el procesamiento de múltiples archivos con una SheetConfig dada."""

    COLUMN_ORDER = [
        'nombre_archivo', 'codigo_departamento', 'departamento', 'codigo_municipio',
        'municipio', 'nombre_ips', 'regimen', 'proyeccion_tiempo', 'consultas_procedimientos',
        'servicios_habilitados', 'frecuencia_edad', 'cups', 'frecuencia_indicada',
        'periodo', 'frecuencia_uso', 'frecuencia_ajustada', 'meta',
        'atenciones_realizar_anual', 'intervenciones_realizadas'
    ]

    def __init__(
        self,
        folder_path: str,
        config: SheetConfig,
        departamentos_file: Optional[str] = None,
        max_workers: Optional[int] = None
    ):
        self.folder_path = folder_path
        self.config = config
        self.results, self.errors = [], []
        self.enricher = None
        cpu_count = os.cpu_count() or 2
        default_workers = min(max(4, cpu_count - 1), 8)
        self.max_workers = max_workers or default_workers
        if departamentos_file and os.path.exists(departamentos_file):
            self.enricher = GeographicEnricher(departamentos_file)

    def _process_single_file(self, filename: str) -> Optional[pd.DataFrame]:
        file_path = os.path.join(self.folder_path, filename)
        try:
            df = SheetExtractor(file_path, self.config).extract()
            df.insert(0, 'nombre_archivo', filename)
            print(f"✓ {filename} - OK ({len(df)} registros)")
            return df
        except Exception as e:
            self.errors.append((filename, str(e)))
            print(f"✗ {filename} - Error: {str(e)}")
            return None

    def process_folder(self) -> pd.DataFrame:
        if not os.path.isdir(self.folder_path):
            raise FileNotFoundError(f"Carpeta no existe: {self.folder_path}")

        excel_files = [
            f for f in os.listdir(self.folder_path)
            if f.lower().endswith(('.xlsx', '.xls')) and not f.startswith('~$')
        ]
        if not excel_files:
            raise ValueError(f"No hay archivos Excel en: {self.folder_path}")

        print(f"🔄 [{self.config.output_label}] Procesando {len(excel_files)} archivos "
              f"con hasta {self.max_workers} hilos...")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._process_single_file, filename): filename
                for filename in sorted(excel_files)
            }
            for future in as_completed(futures):
                df = future.result()
                if df is not None:
                    self.results.append(df)

        if not self.results:
            raise ValueError(f"[{self.config.output_label}] No se procesó ningún archivo correctamente")

        combined = pd.concat(self.results, ignore_index=True)

        if self.enricher:
            combined = self.enricher.enrich(combined)

        combined = DataFormatter.format_dataframe(combined)
        combined = self._order_columns(combined)
        return combined

    def _order_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        available = [col for col in self.COLUMN_ORDER if col in df.columns]
        others = [col for col in df.columns if col not in self.COLUMN_ORDER]
        return df[available + others]

    def get_summary(self) -> Dict:
        return {
            "archivos_procesados": len(self.results),
            "archivos_con_errores": len(self.errors),
            "total_registros": sum(len(df) for df in self.results),
            "errores": self.errors
        }


# ========== FUNCIÓN GENÉRICA DE EXTRACCIÓN ==========
def _extract_sheet_to_csv(
    folder_path: str,
    output_csv_path: str,
    config: SheetConfig,
    separator: str = ';',
    departamentos_file: Optional[str] = None
) -> Dict:
    """
    Función base que extrae una hoja configurada a CSV.
    Usada internamente por extract_nt_rpms_to_csv y extract_nt_rmpn_to_csv.
    """
    processor = ExcelProcessor(folder_path, config, departamentos_file)
    combined_df = processor.process_folder()

    output_dir = os.path.dirname(output_csv_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    combined_df.to_csv(output_csv_path, index=False, sep=separator,
                       encoding='utf-8-sig', quoting=1)

    summary = processor.get_summary()
    print(f"   ✓ [{config.output_label}] CSV generado: {output_csv_path}")
    print(f"{'='*60}\n")

    return {
        "success": True,
        "csv_path": output_csv_path,
        "total_rows": len(combined_df),
        "total_columns": len(combined_df.columns),
        "column_order": combined_df.columns.tolist(),
        "summary": summary,
        "has_geographic_enrichment": departamentos_file is not None,
        "sheet_label": config.output_label
    }


# ========== FUNCIONES PÚBLICAS ==========
def extract_nt_rpms_to_csv(
    folder_path: str,
    output_csv_path: str,
    separator: str = ';',
    departamentos_file: Optional[str] = None,
    config: SheetConfig = NT_RPMS_CONFIG
) -> Dict:
    """Extrae información de la hoja NT RPMS y genera CSV normalizado."""
    print(f"\n{'='*60}")
    print(f"EXTRACCIÓN HOJA: {config.output_label}")
    print(f"{'='*60}")
    return _extract_sheet_to_csv(folder_path, output_csv_path, config, separator, departamentos_file)


def extract_nt_rmpn_to_csv(
    folder_path: str,
    output_csv_path: str,
    separator: str = ';',
    departamentos_file: Optional[str] = None,
    config: SheetConfig = NT_RMPN_CONFIG
) -> Dict:
    """Extrae información de la hoja NT RMPN y genera CSV normalizado."""
    print(f"\n{'='*60}")
    print(f"EXTRACCIÓN HOJA: {config.output_label}")
    print(f"{'='*60}")
    return _extract_sheet_to_csv(folder_path, output_csv_path, config, separator, departamentos_file)
