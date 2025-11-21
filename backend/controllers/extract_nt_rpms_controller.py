# nt_rpms_extractor.py - Versión simplificada y optimizada
import pandas as pd
import os
import re
from typing import List, Dict, Optional, Tuple
from difflib import SequenceMatcher



# ========== UTILIDADES BÁSICAS ==========
def find_target_sheet(sheet_names: List[str]) -> Optional[str]:
    """Encuentra la hoja que contiene 'NT RPMS' o retorna la primera."""
    for name in sheet_names:
        if re.search(r"NT\s*RPMS", name, re.IGNORECASE):
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


# ========== MAPEO DE ENCABEZADOS ==========
class HeaderMapper:
    """Mapea nombres de encabezados a campos canónicos."""
    
    KEYWORDS = {
        'departamento': ['DPTO', 'dpto', 'departamento', 'depto'],
        'municipio': ['MUNICIPIO', 'municipio', 'mpio'],
        'nombre_ips': ['NOMBRE _ IPS', 'NOMBRE_IPS', 'nombre ips', 'ips', 'institucion'],
        'proyeccion_tiempo': ['Proyeccion de Tiempo de NT', 'proyeccion', 'tiempo de nt'],
        'consultas_procedimientos': ['CONSULTAS / PROCEDIMIENTOS', 'consultas/procedimientos', 'consultas'],
        'servicios_habilitados': ['S_HABILITADOS', 's_habilitados', 'servicios habilitados', 'tipo de interven'],
        'frecuencia_edad': ['FRECUENCIA SEGÚN EDAD', 'frecuencia segun edad', 'frecuencia edad'],
        'cups': ['CUPS(AP,AC, MED, OTROS_S)', 'CUPS(AP.AC. MED. OTROS_S)', 'cups', 'código cups'],
        'frecuencia_indicada': ['FRECUENCIA INDICADA', 'frecuencia indicada'],
        'periodo': ['PERIODO', 'periodo', 'período'],
        'frecuencia_uso': ['FRECUENCIA  DE USO_IPS', 'FRECUENCIA DE USO_IPS', 'frecuencia de uso'],
        'frecuencia_ajustada': ['FRECUENCIA AJUSTADA', 'frecuencia ajustada'],
        'meta': ['%META', '%meta', 'meta']
    }
    
    @classmethod
    def find_column(cls, headers: List[str], field: str, used: set) -> Optional[int]:
        """Encuentra índice de columna por keywords con mejor score."""
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
        """Encuentra todas las columnas evitando duplicados."""
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
        """Formatea código preservando ceros iniciales."""
        if pd.isna(value):
            return ""
        
        str_val = str(value).strip().split('.')[0]  # Remover decimales
        return str_val.zfill(zfill_length) if str_val.isdigit() else str_val
    
    @staticmethod
    def to_numeric(value, decimals: int = 1):
        """Convierte a numérico con decimales especificados."""
        if pd.isna(value):
            return None
        try:
            return round(float(value), decimals)
        except (ValueError, TypeError):
            return value
    
    @classmethod
    def format_dataframe(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica formateo completo al DataFrame."""
        df = df.copy()
        
        # Formatear códigos geográficos
        if 'codigo_departamento' in df.columns:
            df['codigo_departamento'] = df['codigo_departamento'].apply(
                lambda x: cls.format_codigo(x, 2)
            )
        
        if 'codigo_municipio' in df.columns:
            df['codigo_municipio'] = df['codigo_municipio'].apply(
                lambda x: cls.format_codigo(x, 3)
            )
        
        # Formatear columnas numéricas
        numeric_cols = ['frecuencia_indicada', 'frecuencia_uso', 'frecuencia_ajustada', 'meta']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: cls.to_numeric(x, 1))
        
        return df


# ========== ENRIQUECIMIENTO GEOGRÁFICO ==========
class GeographicEnricher:
    """Enriquece códigos con nombres geográficos."""
    
    def __init__(self, departamentos_file: str):
        self.mapping = self._load_mapping(departamentos_file)
    
    def _load_mapping(self, file_path: str) -> pd.DataFrame:
        """Carga y prepara el mapeo geográfico."""
        if not os.path.exists(file_path):
            print(f"Archivo geográfico no encontrado: {file_path}")
            return pd.DataFrame()
        
        try:
            print(f"📍 Cargando datos geográficos: {file_path}")
            
            df = pd.read_excel(file_path, engine='openpyxl', dtype=str)
            df.columns = df.columns.str.strip()
            
            # Mapeo flexible de columnas
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
            
            # Crear DataFrame normalizado
            mapping = pd.DataFrame({
                'codigo_departamento': df[col_map['dpto_codigo']].str.strip().str.zfill(2),
                'departamento': df[col_map['dpto_nombre']].str.strip(),
                'codigo_municipio': df[col_map['mpio_codigo']].str.strip().str.zfill(3),
                'municipio': df[col_map['mpio_nombre']].str.strip()
            })
            
            mapping['key'] = mapping['codigo_departamento'] + '_' + mapping['codigo_municipio']
            mapping = mapping.drop_duplicates(subset=['key'])
            
            print(f"✓ Cargados {len(mapping)} registros geográficos únicos")
            return mapping
            
        except Exception as e:
            print(f"Error cargando datos geográficos: {e}")
            return pd.DataFrame()
    
    def enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enriquece DataFrame con nombres geográficos."""
        if self.mapping.empty:
            return df        
        df = df.copy()
        
        # **MODIFICACIÓN: Convertir 91000 a 91001 antes de la búsqueda**
        def normalize_municipio_code(codigo_dpto, codigo_mpio):
            """Normaliza código de municipio: convierte 91000 a 91001"""
            codigo_dpto_str = str(codigo_dpto).strip().zfill(2)
            codigo_mpio_str = str(codigo_mpio).strip().zfill(3)
            
            # Si el código completo es 91000, cambiarlo a 91001
            if codigo_dpto_str == '91' and codigo_mpio_str == '000':
                return '001'
            
            return codigo_mpio_str
        
        # Aplicar normalización antes de crear la clave
        df['codigo_municipio_normalizado'] = df.apply(
            lambda row: normalize_municipio_code(
                row.get('codigo_departamento', ''),
                row.get('codigo_municipio', '')
            ),
            axis=1
        )
        
        # Crear claves de búsqueda con código normalizado
        df['key'] = (
            df['codigo_departamento'].astype(str).str.zfill(2) + '_' +
            df['codigo_municipio_normalizado'].astype(str).str.zfill(3)
        )
        
        # Actualizar codigo_municipio con el valor normalizado
        df['codigo_municipio'] = df['codigo_municipio_normalizado']
        
        # Merge con mapping
        df = df.merge(
            self.mapping[['key', 'departamento', 'municipio']],
            on='key',
            how='left',
            suffixes=('_codigo', '_nombre')
        )
        
        # Renombrar columnas resultantes
        if 'departamento_nombre' in df.columns:
            df = df.rename(columns={
                'departamento_codigo': 'codigo_departamento',
                'departamento_nombre': 'departamento',
                'municipio_codigo': 'codigo_municipio',
                'municipio_nombre': 'municipio'
            })
        
        # Limpiar columnas temporales
        df = df.drop(columns=['key', 'codigo_municipio_normalizado'], errors='ignore')
        
        enriched = df['departamento'].notna().sum()
        print(f"✓ Enriquecidos: {enriched}/{len(df)} registros ({enriched/len(df)*100:.1f}%)")
        
        return df

# ========== EXTRACCIÓN DE DATOS ==========
class SheetExtractor:
    """Extrae datos de hoja Excel con estructura específica."""
    
    HEADER_ROW_GENERAL = 3  # Fila 3 (índice 2)
    HEADER_ROW_DATA = 10     # Fila 10 (índice 9)
    DATA_START_ROW = 11      # Fila 11 (índice 10)
    
    def __init__(self, file_path: str):
        self.file_path = file_path
    
    def extract(self) -> pd.DataFrame:
        """Extrae datos del archivo Excel."""
        excel_file = pd.ExcelFile(self.file_path, engine='openpyxl')
        sheet_name = find_target_sheet(excel_file.sheet_names)
        
        if not sheet_name:
            raise ValueError(f"No se encontró hoja válida en {self.file_path}")
        
        df_raw = pd.read_excel(
            self.file_path,
            sheet_name=sheet_name,
            header=None,
            engine='openpyxl',
            dtype=str
        )
        
        # Extraer campos generales (fila 3)
        general_data = self._extract_general_fields(df_raw)
        
        # Extraer datos tabulares (desde fila 10)
        data_rows = self._extract_data_rows(df_raw, general_data)
        
        if not data_rows:
            raise ValueError("No se encontraron datos válidos")
        
        return pd.DataFrame(data_rows)
    
    def _extract_general_fields(self, df: pd.DataFrame) -> Dict[str, str]:
        """Extrae campos generales de fila 3."""
        row_idx = self.HEADER_ROW_GENERAL - 1
        headers = df.iloc[row_idx].tolist()
        
        fields = ['departamento', 'municipio', 'nombre_ips', 'proyeccion_tiempo']
        columns = HeaderMapper.find_all_columns(headers, fields)
        
        value_row_idx = row_idx + 1
        
        return {
            'codigo_departamento': self._get_value(df, value_row_idx, columns.get('departamento')),
            'codigo_municipio': self._get_value(df, value_row_idx, columns.get('municipio')),
            'nombre_ips': self._get_value(df, value_row_idx, columns.get('nombre_ips')),
            'proyeccion_tiempo': self._get_value(df, value_row_idx, columns.get('proyeccion_tiempo'))
        }
    
    def _extract_data_rows(self, df: pd.DataFrame, general: Dict) -> List[Dict]:
        """Extrae filas de datos desde fila 10."""
        row_idx = self.HEADER_ROW_DATA - 1
        headers = df.iloc[row_idx].tolist()
        
        fields = [
            'consultas_procedimientos', 'servicios_habilitados', 'frecuencia_edad',
            'cups', 'frecuencia_indicada', 'periodo', 'frecuencia_uso',
            'frecuencia_ajustada', 'meta'
        ]
        columns = HeaderMapper.find_all_columns(headers, fields)
        
        data_rows = []
        start_idx = self.DATA_START_ROW - 1
        
        for row_idx in range(start_idx, len(df)):
            row = {
                **general,  # Agregar campos generales
                'consultas_procedimientos': self._get_value(df, row_idx, columns.get('consultas_procedimientos')),
                'servicios_habilitados': self._get_value(df, row_idx, columns.get('servicios_habilitados')),
                'frecuencia_edad': self._get_value(df, row_idx, columns.get('frecuencia_edad')),
                'cups': self._get_value(df, row_idx, columns.get('cups')),
                'frecuencia_indicada': self._get_numeric(df, row_idx, columns.get('frecuencia_indicada')),
                'periodo': self._get_value(df, row_idx, columns.get('periodo')),
                'frecuencia_uso': self._get_numeric(df, row_idx, columns.get('frecuencia_uso')),
                'frecuencia_ajustada': self._get_numeric(df, row_idx, columns.get('frecuencia_ajustada')),
                'meta': self._get_numeric(df, row_idx, columns.get('meta'))
            }
            
            # Solo agregar si tiene datos relevantes
            if pd.notna(row['consultas_procedimientos']) or pd.notna(row['cups']):
                data_rows.append(row)
        
        return data_rows
    
    def _get_value(self, df: pd.DataFrame, row: int, col: Optional[int]) -> Optional[str]:
        """Obtiene valor como texto."""
        if col is None or row >= len(df) or col >= len(df.columns):
            return None
        
        value = df.iloc[row, col]
        
        if pd.isna(value) or str(value).lower() == 'nan':
            return None
        
        return str(value).strip()
    
    def _get_numeric(self, df: pd.DataFrame, row: int, col: Optional[int]):
        """Obtiene valor numérico."""
        value = self._get_value(df, row, col)
        if value is None:
            return None
        
        try:
            return float(value)
        except (ValueError, TypeError):
            return value

# ========== PROCESADOR PRINCIPAL ==========
class ExcelProcessor:
    """Orquesta el procesamiento de múltiples archivos."""
    
    # Orden final de columnas
    COLUMN_ORDER = [
        'nombre_archivo',
        'codigo_departamento',
        'departamento',
        'codigo_municipio',
        'municipio',
        'nombre_ips',
        'proyeccion_tiempo',
        'consultas_procedimientos',
        'servicios_habilitados',
        'frecuencia_edad',
        'cups',
        'frecuencia_indicada',
        'periodo',
        'frecuencia_uso',
        'frecuencia_ajustada',
        'meta'
    ]
    
    def __init__(self, folder_path: str, departamentos_file: Optional[str] = None):
        self.folder_path = folder_path
        self.results = []
        self.errors = []
        
        # Inicializar enriquecedor si hay archivo
        self.enricher = None
        if departamentos_file and os.path.exists(departamentos_file):
            self.enricher = GeographicEnricher(departamentos_file)
    
    def process_folder(self) -> pd.DataFrame:
        """Procesa todos los archivos Excel de la carpeta."""
        if not os.path.isdir(self.folder_path):
            raise FileNotFoundError(f"Carpeta no existe: {self.folder_path}")
        
        # Filtrar archivos Excel válidos
        excel_files = [
            f for f in os.listdir(self.folder_path)
            if f.lower().endswith(('.xlsx', '.xls')) and not f.startswith('~$')
        ]
        
        if not excel_files:
            raise ValueError(f"No hay archivos Excel en: {self.folder_path}")
        
        # Procesar cada archivo
        for filename in sorted(excel_files):
            file_path = os.path.join(self.folder_path, filename)
            
            try:
                df = SheetExtractor(file_path).extract()
                df.insert(0, 'nombre_archivo', filename)
                
                self.results.append(df)
                print(f"✓ {filename} - OK ({len(df)} registros)")
                
            except Exception as e:
                self.errors.append((filename, str(e)))
                print(f"✗ {filename} - Error: {str(e)}")
        
        if not self.results:
            raise ValueError("No se procesó ningún archivo correctamente")
        
        # Combinar resultados
        combined = pd.concat(self.results, ignore_index=True)
        
        # Enriquecer con datos geográficos
        if self.enricher:
            combined = self.enricher.enrich(combined)
        
        # Formatear datos
        combined = DataFormatter.format_dataframe(combined)
        
        # Ordenar columnas según especificación
        combined = self._order_columns(combined)
        
        return combined
    
    def _order_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ordena columnas según COLUMN_ORDER."""
        available = [col for col in self.COLUMN_ORDER if col in df.columns]
        others = [col for col in df.columns if col not in self.COLUMN_ORDER]
        return df[available + others]
    
    def get_summary(self) -> Dict:
        """Retorna resumen del procesamiento."""
        total_records = sum(len(df) for df in self.results)
        
        return {
            "archivos_procesados": len(self.results),
            "archivos_con_errores": len(self.errors),
            "total_registros": total_records,
            "errores": self.errors
        }

# ========== FUNCIÓN PRINCIPAL ==========
def extract_nt_rpms_to_csv(
    folder_path: str,
    output_csv_path: str,
    separator: str = ';',
    departamentos_file: Optional[str] = None
) -> Dict:
    """
    Extrae información de archivos Excel NT RPMS y genera CSV.
    
    Args:
        folder_path: Carpeta con archivos Excel
        output_csv_path: Ruta del CSV de salida
        separator: Separador CSV (default: ';')
        departamentos_file: Archivo opcional para enriquecimiento geográfico
    
    Returns:
        Diccionario con resultado del procesamiento
    """
    # Procesar archivos
    processor = ExcelProcessor(folder_path, departamentos_file)
    combined_df = processor.process_folder()
    
    # Escribir CSV
    output_dir = os.path.dirname(output_csv_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    combined_df.to_csv(
        output_csv_path,
        index=False,
        sep=separator,
        encoding='utf-8-sig',
        quoting=1
    )
    
    summary = processor.get_summary()
    if departamentos_file and 'departamento' in combined_df.columns:
        if combined_df['departamento'].str.len().max() > 2:
            print(f"   - Enriquecimiento geográfico: ✓ ACTIVO")
    print(f"{'='*60}\n")
    
    return {
        "success": True,
        "csv_path": output_csv_path,
        "total_rows": len(combined_df),
        "total_columns": len(combined_df.columns),
        "column_order": combined_df.columns.tolist(),
        "summary": summary,
        "has_geographic_enrichment": departamentos_file is not None
    }
