# services/csv_service.py
import csv
import pandas as pd
import chardet
from pathlib import Path
from typing import List, Optional, Dict, Any
from models.base import AbstractFile
from services.interfaces import IFileService


def detect_encoding(file_path: str, sample_size: int = 100000) -> str:
    """Detecta automáticamente el encoding de un archivo CSV"""
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read(sample_size)
        
        if not raw_data:
            return 'utf-8'
        
        detected = chardet.detect(raw_data)
        encoding = detected.get('encoding', 'utf-8')
        confidence = detected.get('confidence', 0)
        
        print(f"🔍 Encoding detectado: {encoding} (confianza: {confidence:.2f})")
        
        # Manejar UTF-8 con BOM
        if encoding and 'utf-8' in encoding.lower():
            if raw_data.startswith(b'\xef\xbb\xbf'):
                return 'utf-8-sig'
            else:
                return 'utf-8'
        
        if confidence < 0.7:
            print("Confianza baja, probando encodings comunes...")
            return try_common_encodings(file_path)
        
        return encoding
        
    except Exception as e:
        print(f"Error detectando encoding: {e}")
        return try_common_encodings(file_path)


def try_common_encodings(file_path: str) -> str:
    """Prueba encodings comunes cuando chardet falla"""
    common_encodings = [
        'utf-8-sig', 'utf-8', 'latin1', 'cp1252', 'iso-8859-1'
    ]
    
    for encoding in common_encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                for i, line in enumerate(f):
                    if i >= 3:
                        break
                print(f"Encoding funcional encontrado: {encoding}")
                return encoding
        except (UnicodeDecodeError, UnicodeError):
            continue
    
    return 'latin1'


def sniff_delimiter(path: str, encoding: str) -> str:
    """Detecta el delimitador usando el encoding correcto"""
    separators = [';', ',', '\t', '|']
    
    try:
        with open(path, "r", encoding=encoding) as fh:
            sample = fh.read(32768)
            
        if not sample.strip():
            return ';'
            
        try:
            detected_delimiter = csv.Sniffer().sniff(sample, delimiters=''.join(separators)).delimiter
            lines = [line for line in sample.split('\n')[:5] if line.strip()]
            if lines and lines[0].count(detected_delimiter) > 0:
                print(f"📊 Separador detectado: '{detected_delimiter}'")
                return detected_delimiter
        except csv.Error:
            pass
        
        # Fallback manual
        separator_scores = {}
        lines = [line for line in sample.split('\n')[:10] if line.strip()]
        
        for sep in separators:
            counts = [line.count(sep) for line in lines]
            if counts and max(counts) > 0:
                avg_count = sum(counts) / len(counts)
                separator_scores[sep] = avg_count
        
        if separator_scores:
            best_sep = max(separator_scores, key=separator_scores.get)
            print(f"📊 Separador detectado: '{best_sep}'")
            return best_sep
            
    except Exception as e:
        print(f"Error detectando separador: {e}")
    
    return ';'


def get_optimal_engine_and_params(encoding: str, delimiter: str) -> Dict[str, Any]:
    """Determina el mejor engine y parámetros según las características del archivo"""
    if delimiter in [',', ';', '\t'] and encoding in ['utf-8', 'utf-8-sig', 'latin1']:
        return {
            'engine': 'c',
            'low_memory': False,
            'encoding_errors': 'ignore'
        }
    else:
        return {
            'engine': 'python',
            'encoding_errors': 'ignore'
        }


def convert_excel_dates_to_readable(series):
    """Convierte fechas tipo Excel (números) y strings a formato DD/MM/YYYY"""
    result = []
    for val in series:
        # Si es nulo o string vacío
        if pd.isna(val) or str(val).lower() in ['nan', 'null', '', 'none']:
            result.append('')
            continue
        
        # Si es un número (fecha serial de Excel)
        try:
            as_float = float(val)
            # Solo convertir si parece ser una fecha serial válida (> 2000 para años recientes)
            if as_float > 2000:
                d = pd.to_datetime(as_float, unit='D', origin='1899-12-30')
                result.append(d.strftime('%d/%m/%Y'))
                continue
        except (ValueError, TypeError):
            pass
        
        # Si es una string que ya parece fecha
        try:
            d = pd.to_datetime(str(val), errors='coerce', dayfirst=True)
            if pd.notna(d):
                result.append(d.strftime('%d/%m/%Y'))
            else:
                result.append(str(val))
        except Exception:
            result.append(str(val))
    
    return result


class CSVFile(AbstractFile):
    def __init__(self, file_path: str):
        super().__init__(file_path)
        self._encoding: str | None = None
        self._delimiter: str | None = None
        self._total_rows: int | None = None
        self._columns_cache: List[str] | None = None
        self._engine_params: Dict[str, Any] | None = None

    def get_encoding(self) -> str:
        if self._encoding is None:
            self._encoding = detect_encoding(self.file_path)
        return self._encoding

    def get_delimiter(self) -> str:
        if self._delimiter is None:
            encoding = self.get_encoding()
            self._delimiter = sniff_delimiter(self.file_path, encoding)
        return self._delimiter

    def get_engine_params(self) -> Dict[str, Any]:
        if self._engine_params is None:
            encoding = self.get_encoding()
            delimiter = self.get_delimiter()
            self._engine_params = get_optimal_engine_and_params(encoding, delimiter)
        return self._engine_params

    def count_rows_efficiently(self) -> int:
        if self._total_rows is not None:
            return self._total_rows
            
        encoding = self.get_encoding()
        try:
            count = 0
            with open(self.file_path, 'r', encoding=encoding) as f:
                next(f, None)  # Saltar header
                for line in f:
                    if line.strip():
                        count += 1
            self._total_rows = count
            return count
        except Exception as e:
            print(f"Error contando filas: {e}")
            return 0

    def get_columns(self, sheet_name: str | None = None) -> List[str]:
        if self._columns_cache is not None:
            return self._columns_cache
            
        encoding = self.get_encoding()
        delimiter = self.get_delimiter()
        engine_params = self.get_engine_params()
        
        try:
            read_params = {
                'sep': delimiter,
                'nrows': 0,
                'encoding': encoding,
                'on_bad_lines': 'skip',
                **engine_params
            }
            
            df_header = pd.read_csv(self.file_path, **read_params)
            self._columns_cache = df_header.columns.tolist()
            print(f"Columnas detectadas: {len(self._columns_cache)} (engine: {engine_params['engine']})")
            return self._columns_cache
        except Exception as e:
            print(f"Error obteniendo columnas: {e}")
            return []

    def _detect_date_columns(self, columns: List[str]) -> List[str]:
        """Detecta columnas que probablemente contienen fechas"""
        date_indicators = [
            'fecha', 'date', 'datetime', 'time', 'nacimiento', 'prestacion', 
            'radicacion', 'conciliacion', 'retiro', 'afiliacion', 'fecnac'
        ]
        
        date_columns = []
        for col in columns:
            col_lower = col.lower()
            if any(indicator in col_lower for indicator in date_indicators):
                date_columns.append(col)
        
        return date_columns

    def _convert_date_columns(self, df: pd.DataFrame, date_columns: List[str]) -> pd.DataFrame:
        """Convierte columnas detectadas a formato fecha legible"""
        for col in date_columns:
            if col in df.columns:
                try:
                    df[col] = convert_excel_dates_to_readable(df[col])
                    print(f"Columna '{col}' convertida a fecha")
                except Exception as e:
                    print(f"No se pudo convertir '{col}' a fecha: {e}")
        
        return df

    def get_data(self, sheet_name: str | None = None) -> pd.DataFrame:
        """Cargar CSV con conversión automática de fechas"""
        encoding = self.get_encoding()
        delimiter = self.get_delimiter()
        engine_params = self.get_engine_params()
        
        read_params = {
            'sep': delimiter,
            'encoding': encoding,
            'on_bad_lines': 'skip',
            'dtype': str,  # Leer como string primero para poder convertir fechas
            **engine_params
        }
        
        df = pd.read_csv(self.file_path, **read_params)
        
        # Detectar y convertir columnas de fecha
        date_columns = self._detect_date_columns(df.columns.tolist())
        df = self._convert_date_columns(df, date_columns)
        
        return df

    def get_data_chunked(
        self, 
        start_row: int = 0, 
        nrows: Optional[int] = None,
        sheet_name: str | None = None
    ) -> pd.DataFrame:
        """Obtiene datos por chunks con conversión de fechas"""
        encoding = self.get_encoding()
        delimiter = self.get_delimiter()
        engine_params = self.get_engine_params()
        
        read_params = {
            'sep': delimiter,
            'encoding': encoding,
            'on_bad_lines': 'skip',
            'dtype': str,
            **engine_params
        }
        
        if start_row > 0:
            read_params['skiprows'] = list(range(1, start_row + 1))
        
        if nrows is not None:
            read_params['nrows'] = nrows
        
        df = pd.read_csv(self.file_path, **read_params)
        
        # Convertir fechas
        date_columns = self._detect_date_columns(df.columns.tolist())
        df = self._convert_date_columns(df, date_columns)
        
        return df

    def get_chunk_iterator(self, chunk_size: int = 10000):
        """Iterador de chunks con conversión de fechas"""
        encoding = self.get_encoding()
        delimiter = self.get_delimiter()
        engine_params = self.get_engine_params()
        
        read_params = {
            'sep': delimiter,
            'chunksize': chunk_size,
            'encoding': encoding,
            'on_bad_lines': 'skip',
            'dtype': str,
            **engine_params
        }
        
        for chunk in pd.read_csv(self.file_path, **read_params):
            date_columns = self._detect_date_columns(chunk.columns.tolist())
            chunk = self._convert_date_columns(chunk, date_columns)
            yield chunk

    def get_sample_data(self, n_rows: int = 1000) -> pd.DataFrame:
        return self.get_data_chunked(start_row=0, nrows=n_rows)

    def get_file_info(self) -> Dict[str, Any]:
        engine_params = self.get_engine_params()
        return {
            'total_rows': self.count_rows_efficiently(),
            'columns': self.get_columns(),
            'delimiter': self.get_delimiter(),
            'encoding': self.get_encoding(),
            'engine': engine_params['engine'],
            'file_size_bytes': Path(self.file_path).stat().st_size,
            'has_sheets': False
        }


class CSVService(IFileService):
    def load(self, file_path: str) -> CSVFile:
        if not Path(file_path).is_file():
            raise FileNotFoundError(f"Archivo no encontrado: {file_path}")
        return CSVFile(file_path)

    def get_columns(self, obj: CSVFile, sheet_name: str | None = None) -> list:
        return obj.get_columns()

    def get_sheets(self, obj: CSVFile) -> list:
        return []

    def get_data(self, obj: CSVFile, sheet_name: str | None = None):
        return obj.get_data()
    
    def get_data_chunked(
        self, 
        obj: CSVFile, 
        start_row: int = 0, 
        nrows: Optional[int] = None,
        sheet_name: str | None = None
    ) -> pd.DataFrame:
        return obj.get_data_chunked(start_row, nrows, sheet_name)
    
    def get_chunk_iterator(self, obj: CSVFile, chunk_size: int = 10000):
        return obj.get_chunk_iterator(chunk_size)
    
    def get_file_info(self, obj: CSVFile) -> Dict[str, Any]:
        return obj.get_file_info()
