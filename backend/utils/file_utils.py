import os
import pandas as pd
from typing import Dict, Any
from collections import defaultdict

class FileUtils:
    """Utilidades para manejo de archivos"""
    
    def detect_csv_encoding_and_separator(self, file_path: str) -> Dict[str, Any]:
        """Detecta automáticamente encoding y separador para CSV"""
        
        encodings_to_try = ['utf-8', 'utf-8-sig', 'latin1', 'cp1252', 'iso-8859-1', 'iso-8859-15']
        separators_to_try = [';', ',', '\t', '|']
        
        detected_config = {
            "encoding": "utf-8",
            "separator": ",",
            "success": False
        }
        
        for encoding in encodings_to_try:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    first_lines = [f.readline().strip() for _ in range(min(5, sum(1 for _ in open(file_path, 'r', encoding=encoding))))]
                
                if not first_lines:
                    continue
                
                separator_scores = {}
                
                for sep in separators_to_try:
                    counts = [line.count(sep) for line in first_lines if line.strip()]
                    if counts and max(counts) > 0:
                        avg_count = sum(counts) / len(counts)
                        variance = sum((count - avg_count) ** 2 for count in counts) / len(counts)
                        separator_scores[sep] = (avg_count, -variance)
                
                if separator_scores:
                    best_separator = max(separator_scores.items(), key=lambda x: (x[1][0], x[1][1]))[0]
                    
                    detected_config = {
                        "encoding": encoding,
                        "separator": best_separator,
                        "success": True
                    }
                    
                    print(f"Detectado automáticamente: encoding='{encoding}', separador='{best_separator}'")
                    return detected_config
                    
            except UnicodeDecodeError:
                continue
            except Exception as e:
                print(f"Error probando encoding {encoding}: {e}")
                continue
        
        print("No se pudo detectar automáticamente, usando valores por defecto")
        return detected_config

    def robust_csv_read(self, file_path: str, encoding: str, separator: str, **kwargs) -> pd.DataFrame:
        """Método auxiliar para lectura ultra-robusta de CSV con tipos mixtos"""
        
        # Configuración robusta por defecto
        robust_config = {
            'dtype': defaultdict(lambda: str),  # Todas las columnas como string
            'keep_default_na': False,           # No convertir strings a NaN
            'na_filter': False,                 # No procesar valores NA automáticamente
            'low_memory': False,                # Evitar warnings de tipos mixtos
            'on_bad_lines': 'skip',             # Saltar líneas problemáticas
            'encoding_errors': 'replace',       # Reemplazar caracteres problemáticos
            'sep': separator,
            'encoding': encoding,
            **kwargs  # Permitir override de configuraciones
        }
        
        try:
            # Suprimir warning de DtypeWarning usando filtros de warning
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=pd.errors.DtypeWarning)
                df = pd.read_csv(file_path, **robust_config)
            
            # Limpieza POST-LECTURA
            print(f"📊 CSV leído: {len(df)} filas, {len(df.columns)} columnas")
            
            # Asegurar que todas las columnas son string
            for col in df.columns:
                df[col] = df[col].astype(str)
            
            # Reemplazar valores problemáticos comunes
            problematic_values = ['nan', '<NA>', 'None', 'NaT', 'NULL', 'null']
            for val in problematic_values:
                df = df.replace(val, '')
            
            df = df.fillna('')
            
            print("Limpieza completada: todos los tipos son string")
            return df
            
        except Exception as e:
            print(f"Error en lectura robusta: {e}")
            raise e

    def calculate_file_size_mb(self, file_path: str) -> float:
        """Calcula el tamaño del archivo en MB"""
        if os.path.exists(file_path):
            return os.path.getsize(file_path) / 1024 / 1024
        return 0.0

    def ensure_directory_exists(self, directory_path: str):
        """Asegura que el directorio existe, lo crea si no existe"""
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)
            print(f"📁 Directorio creado: {directory_path}")

    def clean_filename(self, filename: str) -> str:
        """Limpia un nombre de archivo de caracteres problemáticos"""
        import re
        # Remover caracteres problemáticos
        cleaned = re.sub(r'[<>:"/\\|?*]', '_', filename)
        # Limitar longitud
        if len(cleaned) > 200:
            name, ext = os.path.splitext(cleaned)
            cleaned = name[:200-len(ext)] + ext
        return cleaned

    def get_file_extension(self, file_path: str) -> str:
        """Obtiene la extensión del archivo en minúsculas"""
        return os.path.splitext(file_path)[1].lower().lstrip('.')

    def is_excel_file(self, file_path: str) -> bool:
        """Verifica si el archivo es de Excel"""
        excel_extensions = ['xlsx', 'xls', 'xlsm', 'xlsb']
        return self.get_file_extension(file_path) in excel_extensions

    def is_csv_file(self, file_path: str) -> bool:
        """Verifica si el archivo es CSV"""
        csv_extensions = ['csv', 'txt', 'tsv']
        return self.get_file_extension(file_path) in csv_extensions

    def get_safe_filename(self, original_name: str, file_id: str) -> str:
        """Genera un nombre de archivo seguro usando el hash"""
        extension = self.get_file_extension(original_name)
        safe_name = f"{file_id[:8]}_{self.clean_filename(original_name)}"
        if extension:
            safe_name = f"{safe_name}.{extension}"
        return safe_name

    def validate_file_exists(self, file_path: str) -> Dict[str, Any]:
        """Valida que un archivo existe y es accesible"""
        try:
            if not os.path.exists(file_path):
                return {
                    "valid": False,
                    "error": f"El archivo no existe: {file_path}"
                }
            
            if not os.path.isfile(file_path):
                return {
                    "valid": False,
                    "error": f"La ruta no es un archivo: {file_path}"
                }
            
            # Verificar que se puede leer
            with open(file_path, 'rb') as f:
                f.read(1)
            
            file_size = os.path.getsize(file_path)
            return {
                "valid": True,
                "file_path": file_path,
                "file_size_bytes": file_size,
                "file_size_mb": file_size / 1024 / 1024
            }
            
        except PermissionError:
            return {
                "valid": False,
                "error": f"Sin permisos para leer el archivo: {file_path}"
            }
        except Exception as e:
            return {
                "valid": False,
                "error": f"Error validando archivo: {str(e)}"
            }

    def backup_file(self, file_path: str, backup_dir: str) -> Dict[str, Any]:
        """Crea una copia de seguridad de un archivo"""
        try:
            if not os.path.exists(file_path):
                return {"success": False, "error": "Archivo original no existe"}
            
            self.ensure_directory_exists(backup_dir)
            
            # Generar nombre de backup con timestamp
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = os.path.basename(file_path)
            name, ext = os.path.splitext(filename)
            backup_filename = f"{name}_backup_{timestamp}{ext}"
            backup_path = os.path.join(backup_dir, backup_filename)
            
            # Copiar archivo
            import shutil
            shutil.copy2(file_path, backup_path)
            
            return {
                "success": True,
                "backup_path": backup_path,
                "backup_size_mb": self.calculate_file_size_mb(backup_path)
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Error creando backup: {str(e)}"
            }

    def remove_file_safely(self, file_path: str) -> bool:
        """Remueve un archivo de forma segura"""
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                return True
            return False
        except Exception as e:
            print(f"Error removiendo archivo {file_path}: {e}")
            return False

    def get_directory_size(self, directory_path: str) -> Dict[str, Any]:
        """Calcula el tamaño total de un directorio"""
        try:
            total_size = 0
            file_count = 0
            
            for dirpath, dirnames, filenames in os.walk(directory_path):
                for filename in filenames:
                    file_path = os.path.join(dirpath, filename)
                    try:
                        total_size += os.path.getsize(file_path)
                        file_count += 1
                    except FileNotFoundError:
                        continue
            
            return {
                "total_size_bytes": total_size,
                "total_size_mb": total_size / 1024 / 1024,
                "file_count": file_count
            }
            
        except Exception as e:
            return {
                "total_size_bytes": 0,
                "total_size_mb": 0,
                "file_count": 0,
                "error": str(e)
            }
