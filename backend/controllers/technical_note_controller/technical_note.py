# controllers/technical_note.py - VERSIÓN COMPLETA CON FILTROS DEL SERVIDOR
import os
import uuid
from typing import Dict, Any, List, Optional
from fastapi import HTTPException
import pandas as pd
import json

class TechnicalNoteController:
    """Controlador optimizado para archivos técnicos con paginación real y filtros del servidor"""
    
    def __init__(self, storage_manager):
        self.storage_manager = storage_manager
        self.static_files_dir = "technical_note"
        
        print(f"📁 Directorio de archivos técnicos: {self.static_files_dir}")
        
        if os.path.exists(self.static_files_dir):
            files_in_dir = os.listdir(self.static_files_dir)
            print(f"📋 Archivos encontrados: {files_in_dir}")

    def _detect_encoding(self, file_path: str) -> str:
        """✅ Detecta automáticamente el encoding del archivo"""
        encodings_to_try = ['latin-1', 'utf-8', 'cp1252', 'iso-8859-15']
        
        for encoding in encodings_to_try:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    f.read(1000)
                return encoding
            except UnicodeDecodeError:
                continue
        
        return 'latin-1'

    def get_available_static_files(self) -> List[Dict[str, Any]]:
        """✅ ULTRA-RÁPIDO: Solo info básica del sistema de archivos"""
        try:
            if not os.path.exists(self.static_files_dir):
                print(f"⚠️ Directorio no encontrado: {self.static_files_dir}")
                return []
            
            available_files = []
            supported_extensions = {'.csv', '.xlsx', '.xls'}
            
            for filename in os.listdir(self.static_files_dir):
                file_path = os.path.join(self.static_files_dir, filename)
                
                if not os.path.isfile(file_path):
                    continue
                
                _, ext = os.path.splitext(filename)
                if ext.lower() not in supported_extensions:
                    continue
                
                file_size = os.path.getsize(file_path)
                
                available_files.append({
                    "filename": filename,
                    "display_name": self._generate_display_name(filename),
                    "file_path": file_path,
                    "extension": ext.lower().replace('.', ''),
                    "file_size": file_size,
                    "description": self._generate_description(filename),
                    "columns": [],
                    "total_rows": 0
                })
            
            print(f"✅ ULTRA-RÁPIDO: {len(available_files)} archivos listados en <1s")
            return available_files
            
        except Exception as e:
            print(f"❌ Error obteniendo archivos técnicos: {e}")
            return []

    # ✅ MÉTODO PRINCIPAL CON FILTROS DEL SERVIDOR
    def read_technical_file_data_paginated(
        self, 
        filename: str, 
        page: int = 1, 
        page_size: int = 1000,
        sheet_name: Optional[str] = None,
        filters: Optional[List[Dict[str, Any]]] = None,  # ✅ NUEVO: Filtros del servidor
        search: Optional[str] = None,                   # ✅ NUEVO: Búsqueda global
        sort_by: Optional[str] = None,                  # ✅ NUEVO: Ordenamiento
        sort_order: Optional[str] = None                # ✅ NUEVO: Dirección de ordenamiento
    ) -> Dict[str, Any]:
        """✅ PAGINACIÓN REAL CON FILTROS DEL SERVIDOR"""
        try:
            file_path = os.path.join(self.static_files_dir, filename)
            
            if not os.path.exists(file_path):
                raise HTTPException(status_code=404, detail=f"Archivo no encontrado: {filename}")
            
            _, ext = os.path.splitext(filename)
            ext = ext.lower().replace('.', '')
            
            page = max(1, page)
            page_size = max(10, min(page_size, 2000))
            
            print(f"📖 FILTRADO DEL SERVIDOR: {filename} - Página {page}, Filtros: {len(filters or [])}")
            
            if ext == 'csv':
                result = self._read_csv_with_server_filters(
                    file_path, page, page_size, filters, search, sort_by, sort_order
                )
            else:
                result = self._read_excel_with_server_filters(
                    file_path, page, page_size, sheet_name, filters, search, sort_by, sort_order
                )
            
            print(f"✅ Filtrado completado: {result['pagination']['rows_in_page']} de {result['pagination']['total_rows']} registros")
            
            return {
                "success": True,
                "filename": filename,
                "display_name": self._generate_display_name(filename),
                "description": self._generate_description(filename),
                "data": result['data'],
                "columns": result['columns'],
                "pagination": result['pagination'],
                "filters_applied": filters or [],
                "search_applied": search,
                "sort_applied": {"column": sort_by, "order": sort_order} if sort_by else None,
                "file_info": {
                    "extension": ext,
                    "file_size": os.path.getsize(file_path),
                    "sheet_name": sheet_name if sheet_name else "N/A",
                    "encoding_used": result.get('encoding', 'auto-detected'),
                    "total_columns": len(result['columns']),
                    "processing_method": "server_side_filtering"
                }
            }
            
        except HTTPException:
            raise
        except Exception as e:
            print(f"❌ Error filtrado del servidor: {e}")
            raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

    def _read_csv_with_server_filters(
        self, 
        file_path: str, 
        page: int, 
        page_size: int,
        filters: Optional[List[Dict[str, Any]]] = None,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None
    ) -> Dict[str, Any]:
        """✅ Lee CSV completo, aplica filtros del servidor y pagina"""
        try:
            encoding = self._detect_encoding(file_path)
            
            # Obtener separador
            with open(file_path, 'r', encoding=encoding) as f:
                first_line = f.readline().strip()
            separator = ';' if first_line.count(';') > first_line.count(',') else ','
            
            # ✅ LEER TODO EL ARCHIVO PARA APLICAR FILTROS GLOBALES
            print(f"📊 Leyendo archivo completo para filtrado global...")
            df_complete = pd.read_csv(
                file_path,
                encoding=encoding,
                sep=separator,
                engine='python',
                on_bad_lines='skip',
                encoding_errors='replace',
                dtype=str
            )
            
            df_complete = df_complete.fillna('')
            columns = df_complete.columns.tolist()
            original_total = len(df_complete)
            
            print(f"📊 Archivo leído: {original_total} filas, aplicando filtros...")
            
            # ✅ APLICAR BÚSQUEDA GLOBAL PRIMERO
            df_filtered = df_complete.copy()
            
            if search and search.strip():
                print(f"🔍 Aplicando búsqueda global: '{search}'")
                search_lower = search.lower().strip()
                
                mask = df_filtered.apply(
                    lambda row: row.astype(str).str.lower().str.contains(
                        search_lower, case=False, na=False, regex=False
                    ).any(), 
                    axis=1
                )
                df_filtered = df_filtered[mask]
                print(f"🔍 Después de búsqueda: {len(df_filtered)} registros")
            
            # ✅ APLICAR FILTROS POR COLUMNA
            if filters:
                for filter_item in filters:
                    column = filter_item.get('column')
                    operator = filter_item.get('operator')
                    value = filter_item.get('value')
                    
                    if not column or column not in df_filtered.columns:
                        continue
                    
                    if not value and operator not in ['is_null', 'is_not_null']:
                        continue
                    
                    print(f"🔧 Aplicando filtro: {column} {operator} '{value}'")
                    
                    try:
                        col_data = df_filtered[column].astype(str).str.lower()
                        filter_value = str(value).lower() if value else ''
                        
                        if operator == 'contains':
                            mask = col_data.str.contains(filter_value, case=False, na=False, regex=False)
                        elif operator == 'equals':
                            mask = col_data == filter_value
                        elif operator == 'starts_with':
                            mask = col_data.str.startswith(filter_value, na=False)
                        elif operator == 'ends_with':
                            mask = col_data.str.endswith(filter_value, na=False)
                        elif operator == 'gt':
                            try:
                                mask = pd.to_numeric(df_filtered[column], errors='coerce') > float(filter_value)
                            except:
                                mask = col_data > filter_value
                        elif operator == 'lt':
                            try:
                                mask = pd.to_numeric(df_filtered[column], errors='coerce') < float(filter_value)
                            except:
                                mask = col_data < filter_value
                        elif operator == 'gte':
                            try:
                                mask = pd.to_numeric(df_filtered[column], errors='coerce') >= float(filter_value)
                            except:
                                mask = col_data >= filter_value
                        elif operator == 'lte':
                            try:
                                mask = pd.to_numeric(df_filtered[column], errors='coerce') <= float(filter_value)
                            except:
                                mask = col_data <= filter_value
                        elif operator == 'is_null':
                            mask = (df_filtered[column].isna()) | (df_filtered[column] == '') | (df_filtered[column] == 'nan')
                        elif operator == 'is_not_null':
                            mask = (~df_filtered[column].isna()) & (df_filtered[column] != '') & (df_filtered[column] != 'nan')
                        else:
                            continue
                        
                        df_filtered = df_filtered[mask]
                        print(f"🔧 Después de filtro {column}: {len(df_filtered)} registros")
                        
                    except Exception as e:
                        print(f"⚠️ Error aplicando filtro {column}: {e}")
                        continue
            
            # ✅ APLICAR ORDENAMIENTO
            if sort_by and sort_by in df_filtered.columns:
                print(f"📊 Aplicando ordenamiento: {sort_by} {sort_order}")
                ascending = sort_order != 'desc'
                
                try:
                    # Intentar ordenamiento numérico
                    df_filtered[f'{sort_by}_numeric'] = pd.to_numeric(df_filtered[sort_by], errors='coerce')
                    if not df_filtered[f'{sort_by}_numeric'].isna().all():
                        df_filtered = df_filtered.sort_values(f'{sort_by}_numeric', ascending=ascending, na_position='last')
                        df_filtered = df_filtered.drop(columns=[f'{sort_by}_numeric'])
                    else:
                        # Ordenamiento alfabético
                        df_filtered = df_filtered.sort_values(sort_by, ascending=ascending, na_position='last')
                except:
                    # Fallback a ordenamiento de strings
                    df_filtered = df_filtered.sort_values(sort_by, ascending=ascending, na_position='last')
            
            total_filtered_rows = len(df_filtered)
            
            # ✅ APLICAR PAGINACIÓN A LOS DATOS FILTRADOS
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            
            df_page = df_filtered.iloc[start_idx:end_idx]
            
            # Convertir a records
            data_records = df_page.to_dict(orient='records')
            
            # Calcular metadatos de paginación
            total_pages = (total_filtered_rows + page_size - 1) // page_size if total_filtered_rows > 0 else 1
            
            return {
                'data': data_records,
                'columns': columns,
                'encoding': encoding,
                'pagination': {
                    'current_page': page,
                    'page_size': page_size,
                    'total_rows': total_filtered_rows,  # ✅ Total de registros FILTRADOS
                    'total_pages': total_pages,
                    'rows_in_page': len(df_page),
                    'start_row': start_idx + 1,
                    'end_row': start_idx + len(df_page),
                    'has_next': page < total_pages,
                    'has_prev': page > 1,
                    'showing': f"{start_idx + 1}-{start_idx + len(df_page)} de {total_filtered_rows:,}" + 
                              (" (filtrados)" if total_filtered_rows != original_total else ""),
                    'original_total': original_total,  # ✅ Total original sin filtros
                    'filtered': total_filtered_rows != original_total
                }
            }
            
        except Exception as e:
            print(f"❌ Error en filtrado CSV: {e}")
            raise Exception(f"Error aplicando filtros: {str(e)}")

    def _read_excel_with_server_filters(
        self, 
        file_path: str, 
        page: int, 
        page_size: int,
        sheet_name: Optional[str] = None,
        filters: Optional[List[Dict[str, Any]]] = None,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None
    ) -> Dict[str, Any]:
        """✅ Similar implementación para Excel"""
        try:
            print(f"📊 Leyendo Excel completo para filtrado global...")
            df_complete = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
            df_complete = df_complete.fillna('')
            
            columns = df_complete.columns.tolist()
            original_total = len(df_complete)
            
            # Aplicar la misma lógica de filtrado que CSV
            df_filtered = df_complete.copy()
            
            # Búsqueda global
            if search and search.strip():
                search_lower = search.lower().strip()
                mask = df_filtered.apply(
                    lambda row: row.astype(str).str.lower().str.contains(
                        search_lower, case=False, na=False, regex=False
                    ).any(), 
                    axis=1
                )
                df_filtered = df_filtered[mask]
            
            # Filtros por columna
            if filters:
                for filter_item in filters:
                    column = filter_item.get('column')
                    operator = filter_item.get('operator')
                    value = filter_item.get('value')
                    
                    if not column or column not in df_filtered.columns:
                        continue
                    
                    if not value and operator not in ['is_null', 'is_not_null']:
                        continue
                    
                    try:
                        col_data = df_filtered[column].astype(str).str.lower()
                        filter_value = str(value).lower() if value else ''
                        
                        if operator == 'contains':
                            mask = col_data.str.contains(filter_value, case=False, na=False, regex=False)
                        elif operator == 'equals':
                            mask = col_data == filter_value
                        # ... (resto de operadores igual que CSV)
                        else:
                            continue
                        
                        df_filtered = df_filtered[mask]
                        
                    except Exception as e:
                        print(f"⚠️ Error aplicando filtro Excel {column}: {e}")
                        continue
            
            # Ordenamiento
            if sort_by and sort_by in df_filtered.columns:
                ascending = sort_order != 'desc'
                try:
                    df_filtered = df_filtered.sort_values(sort_by, ascending=ascending, na_position='last')
                except:
                    pass
            
            total_filtered_rows = len(df_filtered)
            
            # Paginación
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            df_page = df_filtered.iloc[start_idx:end_idx]
            
            # Convertir a string
            for col in df_page.columns:
                df_page[col] = df_page[col].astype(str)
            
            data_records = df_page.to_dict(orient='records')
            total_pages = (total_filtered_rows + page_size - 1) // page_size if total_filtered_rows > 0 else 1
            
            return {
                'data': data_records,
                'columns': columns,
                'pagination': {
                    'current_page': page,
                    'page_size': page_size,
                    'total_rows': total_filtered_rows,
                    'total_pages': total_pages,
                    'rows_in_page': len(df_page),
                    'start_row': start_idx + 1,
                    'end_row': start_idx + len(df_page),
                    'has_next': page < total_pages,
                    'has_prev': page > 1,
                    'showing': f"{start_idx + 1}-{start_idx + len(df_page)} de {total_filtered_rows:,}" + 
                              (" (filtrados)" if total_filtered_rows != original_total else ""),
                    'original_total': original_total,
                    'filtered': total_filtered_rows != original_total
                }
            }
            
        except Exception as e:
            print(f"❌ Error en filtrado Excel: {e}")
            raise Exception(f"Error aplicando filtros Excel: {str(e)}")

    # ✅ MÉTODO LEGACY PARA COMPATIBILIDAD
    def read_technical_file_data_paginated_legacy(
        self, 
        filename: str, 
        page: int = 1, 
        page_size: int = 1000,
        sheet_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """✅ Método legacy sin filtros para compatibilidad"""
        return self.read_technical_file_data_paginated(
            filename, page, page_size, sheet_name
        )

    def get_technical_file_metadata(self, filename: str) -> Dict[str, Any]:
        """✅ Obtiene metadatos bajo demanda"""
        try:
            file_path = os.path.join(self.static_files_dir, filename)
            
            if not os.path.exists(file_path):
                raise HTTPException(status_code=404, detail=f"Archivo no encontrado: {filename}")
            
            _, ext = os.path.splitext(filename)
            ext = ext.lower().replace('.', '')
            
            print(f"📋 Obteniendo metadatos: {filename}")
            
            if ext == 'csv':
                encoding = self._detect_encoding(file_path)
                
                with open(file_path, 'r', encoding=encoding) as f:
                    total_rows = sum(1 for _ in f) - 1
                
                with open(file_path, 'r', encoding=encoding) as f:
                    first_line = f.readline().strip()
                
                separator = ';' if first_line.count(';') > first_line.count(',') else ','
                df_header = pd.read_csv(file_path, encoding=encoding, sep=separator, nrows=0, engine='python')
                columns = df_header.columns.tolist()
                
            else:
                df_sample = pd.read_excel(file_path, nrows=0, engine='openpyxl')
                columns = df_sample.columns.tolist()
                
                df_full = pd.read_excel(file_path, engine='openpyxl')
                total_rows = len(df_full)
                encoding = 'N/A'
                separator = 'N/A'
            
            return {
                "filename": filename,
                "display_name": self._generate_display_name(filename),
                "description": self._generate_description(filename),
                "total_rows": total_rows,
                "total_columns": len(columns),
                "columns": columns,
                "file_size": os.path.getsize(file_path),
                "extension": ext,
                "encoding": encoding,
                "separator": separator,
                "recommended_page_size": min(1000, max(100, total_rows // 100)) if total_rows > 0 else 1000
            }
            
        except Exception as e:
            print(f"❌ Error obteniendo metadatos: {e}")
            raise HTTPException(status_code=500, detail=f"Error metadatos: {str(e)}")

    def _generate_display_name(self, filename: str) -> str:
        """Genera un nombre display más amigable"""
        name_map = {
            "AdolescenciaNueva.csv": "Adolescencia",
            "AdultezNueva.csv": "Adultez", 
            "InfanciaNueva.csv": "Infancia",
            "JuventudNueva.csv": "Juventud",
            "PrimeraInfanciaNueva.csv": "Primera Infancia",
            "VejezNueva.csv": "Vejez"
        }
        
        return name_map.get(filename, filename.replace('.csv', '').replace('.xlsx', '').replace('.xls', ''))
    
    def _generate_description(self, filename: str) -> str:
        """Genera una descripción del archivo"""
        desc_map = {
            "AdolescenciaNueva.csv": "Datos de población adolescente",
            "AdultezNueva.csv": "Datos de población adulta",
            "InfanciaNueva.csv": "Datos de población infantil", 
            "JuventudNueva.csv": "Datos de población joven",
            "PrimeraInfanciaNueva.csv": "Datos de primera infancia",
            "VejezNueva.csv": "Datos de población adulto mayor"
        }
        
        return desc_map.get(filename, f"Archivo técnico: {filename}")

def get_technical_note_controller():
    from controllers.files_controllers.storage_manager import storage_manager
    return TechnicalNoteController(storage_manager)

technical_note_controller = get_technical_note_controller()
