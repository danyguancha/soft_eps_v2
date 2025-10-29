# services/technical_note_services/report_service_aux/csv_exporter.py
import csv
import io
from typing import Any, Dict, List


class CSVExporter:
    """
    📄 EXPORTADOR CSV
    - Exporta reportes temporales con jerarquía (ACTIVIDAD → AÑO → MES)
    - Devuelve contenido en memoria
    """
    
    def __init__(self, separator: str = ';', encoding: str = 'latin1'):
        """
        Inicializa el exportador CSV
        
        Args:
            separator: Separador de columnas (por defecto ';')
            encoding: Codificación del archivo (por defecto 'latin1')
        """
        self.separator = separator
        self.encoding = encoding
    
    def export_temporal_report(
        self,
        report_data: Dict[str, Any]
    ) -> io.BytesIO:
        """
        Exporta reporte temporal en memoria (sin archivos temporales)
        
        Args:
            report_data: Datos del reporte con 'items' y 'temporal_data'
            
        Returns:
            BytesIO con contenido CSV listo para descarga
        """
        try:
            temporal_data = report_data.get('temporal_data', {})
            items = report_data.get('items', [])
            
            print(f"📊 Exportando CSV temporal: {len(items)} actividades, {len(temporal_data)} claves")
            
            if not temporal_data:
                print("⚠️ Sin datos temporales para exportar")
                return None
            
            # Construir datos
            headers = self._get_headers()
            rows = self._build_rows(temporal_data, items)
            
            # Escribir en memoria
            csv_content = self._write_csv_to_memory(headers, rows)
            
            print(f"✅ CSV Temporal generado en memoria: {len(rows)} filas")
            return csv_content
            
        except Exception as e:
            print(f"❌ Error exportando CSV temporal: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _get_headers(self) -> List[str]:
        """Define los headers del CSV"""
        return [
            'Procedimiento/Consulta',
            'Palabra Clave',
            'Tipo',
            'Periodo',
            'Denominador',
            'Numerador',
            '% Cumplimiento',
            'Estado',
            'Descripcion'
        ]
    
    def _build_rows(
        self,
        temporal_data: Dict[str, Any],
        items: List[Dict[str, Any]]
    ) -> List[List[str]]:
        """
        Construye las filas del CSV con jerarquía ACTIVIDAD → AÑO → MES
        
        Args:
            temporal_data: Datos temporales del reporte
            items: Lista de actividades con totales globales
            
        Returns:
            Lista de filas (cada fila es una lista de strings)
        """
        rows = []
        
        for key, temporal_info in temporal_data.items():
            column = str(temporal_info.get('column', '')).replace(self.separator, ' ')
            keyword = str(temporal_info.get('keyword', ''))
            
            # Buscar totales globales de la actividad
            global_item = self._find_global_item(temporal_info, items)
            
            # Agregar fila de actividad global
            if global_item:
                rows.append(self._build_activity_row(column, keyword, global_item))
            
            # Agregar filas de años y meses
            years_dict = temporal_info.get('years', {})
            self._add_temporal_rows(rows, column, keyword, years_dict)
            
            # Separador entre actividades
            rows.append([''] * len(self._get_headers()))
        
        return rows
    
    def _find_global_item(
        self,
        temporal_info: Dict[str, Any],
        items: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Busca el item global que coincide con la actividad temporal"""
        for item in items:
            if (item.get('column') == temporal_info.get('column') and
                item.get('keyword') == temporal_info.get('keyword') and
                item.get('age_range') == temporal_info.get('age_range')):
                return item
        return None
    
    def _build_activity_row(
        self,
        column: str,
        keyword: str,
        global_item: Dict[str, Any]
    ) -> List[str]:
        """Construye fila de ACTIVIDAD (totales globales)"""
        return [
            column,
            keyword,
            'ACTIVIDAD',
            column,
            str(global_item.get('denominador', 0)),
            str(global_item.get('numerador', 0)),
            str(round(global_item.get('cobertura_porcentaje', 0.0), 1)).replace('.', ','),
            global_item.get('semaforizacion', 'NA'),
            global_item.get('descripcion', '').replace(self.separator, ' ')
        ]
    
    def _add_temporal_rows(
        self,
        rows: List[List[str]],
        column: str,
        keyword: str,
        years_dict: Dict[str, Any]
    ):
        """Agrega filas de años y meses"""
        for year_str in sorted(years_dict.keys(), key=lambda y: int(y), reverse=True):
            year_info = years_dict[year_str]
            
            # Fila del AÑO
            rows.append(self._build_year_row(column, keyword, year_str, year_info))
            
            # Filas de MESES
            months_dict = year_info.get('months', {})
            sorted_months = sorted(months_dict.items(), key=lambda m: m[1].get('month', 0))
            
            for month_name, month_info in sorted_months:
                rows.append(self._build_month_row(column, keyword, year_str, month_name, month_info))
    
    def _build_year_row(
        self,
        column: str,
        keyword: str,
        year_str: str,
        year_info: Dict[str, Any]
    ) -> List[str]:
        """Construye fila de AÑO"""
        year_den = year_info.get('total_den') or year_info.get('denominador') or 0
        year_num = year_info.get('total_num') or year_info.get('numerador') or 0
        year_pct = year_info.get('pct') or year_info.get('cobertura_porcentaje') or 0.0
        
        return [
            column,
            keyword,
            'AÑO',
            year_str,
            str(year_den),
            str(year_num),
            str(round(year_pct, 1)).replace('.', ','),
            year_info.get('semaforizacion', 'NA'),
            year_info.get('descripcion', '').replace(self.separator, ' ')
        ]
    
    def _build_month_row(
        self,
        column: str,
        keyword: str,
        year_str: str,
        month_name: str,
        month_info: Dict[str, Any]
    ) -> List[str]:
        """Construye fila de MES"""
        month_den = month_info.get('denominador') or month_info.get('den') or 0
        month_num = month_info.get('numerador') or month_info.get('num') or 0
        month_pct = month_info.get('pct') or month_info.get('cobertura_porcentaje') or 0.0
        
        return [
            column,
            keyword,
            'MES',
            f"{month_name} {year_str}",
            str(month_den),
            str(month_num),
            str(round(month_pct, 1)).replace('.', ','),
            month_info.get('semaforizacion', 'NA'),
            month_info.get('descripcion', '').replace(self.separator, ' ')
        ]
    
    def _write_csv_to_memory(
        self,
        headers: List[str],
        rows: List[List[str]]
    ) -> io.BytesIO:
        """
        Escribe CSV en memoria (BytesIO)
        
        Args:
            headers: Lista de headers
            rows: Lista de filas
            
        Returns:
            BytesIO con contenido CSV
        """
        # Usar StringIO para escribir texto
        string_buffer = io.StringIO()
        writer = csv.writer(string_buffer, delimiter=self.separator)
        writer.writerow(headers)
        writer.writerows(rows)
        
        # Convertir a bytes con la codificación especificada
        csv_text = string_buffer.getvalue()
        bytes_buffer = io.BytesIO(csv_text.encode(self.encoding, errors='replace'))
        bytes_buffer.seek(0)
        
        return bytes_buffer
