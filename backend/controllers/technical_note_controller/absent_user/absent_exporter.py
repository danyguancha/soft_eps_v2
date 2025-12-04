import io
import csv
from typing import List, Dict, Any
import pandas as pd
from fastapi.responses import StreamingResponse


class AbsentExporter:
    """Exporta reportes de inasistentes a CSV"""
    
    MESES_NOMBRES = [
        "enero", "febrero", "marzo", "abril", "mayo", "junio",
        "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"
    ]
    
    @staticmethod
    def export_to_csv(
        report_data: Dict[str, Any],
        filename: str,
        encoding: str = "utf-8-sig",
        use_excel_sep_hint: bool = False,
        sep: str = ";"
    ) -> StreamingResponse:
        """Exporta reporte completo a CSV"""
        
        if not report_data.get("success"):
            raise ValueError(report_data.get("error", "Error en reporte"))
        
        rows = AbsentExporter._extract_all_records(report_data)
        
        df = AbsentExporter._create_dataframe(rows)
        
        buf = AbsentExporter._create_csv_buffer(
            df, encoding, use_excel_sep_hint, sep
        )
        
        csv_filename = AbsentExporter._build_filename(
            filename,
            report_data.get("filtros_aplicados", {}).get("keywords", []),
            report_data.get("filtros_aplicados", {}).get("departamento"),
            report_data.get("corte_fecha", "")
        )
        
        return AbsentExporter._create_streaming_response(
            buf, csv_filename, encoding
        )
    
    @staticmethod
    def _extract_all_records(report_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extrae todos los registros individuales del reporte"""
        all_records = []
        
        for activity in report_data.get("inasistentes_por_actividad", []):
            consulta = activity.get("actividad", "")
            rango_edad = activity.get("rango_edad", "")
            
            for mes_nombre in AbsentExporter.MESES_NOMBRES:
                if mes_nombre in activity:
                    mes_data = activity[mes_nombre]
                    inasistentes = mes_data.get("inasistentes", [])
                    
                    for inasistente in inasistentes:
                        record = {
                            "Departamento": inasistente.get("departamento", ""),
                            "Municipio": inasistente.get("municipio", ""),
                            "Nombre IPS": inasistente.get("nombre_ips", ""),
                            "Numero Identificacion": inasistente.get("nro_identificacion", ""),
                            "Primer Apellido": inasistente.get("primer_apellido", ""),
                            "Segundo Apellido": inasistente.get("segundo_apellido", ""),
                            "Primer Nombre": inasistente.get("primer_nombre", ""),
                            "Segundo Nombre": inasistente.get("segundo_nombre", ""),
                            "Fecha Nacimiento": inasistente.get("fecha_nacimiento", ""),
                            "Edad Anos": inasistente.get("edad_anos", ""),
                            "Mes Correspondiente": mes_nombre.capitalize(),
                            "Consulta Faltante": consulta,
                            "Rango Edad": rango_edad,
                            "Estado Actividad": inasistente.get("actividad_valor", ""),
                            "Fecha Corte": report_data.get("corte_fecha", "")
                        }
                        all_records.append(record)
        
        return all_records
    
    @staticmethod
    def _create_dataframe(rows: List[Dict[str, Any]]) -> pd.DataFrame:
        """Crea DataFrame con columnas predefinidas"""
        if not rows:
            return pd.DataFrame(columns=[
                "Departamento", "Municipio", "Nombre IPS", "Numero Identificacion",
                "Primer Apellido", "Segundo Apellido", "Primer Nombre", "Segundo Nombre",
                "Fecha Nacimiento", "Edad Anos", "Mes Correspondiente",
                "Consulta Faltante", "Rango Edad", "Estado Actividad", "Fecha Corte"
            ])
        return pd.DataFrame(rows)
    
    @staticmethod
    def _create_csv_buffer(
        df: pd.DataFrame,
        encoding: str,
        use_excel_sep_hint: bool,
        sep: str
    ) -> io.BytesIO:
        """Crea buffer con CSV"""
        enc_map = {"cp1252": "cp1252", "latin-1": "latin-1", "utf-8-sig": "utf-8-sig"}
        enc = enc_map.get(encoding.lower(), "utf-8-sig")
        
        buf = io.BytesIO()
        
        if use_excel_sep_hint:
            buf.write(f"sep={sep}\n".encode(enc))
        
        df.to_csv(buf, index=False, encoding=enc, sep=sep,
                 quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
        buf.seek(0)
        
        return buf
    
    @staticmethod
    def _build_filename(
        filename: str,
        keywords: List[str],
        departamento: str,
        corte_fecha: str
    ) -> str:
        """Construye nombre del archivo CSV"""
        
        def sanitize(text):
            if not text:
                return ""
            replacements = [
                ("ñ", "n"), ("Ñ", "N"), ("á", "a"), ("é", "e"),
                ("í", "i"), ("ó", "o"), ("ú", "u"), (" ", "-")
            ]
            for old, new in replacements:
                text = text.replace(old, new)
            return text
        
        filters = []
        
        if keywords:
            filters.append("palabras-" + "-".join(sanitize(k) for k in keywords))
        
        if departamento:
            filters.append("dept-" + sanitize(departamento))
        
        suffix = "_" + "_".join(filters) if filters else ""
        base_name = filename.replace('.csv', '').replace(' ', '-')
        
        return f"inasistentes_{base_name}{suffix}_{corte_fecha}.csv"
    
    @staticmethod
    def _create_streaming_response(
        buf: io.BytesIO,
        filename: str,
        encoding: str
    ) -> StreamingResponse:
        """Crea StreamingResponse para descarga"""
        
        def stream():
            buf.seek(0)
            while chunk := buf.read(8192):
                yield chunk
        
        charset_map = {
            "cp1252": "windows-1252",
            "latin-1": "ISO-8859-1",
            "utf-8-sig": "utf-8"
        }
        charset = charset_map.get(encoding.lower(), "utf-8")
        
        return StreamingResponse(
            stream(),
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Content-Type": f"text/csv; charset={charset}"
            }
        )
