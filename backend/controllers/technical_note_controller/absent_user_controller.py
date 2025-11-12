import csv
import io
from typing import Any, Dict, List, Optional
import unicodedata
from fastapi.responses import StreamingResponse
import pandas as pd

from services.duckdb_service.duckdb_service import duckdb_service
from services.technical_note_services.data_source_service import DataSourceService
from controllers.technical_note_controller.absent_user.reports_activity import ReportActivity
from controllers.technical_note_controller.absent_user.activity_column import ActivityColumn


class AbsentUserController:
    
    # Mapeo de fallback como atributo de clase para evitar repetición
    FALLBACK_MAPPINGS = {
        'vacunacion': [
            '"Esquema de vacunación Regular"', '"Esquema de Vacunación COVID"', 
            '"Fecha más reciente de Vacunación"', '"BCG (Tuberculosis)"',
            '"Hepatitis B recién nacido"', '"Pentavalente primera dosis (2 meses)"',
            '"Pentavalente segunda dosis (4 meses)"', '"Pentavalente tercera dosis (6 meses)"',
            '"Polio (VOP) primera dosis (2 meses)"', '"Neumococo primera dosis (2 meses)"',
            '"Rotavirus primera dosis (2 meses)"'
        ],
        'medicina': [
            '"Consulta por Medicina 1 mes"', '"Consulta por Medicina General"',
            '"Consulta por Medicina 2 meses"', '"Consulta por Medicina 4 meses"',
            '"Consulta por Medicina 6 meses"', '"Consulta por Medicina 9 meses"',
            '"Consulta por Medicina 12 meses"', '"Consulta por Medicina 18 meses"'
        ],
        'nutricion': [
            '"Consulta por Nutrición"', '"Seguimiento Nutricional"',
            '"Valoración del estado nutricional"', '"Consulta por Nutrición y Dietética"'
        ],
        'pediatria': [
            '"Consulta por Pediatría"', '"Control de Crecimiento y Desarrollo"',
            '"Consulta especializada por Pediatría"'
        ],
        'enfermeria': ['"Consulta por Enfermería"', '"Procedimiento de enfermería"'],
        'odontologia': ['"Consulta por Odontología"', '"Procedimiento de odontología preventiva"']
    }

    def _normalize_keyword(self, keyword: str) -> str:
        """Normaliza palabra clave removiendo tildes y convirtiendo a minúsculas"""
        normalized = ''.join(
            c for c in unicodedata.normalize('NFD', keyword)
            if unicodedata.category(c) != 'Mn'
        ).lower().strip()
        return normalized[:-3] + 'ia' if normalized.endswith('ica') else normalized

    def _extract_clean_path(self, data_source: str) -> str:
        """Extrae la ruta limpia del data_source"""
        if data_source.startswith("read_parquet('") and data_source.endswith("')"):
            return data_source[14:-2]
        return data_source.strip("'")

    def _try_column_discovery(self, clean_path: str, method: str) -> list:
        """Método unificado para intentar descubrir columnas con diferentes estrategias"""
        try:
            if method == 'parquet_schema':
                result = duckdb_service.conn.execute(
                    f"SELECT name FROM parquet_schema('{clean_path}')"
                ).fetchall()
                columns = [row[0] for row in result if row[0]]
            elif method == 'limit_zero':
                result = duckdb_service.conn.execute(f"SELECT * FROM '{clean_path}' LIMIT 0")
                columns = [desc[0] for desc in result.description] if result.description else []
            elif method == 'describe':
                result = duckdb_service.conn.execute(
                    f"DESCRIBE SELECT * FROM '{clean_path}'"
                ).fetchall()
                columns = [row[0] for row in result]
            elif method == 'pandas':
                import pandas as pd
                df_sample = pd.read_parquet(clean_path, nrows=1)
                columns = df_sample.columns.tolist()
            else:
                return []
            
            if columns:
                print(f"✓ {method} exitoso: {len(columns)} columnas")
            return columns
        except Exception as e:
            print(f"✗ {method} falló: {e}")
            return []

    def _get_available_columns(self, clean_path: str) -> list:
        """Obtiene columnas disponibles usando múltiples enfoques"""
        import os
        if not os.path.exists(clean_path):
            print(f"ARCHIVO NO EXISTE: {clean_path}")
            return []
        
        print(f"✓ Archivo encontrado: {os.path.getsize(clean_path):,} bytes")
        
        for method in ['parquet_schema', 'limit_zero', 'describe', 'pandas']:
            columns = self._try_column_discovery(clean_path, method)
            if columns:
                return columns
        
        print("TODOS LOS MÉTODOS FALLARON")
        return []

    def _find_matching_columns(self, keyword: str, available_columns: list) -> tuple:
        """Encuentra columnas que coincidan con una keyword"""
        normalized_keyword = self._normalize_keyword(keyword)
        activity_columns = []
        
        for col in available_columns:
            if normalized_keyword in self._normalize_keyword(col):
                try:
                    if ActivityColumn().is_activity_column(col):
                        activity_columns.append(f'"{col}"')
                except Exception:
                    activity_columns.append(f'"{col}"')
        
        status = "✓" if activity_columns else "✗"
        print(f"{status} {keyword}: {len(activity_columns)} columnas encontradas")
        return (normalized_keyword, activity_columns)

    def _discover_activity_columns(self, data_source: str, keywords: List[str]) -> Dict[str, List[str]]:
        """Descubre dinámicamente columnas de actividades"""
        try:
            clean_path = self._extract_clean_path(data_source)
            print(f"Descubriendo columnas en: {clean_path}")
            
            available_columns = self._get_available_columns(clean_path)
            if not available_columns:
                return self._get_fallback_mapping(keywords)
            
            discovered_mapping = {
                norm_key: cols 
                for keyword in keywords
                for norm_key, cols in [self._find_matching_columns(keyword, available_columns)]
                if cols
            }
            
            return discovered_mapping if discovered_mapping else self._get_fallback_mapping(keywords)
            
        except Exception as e:
            print(f"ERROR en descubrimiento: {e}")
            return self._get_fallback_mapping(keywords)

    def _get_fallback_mapping(self, keywords: List[str]) -> Dict[str, List[str]]:
        """Mapeo de fallback robusto"""
        print(f"Aplicando fallback para: {keywords}")
        result_mapping = {}
        
        for keyword in keywords:
            normalized_keyword = self._normalize_keyword(keyword)
            matched = False
            
            for known_key, known_columns in self.FALLBACK_MAPPINGS.items():
                if any([
                    normalized_keyword == known_key,
                    normalized_keyword in known_key,
                    known_key in normalized_keyword
                ]):
                    result_mapping[normalized_keyword] = known_columns
                    print(f"Fallback '{keyword}': {len(known_columns)} columnas ({known_key})")
                    matched = True
                    break
            
            if not matched:
                result_mapping[normalized_keyword] = self.FALLBACK_MAPPINGS['vacunacion']
                print(f"Fallback genérico '{keyword}': vacunacion")
        
        return result_mapping

    def get_inasistentes_report(
        self,
        filename: str,
        selected_months: List[int],
        selected_years: List[int] = None,
        selected_keywords: List[str] = None,
        corte_fecha: str = "2025-07-31",
        departamento: Optional[str] = None,
        municipio: Optional[str] = None,
        ips: Optional[str] = None,
        path_technical_note = ''
    ) -> Dict[str, Any]:
        """Genera reporte de inasistentes dinámico con descubrimiento automático de actividades"""
        try:
            print(f"Iniciando reporte: {filename}")
            
            # Configurar fuente de datos
            file_key = f"technical_{filename.replace('.', '_').replace(' ', '_').replace('-', '_')}"
            data_source = DataSourceService(path_technical_note).ensure_data_source_available(filename, file_key)
            
            # Configurar keywords con fallback
            selected_keywords = selected_keywords or ['medicina']
            
            # Descubrir columnas
            discovered_columns = self._discover_activity_columns(data_source, selected_keywords)
            all_activity_columns = [col for cols in discovered_columns.values() for col in cols]
            
            if not all_activity_columns:
                return {
                    "success": False,
                    "error": f"No se encontraron columnas para: {selected_keywords}",
                    "inasistentes_por_actividad": []
                }
            
            # Construir filtros
            age_conditions = []
            if selected_months:
                age_conditions.append(
                    f"date_diff('month', strptime(\"Fecha Nacimiento\", '%d/%m/%Y'), DATE '{corte_fecha}') "
                    f"IN ({','.join(map(str, selected_months))})"
                )
            if selected_years:
                age_conditions.append(f"TRY_CAST(edad AS INTEGER) IN ({','.join(map(str, selected_years))})")
            
            if not age_conditions:
                return {
                    "success": False,
                    "error": "Debe seleccionar al menos una edad",
                    "inasistentes_por_actividad": []
                }
            
            age_filter = " OR ".join(age_conditions)
            
            geo_conditions = []
            if departamento:
                geo_conditions.append(f'"Departamento" = \'{departamento}\'')
            if municipio:
                geo_conditions.append(f'"Municipio" = \'{municipio}\'')
            if ips:
                geo_conditions.append(f'"Nombre IPS" = \'{ips}\'')
            geo_filter = " AND ".join(geo_conditions) if geo_conditions else "1=1"
            
            # Generar reportes
            activity_reports = ReportActivity().generate_activity_reports(
                data_source, all_activity_columns, age_filter, geo_filter, corte_fecha
            )
            
            # Calcular resumen
            total_inasistentes = sum(r["statistics"]["total_inasistentes"] for r in activity_reports)
            resumen_general = {
                "total_actividades_evaluadas": len(all_activity_columns),
                "total_inasistentes_global": total_inasistentes,
                "actividades_con_inasistentes": sum(1 for r in activity_reports if r["statistics"]["total_inasistentes"] > 0),
                "actividades_sin_inasistentes": sum(1 for r in activity_reports if r["statistics"]["total_inasistentes"] == 0)
            }
            
            print(f"Completado: {total_inasistentes} inasistentes")
            
            return {
                "success": True,
                "filename": filename,
                "corte_fecha": corte_fecha,
                "metodo": "DESCUBRIMIENTO_DINAMICO_ROBUSTO",
                "filtros_aplicados": {
                    "selected_months": selected_months,
                    "selected_years": selected_years or [],
                    "selected_keywords": selected_keywords,
                    "departamento": departamento,
                    "municipio": municipio,
                    "ips": ips
                },
                "columnas_descubiertas": {
                    k: [c.replace('"', '') for c in v] 
                    for k, v in discovered_columns.items()
                },
                "inasistentes_por_actividad": activity_reports,
                "resumen_general": resumen_general,
                "engine": "DuckDB_Dynamic_Discovery_v2"
            }
            
        except Exception as e:
            print(f"ERROR CRÍTICO: {e}")
            import traceback
            traceback.print_exc()
            return {
                "success": False,
                "error": str(e),
                "inasistentes_por_actividad": []
            }

    def export_inasistentes_to_csv(
        self,
        filename: str,
        selected_months: List[int],
        selected_years: List[int] = None,
        selected_keywords: List[str] = None,
        corte_fecha: str = "2025-07-31",
        departamento: Optional[str] = None,
        municipio: Optional[str] = None,
        ips: Optional[str] = None,
        path_technical_note = '',
        encoding: str = "cp1252",
        use_excel_sep_hint: bool = True,
        sep: str = ";"
    ) -> StreamingResponse:
        """Exporta reporte de inasistentes a CSV con encoding configurable"""
        
        # Generar reporte
        report_data = self.get_inasistentes_report(
            filename, selected_months, selected_years, selected_keywords,
            corte_fecha, departamento, municipio, ips, path_technical_note
        )
        
        if not report_data.get("success"):
            raise ValueError(report_data.get("error", "Error generando reporte"))
        
        # Extraer filas
        rows = [
            {
                "Departamento": r["departamento"],
                "Municipio": r["municipio"],
                "Nombre IPS": r["nombre_ips"],
                "Número Identificación": r["nro_identificacion"],
                "Primer Apellido": r["primer_apellido"],
                "Segundo Apellido": r["segundo_apellido"],
                "Primer Nombre": r["primer_nombre"],
                "Segundo Nombre": r["segundo_nombre"],
                "Fecha Nacimiento": r["fecha_nacimiento"],
                "Edad Años": r["edad_anos"],
                "Edad Meses": r["edad_meses"],
                "Actividad Faltante": r["columna_evaluada"],
                "Estado Actividad": r["actividad_valor"],
                "Grupo Actividad": activity_report["actividad"],
                "Fecha Corte": report_data["corte_fecha"],
            }
            for activity_report in report_data["inasistentes_por_actividad"]
            for r in activity_report["inasistentes"]
        ]
        
        # Crear DataFrame
        df = pd.DataFrame(rows)
        
        # Mapeo de encoding
        enc_map = {"cp1252": "cp1252", "latin-1": "latin-1", "utf-8-sig": "utf-8-sig"}
        enc = enc_map.get(encoding.lower(), "cp1252")
        
        # Escribir a buffer
        buf = io.BytesIO()
        if use_excel_sep_hint:
            buf.write(f"sep={sep}\n".encode(enc))
        
        df.to_csv(buf, index=False, encoding=enc, sep=sep, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
        buf.seek(0)
        
        # Construir nombre de archivo
        def sanitize(text):
            for old, new in [("ñ", "n"), ("Ñ", "N"), ("á", "a"), ("é", "e"), ("í", "i"), 
                           ("ó", "o"), ("ú", "u"), (" ", "-")]:
                text = text.replace(old, new)
            return text
        
        filters = []
        if selected_keywords:
            filters.append("palabras-" + "-".join(sanitize(k) for k in selected_keywords))
        if selected_months:
            filters.append("meses-" + "-".join(map(str, selected_months)))
        if selected_years:
            filters.append("años-" + "-".join(map(str, selected_years)))
        if departamento:
            filters.append("dept-" + sanitize(departamento))
        
        suffix = "_" + "_".join(filters) if filters else ""
        out_name = f"inasistentes_{filename.replace('.csv', '')}{suffix}_{corte_fecha}.csv"
        
        # Streaming
        def stream():
            buf.seek(0)
            while chunk := buf.read(8192):
                yield chunk
        
        charset_map = {"cp1252": "windows-1252", "latin-1": "ISO-8859-1"}
        charset = charset_map.get(enc, "utf-8")
        
        return StreamingResponse(
            stream(),
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": f'attachment; filename="{out_name}"',
                "Content-Type": f"text/csv; charset={charset}"
            }
        )
