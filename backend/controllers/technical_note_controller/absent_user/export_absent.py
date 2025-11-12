

import io
from typing import List, Optional
from pandas import pd
from fastapi.responses import StreamingResponse


class ExportAbsent:
    def _process_inasistente_record(self, inasistente: dict, activity_report: dict, 
                                    corte_fecha: str, metodo: str) -> dict:
        """Procesa un registro individual de inasistente"""
        return {
            # Información personal
            "Departamento": inasistente["departamento"],
            "Municipio": inasistente["municipio"], 
            "Nombre IPS": inasistente["nombre_ips"],
            "Número Identificación": inasistente["nro_identificacion"],
            "Primer Apellido": inasistente["primer_apellido"],
            "Segundo Apellido": inasistente["segundo_apellido"],
            "Primer Nombre": inasistente["primer_nombre"],
            "Segundo Nombre": inasistente["segundo_nombre"],
            "Fecha Nacimiento": inasistente["fecha_nacimiento"],
            "Edad Años": inasistente["edad_anos"],
            "Edad Meses": inasistente["edad_meses"],
            
            # Información de la actividad
            "Actividad Faltante": inasistente["columna_evaluada"],
            "Estado Actividad": inasistente["actividad_valor"],
            "Grupo Actividad": activity_report["actividad"],
            
            # Metadatos
            "Fecha Corte": corte_fecha,
            "Método Generación": metodo
        }


    def _extract_all_records(self, report_data: dict) -> list:
        """Extrae todos los registros del reporte"""
        all_records = []
        
        for activity_report in report_data["inasistentes_por_actividad"]:
            for inasistente in activity_report["inasistentes"]:
                record = self._process_inasistente_record(
                    inasistente, 
                    activity_report, 
                    report_data["corte_fecha"],
                    report_data["metodo"]
                )
                all_records.append(record)
        
        return all_records


    def _create_dataframe(self, all_records: list) -> pd.DataFrame:
        """Crea DataFrame vacío o con datos"""
        if not all_records:
            return pd.DataFrame(columns=[
                "Departamento", "Municipio", "Nombre IPS", "Número Identificación",
                "Primer Apellido", "Segundo Apellido", "Primer Nombre", "Segundo Nombre",
                "Fecha Nacimiento", "Edad Años", "Edad Meses", "Actividad Faltante",
                "Estado Actividad", "Grupo Actividad", "Fecha Corte", "Método Generación"
            ])
        return pd.DataFrame(all_records)


    def _sanitize_text(self, text: str) -> str:
        """Remueve caracteres especiales de texto"""
        replacements = {'ñ': 'n', 'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u'}
        for old, new in replacements.items():
            text = text.replace(old, new)
        return text


    def _build_filename(self, filename: str, selected_keywords: list, selected_months: list,
                        selected_years: list, departamento: str, corte_fecha: str) -> str:
        """Construye el nombre del archivo CSV con filtros"""
        filters_info = []
        
        if selected_keywords:
            keywords_safe = [self._sanitize_text(kw) for kw in selected_keywords]
            filters_info.append(f"palabras-{'-'.join(keywords_safe)}")
        
        if selected_months:
            filters_info.append(f"meses-{'-'.join(map(str, selected_months))}")
        
        if selected_years:
            filters_info.append(f"años-{'-'.join(map(str, selected_years))}")
        
        if departamento:
            dept_safe = self._sanitize_text(departamento.replace(' ', '-'))
            filters_info.append(f"dept-{dept_safe}")
        
        filter_suffix = "_" + "_".join(filters_info) if filters_info else ""
        return f"inasistentes_{filename.replace('.csv', '')}{filter_suffix}_{corte_fecha}.csv"


    def _create_csv_buffer(self, df: pd.DataFrame) -> io.BytesIO:
        """Genera CSV con UTF-8-SIG en buffer binario"""
        buffer = io.BytesIO()
        df.to_csv(buffer, index=False, encoding='utf-8-sig')
        buffer.seek(0)
        return buffer


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
        path_technical_note = ''
    ) -> StreamingResponse:
        """Exporta reporte de inasistentes a CSV con encoding UTF-8 correcto"""
        try:
            print(f"📊 Exportando reporte a CSV: {filename}")
            
            # PASO 1: Generar reporte completo
            report_data = self.get_inasistentes_report(
                filename, selected_months, selected_years, selected_keywords,
                corte_fecha, departamento, municipio, ips, path_technical_note
            )
            
            if not report_data.get("success"):
                raise ValueError(report_data.get("error", "Error generando reporte"))
            
            # PASO 2: Extraer registros
            all_records = self._extract_all_records(report_data)
            
            # PASO 3: Crear DataFrame
            df = self._create_dataframe(all_records)
            
            # PASO 4: Generar CSV en buffer
            buffer = self._create_csv_buffer(df)
            
            # PASO 5: Construir nombre de archivo
            csv_filename = self._build_filename(
                filename, selected_keywords, selected_months, 
                selected_years, departamento, corte_fecha
            )
            
            print(f"CSV generado correctamente: {len(all_records)} registros")
            print("Headers con tildes preservados en contenido del archivo")
            
            # PASO 6: Función generadora para streaming
            def iter_csv():
                buffer.seek(0)
                while True:
                    chunk = buffer.read(8192)  # Leer en chunks de 8KB
                    if not chunk:
                        break
                    yield chunk
            
            # PASO 7: Retornar streaming response
            return StreamingResponse(
                iter_csv(),
                media_type="application/octet-stream",
                headers={
                    "Content-Disposition": f"attachment; filename=\"{csv_filename}\"",
                    "Content-Type": "text/csv; charset=utf-8"
                }
            )
            
        except Exception as e:
            print(f"❌ Error exportando CSV: {e}")
            import traceback
            traceback.print_exc()
            raise ValueError(f"Error exportando reporte: {str(e)}")
