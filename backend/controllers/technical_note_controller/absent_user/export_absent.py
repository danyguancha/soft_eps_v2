

import io
from typing import List, Optional
from pandas import pd
from fastapi.responses import StreamingResponse


class ExportAbsent:
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
            print(f"Exportando reporte a CSV: {filename}")
            
            # GENERAR REPORTE COMPLETO
            report_data = self.get_inasistentes_report(
                filename, selected_months, selected_years, selected_keywords,
                corte_fecha, departamento, municipio, ips, path_technical_note
            )
            
            if not report_data.get("success"):
                raise Exception(report_data.get("error", "Error generando reporte"))
            
            # PROCESAR DATOS PARA CSV
            all_records = []
            
            for activity_report in report_data["inasistentes_por_actividad"]:
                for inasistente in activity_report["inasistentes"]:
                    record = {
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
                        "Fecha Corte": report_data["corte_fecha"],
                        "Método Generación": report_data["metodo"]
                    }
                    all_records.append(record)
            
            # CREAR DATAFRAME
            if not all_records:
                df = pd.DataFrame(columns=[
                    "Departamento", "Municipio", "Nombre IPS", "Número Identificación",
                    "Primer Apellido", "Segundo Apellido", "Primer Nombre", "Segundo Nombre",
                    "Fecha Nacimiento", "Edad Años", "Edad Meses", "Actividad Faltante",
                    "Estado Actividad", "Grupo Actividad", "Fecha Corte", "Método Generación"
                ])
            else:
                df = pd.DataFrame(all_records)
            
            # GENERAR CSV CON UTF-8-SIG EN BUFFER BINARIO
            buffer = io.BytesIO()
            df.to_csv(buffer, index=False, encoding='utf-8-sig')
            buffer.seek(0)
            
            # NOMBRE DEL ARCHIVO (sin caracteres problemáticos para el filename)
            filters_info = []
            if selected_keywords:
                # Limpiar caracteres especiales solo para el nombre del archivo
                keywords_safe = [kw.replace('ñ', 'n').replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u') for kw in selected_keywords]
                filters_info.append(f"palabras-{'-'.join(keywords_safe)}")
            if selected_months:
                filters_info.append(f"meses-{'-'.join(map(str, selected_months))}")
            if selected_years:
                filters_info.append(f"años-{'-'.join(map(str, selected_years))}")
            if departamento:
                dept_safe = departamento.replace(' ', '-').replace('ñ', 'n').replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u')
                filters_info.append(f"dept-{dept_safe}")
            
            filter_suffix = "_" + "_".join(filters_info) if filters_info else ""
            csv_filename = f"inasistentes_{filename.replace('.csv', '')}{filter_suffix}_{corte_fecha}.csv"
            
            print(f"CSV generado correctamente: {len(all_records)} registros")
            print(f"Headers con tildes preservados en contenido del archivo")
            
            # FUNCIÓN GENERADORA PARA STREAMING CORRECTO
            def iter_csv():
                buffer.seek(0)
                while True:
                    chunk = buffer.read(8192)  # Leer en chunks de 8KB
                    if not chunk:
                        break
                    yield chunk
            
            # RETORNAR STREAMING RESPONSE CORRECTO
            return StreamingResponse(
                iter_csv(),
                media_type="application/octet-stream",  # Forzar descarga binaria
                headers={
                    "Content-Disposition": f"attachment; filename=\"{csv_filename}\"",
                    "Content-Type": "text/csv; charset=utf-8"
                }
            )
            
        except Exception as e:
            print(f"Error exportando CSV: {e}")
            import traceback
            traceback.print_exc()
            raise Exception(f"Error exportando reporte: {str(e)}")