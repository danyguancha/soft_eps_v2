# controllers/technical_note_controller/absent_user_controller.py
from typing import Any, Dict, List, Optional
from datetime import datetime
from fastapi.responses import StreamingResponse



from services.technical_note_services.data_source_service import DataSourceService
from controllers.technical_note_controller.absent_user.absent_calculator import AbsentCalculator
from controllers.technical_note_controller.absent_user.absent_report_builder import AbsentReportBuilder
from controllers.technical_note_controller.absent_user.absent_exporter import AbsentExporter
from controllers.technical_note_controller.absent_user.report_helper import ReportHelper
from services.technical_note_services.report_service_aux.column_matcher import ColumnMatcher




class AbsentUserController:
    """
    Controlador orquestador para reportes de inasistentes.
    Usa columna 'Edad' del dataset para filtrado.
    """
    
    def __init__(self):
        """
        Inicializa el controlador sin necesidad de rangos de fechas de nacimiento.
        Ahora usa directamente la columna 'Edad' del dataset.
        """
        # Inicializar calculator SIN parámetros
        absent_calculator = AbsentCalculator()
        self.report_builder = AbsentReportBuilder(absent_calculator, ColumnMatcher)
        print("✓ AbsentUserController inicializado (modo Edad)")
    
    def get_inasistentes_report(
        self,
        filename: str,
        keywords: List[str] = None,
        corte_fecha: str = "2025-10-31",
        departamento: Optional[str] = None,
        municipio: Optional[str] = None,
        ips: Optional[str] = None,
        path_technical_note: str = '',
        column_mappings: Optional[Dict[str, Any]] = None,
        regimen: Optional[str] = None  # ← PARÁMETRO
    ) -> Dict[str, Any]:
        """
        Genera reporte de inasistentes mes a mes usando columna 'Edad'.
        
        Args:
            filename: Nombre del archivo CSV
            keywords: Palabras clave para filtrar columnas
            corte_fecha: Fecha de corte (YYYY-MM-DD)
            departamento: Filtro por departamento
            municipio: Filtro por municipio
            ips: Filtro por IPS
            path_technical_note: Path base de archivos
            column_mappings: Mappings de columnas
            regimen: Filtro por régimen (Subsidiado/Contributivo)
        
        Returns:
            Diccionario con reporte completo
        """
        try:
            print(f"\n{'='*80}")
            print("GENERANDO REPORTE DE INASISTENTES (POR EDAD)")
            print(f"{'='*80}")
            print(f"Archivo: {filename}")
            print(f"Fecha de corte: {corte_fecha}")
            print(f"Régimen RECIBIDO: '{regimen}' (tipo: {type(regimen)})")  # ← LOG DETALLADO
            print(f"Departamento: {departamento}")
            print(f"Municipio: {municipio}")
            print(f"IPS: {ips}")
            
            # Parse fecha de corte
            corte_dt = datetime.strptime(corte_fecha, '%Y-%m-%d')
            mes_limite = corte_dt.month
            anio_corte = corte_dt.year
            
            # Obtener data source
            file_key = f"technical_{filename.replace('.', '_').replace(' ', '_').replace('-', '_')}"
            data_source = DataSourceService(path_technical_note).ensure_data_source_available(
                filename, file_key
            )
            
            print(f"✓ Data source obtenido")
            
            # Validar columna 'Edad'
            all_columns = ReportHelper.get_dataset_columns(data_source)
            if 'Edad' not in all_columns:
                print(f"⚠️  Columna 'Edad' no encontrada en el dataset")
                print(f"   Columnas disponibles: {all_columns[:10]}...")
                return {
                    "success": False,
                    "error": "Columna 'Edad' no encontrada en el dataset",
                    "inasistentes_por_actividad": []
                }
            
            print(f"✓ Columna 'Edad' validada")
            
            # Preparar parámetros
            keywords = keywords or ['medicina']
            
            # ← NUEVO: LOG ANTES DE CREAR geographic_info
            print(f"\n{'🚨'*40}")
            print(f"CREANDO geographic_info:")
            print(f"   departamento (param): {departamento}")
            print(f"   municipio (param): {municipio}")
            print(f"   ips (param): {ips}")
            print(f"   regimen (param): '{regimen}'")
            print(f"{'🚨'*40}")
            
            geographic_info = {
                'depto': departamento, 
                'muni': municipio, 
                'ips_name': ips,
                'regimen': regimen  # ← Agregar régimen
            }
            
            # ← NUEVO: LOG DESPUÉS DE CREAR geographic_info
            print(f"\n{'✅'*40}")
            print(f"geographic_info CREADO:")
            print(f"   Contenido completo: {geographic_info}")
            print(f"   Tiene 'regimen'?: {'regimen' in geographic_info}")
            print(f"   Valor de 'regimen': '{geographic_info.get('regimen')}'")
            print(f"   Tipo: {type(geographic_info.get('regimen'))}")
            print(f"{'✅'*40}\n")
            
            # Validar filtros geográficos
            if not ReportHelper.has_geographic_filters(geographic_info):
                print("⚠️  Sin filtros geográficos - retornando reporte vacío")
                return ReportHelper.build_empty_report(
                    filename, corte_fecha, keywords, geographic_info
                )
            
            # ← NUEVO: LOG ANTES DE build_where_clause
            print(f"\n{'📋'*40}")
            print(f"LLAMANDO A build_where_clause con:")
            print(f"   {geographic_info}")
            print(f"{'📋'*40}\n")
            
            where_clause = ReportHelper.build_where_clause(geographic_info)
            
            print(f"\n{'✓'*40}")
            print(f"WHERE CLAUSE RESULTANTE:")
            print(f"   {where_clause}")
            print(f"   Contiene 'Régimen'?: {'Régimen' in where_clause}")
            print(f"{'✓'*40}\n")
            
            print(f"Dataset: {len(all_columns)} columnas")
            
            # Encontrar columnas que coincidan con keywords
            matching_columns = ColumnMatcher.find_matching_columns(
                all_columns, column_mappings, keywords
            )
            
            if not matching_columns:
                print("⚠️  No se encontraron columnas que coincidan con keywords")
                return ReportHelper.build_empty_report(
                    filename, corte_fecha, keywords, geographic_info
                )
            
            print(f"✓ Columnas encontradas: {len(matching_columns)}")
            
            # Procesar todas las columnas
            report_items = self.report_builder.process_all_columns(
                matching_columns=matching_columns,
                column_mappings=column_mappings,
                data_source=data_source,
                where_clause=where_clause,
                anio_corte=anio_corte,
                mes_limite=mes_limite
            )
            
            # Calcular totales
            total_inasistentes = sum(item['total_inasistentes'] for item in report_items)
            actividades_con_inasistentes = sum(
                1 for item in report_items if item['total_inasistentes'] > 0
            )
            actividades_sin_inasistentes = sum(
                1 for item in report_items if item['total_inasistentes'] == 0
            )
            
            print(f"\n{'='*80}")
            print(f"Consultas procesadas: {len(report_items)}")
            print(f"Total inasistentes: {total_inasistentes}")
            print(f"Con inasistentes: {actividades_con_inasistentes}")
            print(f"Sin inasistentes: {actividades_sin_inasistentes}")
            print(f"{'='*80}\n")
            
            return {
                "success": True,
                "filename": filename,
                "corte_fecha": corte_fecha,
                "metodo": "INASISTENTES_POR_EDAD_V1",
                "filtros_aplicados": {
                    "keywords": keywords,
                    "departamento": departamento,
                    "municipio": municipio,
                    "ips": ips,
                    "regimen": regimen  # ← Incluir en respuesta
                },
                "inasistentes_por_actividad": report_items,
                "resumen_general": {
                    "total_actividades_evaluadas": len(matching_columns),
                    "total_inasistentes_global": total_inasistentes,
                    "actividades_con_inasistentes": actividades_con_inasistentes,
                    "actividades_sin_inasistentes": actividades_sin_inasistentes
                },
                "engine": "DuckDB_Edad_v1"
            }
            
        except Exception as e:
            print(f"❌ ERROR en get_inasistentes_report: {e}")
            import traceback
            traceback.print_exc()
            return {
                "success": False,
                "error": str(e),
                "inasistentes_por_actividad": [],
                "resumen_general": {
                    "total_actividades_evaluadas": 0,
                    "total_inasistentes_global": 0,
                    "actividades_con_inasistentes": 0,
                    "actividades_sin_inasistentes": 0
                }
            }
    
    def export_inasistentes_to_csv(
        self,
        filename: str,
        keywords: List[str] = None,
        corte_fecha: str = "2025-10-31",
        departamento: Optional[str] = None,
        municipio: Optional[str] = None,
        ips: Optional[str] = None,
        path_technical_note: str = '',
        column_mappings: Optional[Dict[str, Any]] = None,
        regimen: Optional[str] = None,  # ← PARÁMETRO
        encoding: str = "utf-8-sig",
        use_excel_sep_hint: bool = False,
        sep: str = ";"
    ) -> StreamingResponse:
        """
        Exporta reporte de inasistentes a CSV.
        """
        try:
            print(f"\n{'='*80}")
            print("EXPORTANDO REPORTE DE INASISTENTES A CSV")
            print(f"{'='*80}")
            print(f"Régimen para export: '{regimen}'")
            
            # Generar reporte
            report_data = self.get_inasistentes_report(
                filename=filename,
                keywords=keywords,
                corte_fecha=corte_fecha,
                departamento=departamento,
                municipio=municipio,
                ips=ips,
                path_technical_note=path_technical_note,
                column_mappings=column_mappings,
                regimen=regimen  # ← Pasar régimen
            )
            
            if not report_data.get("success"):
                raise Exception(report_data.get("error", "Error generando reporte"))
            
            # Exportar a CSV
            csv_response = AbsentExporter.export_to_csv(
                report_data=report_data,
                filename=filename,
                encoding=encoding,
                use_excel_sep_hint=use_excel_sep_hint,
                sep=sep
            )
            
            print(f"✅ CSV exportado exitosamente")
            return csv_response
            
        except Exception as e:
            print(f"❌ ERROR exportando CSV: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def validate_inasistentes_source(
        self,
        filename: str,
        path_technical_note: str = ''
    ) -> Dict[str, Any]:
        """Valida que el archivo tenga estructura válida para reportes."""
        try:
            file_key = f"technical_{filename.replace('.', '_').replace(' ', '_').replace('-', '_')}"
            data_source = DataSourceService(path_technical_note).ensure_data_source_available(
                filename, file_key
            )
            
            all_columns = ReportHelper.get_dataset_columns(data_source)
            
            # Validar columna Edad
            has_edad = 'Edad' in all_columns
            
            errors = []
            warnings = []
            
            if not has_edad:
                errors.append("Columna 'Edad' no encontrada")
            
            # Validar columnas básicas requeridas
            required_cols = [
                'Departamento', 'Municipio', 'Nombre IPS',
                'Nro Identificación', 'Primer Nombre', 'Primer Apellido'
            ]
            
            missing_cols = [col for col in required_cols if col not in all_columns]
            if missing_cols:
                errors.append(f"Columnas faltantes: {', '.join(missing_cols)}")
            
            # Si tiene Edad, validar datos
            if has_edad:
                calculator = AbsentCalculator()
                if not calculator.validate_edad_column(data_source):
                    warnings.append("Columna 'Edad' tiene valores inválidos")
            
            is_valid = len(errors) == 0
            
            return {
                "valid": is_valid,
                "has_edad_column": has_edad,
                "total_columns": len(all_columns),
                "required_columns_present": len(missing_cols) == 0,
                "errors": errors,
                "warnings": warnings,
                "columns_sample": all_columns[:20]
            }
            
        except Exception as e:
            return {
                "valid": False,
                "error": str(e),
                "errors": [str(e)],
                "warnings": []
            }
    
    def get_edad_distribution(
        self,
        filename: str,
        path_technical_note: str = '',
        departamento: Optional[str] = None,
        municipio: Optional[str] = None,
        ips: Optional[str] = None,
        regimen: Optional[str] = None  # ← PARÁMETRO
    ) -> Dict[str, Any]:
        """Obtiene distribución de edades en el dataset."""
        try:
            file_key = f"technical_{filename.replace('.', '_').replace(' ', '_').replace('-', '_')}"
            data_source = DataSourceService(path_technical_note).ensure_data_source_available(
                filename, file_key
            )
            
            geographic_info = {
                'depto': departamento,
                'muni': municipio,
                'ips_name': ips,
                'regimen': regimen
            }
            
            where_clause = ReportHelper.build_where_clause(geographic_info)
            
            calculator = AbsentCalculator()
            
            # Obtener estadísticas
            stats = calculator.get_edad_statistics(data_source, where_clause)
            
            # Obtener distribución
            distribution = calculator.get_edad_distribution(data_source, where_clause)
            
            return {
                "estadisticas": stats,
                "distribucion": distribution
            }
            
        except Exception as e:
            print(f"❌ Error obteniendo distribución: {e}")
            return {
                "error": str(e),
                "estadisticas": {},
                "distribucion": []
            }
