# services/technical_note_services/report_service.py

from typing import Dict, Any, List, Optional
from datetime import datetime

from services.duckdb_service.duckdb_service import duckdb_service
from services.technical_note_services.nt_rpms_integration import NTRPMSIntegration
from services.technical_note_services.report_service_aux.corrected_months import CorrectedMonths
from services.technical_note_services.report_service_aux.corrected_years import CorrectedYear
from services.technical_note_services.report_service_aux.identity_document import IdentityDocument
from services.technical_note_services.report_service_aux.config_loader import ConfigLoader
from services.technical_note_services.report_service_aux.column_matcher import ColumnMatcher
from services.technical_note_services.report_service_aux.numerator_calculator import NumeratorCalculator
from services.technical_note_services.report_service_aux.denominator_calculator import PopulationCalculator
from services.technical_note_services.report_service_aux.aggregation_calculator import AggregationCalculator
from services.technical_note_services.report_service_aux.report_builder import ReportBuilder
from services.technical_note_services.report_service_aux.report_sorter import ReportSorter
from utils.text_normalizer import normalize_text


class ReportService:
    """
    Servicio orquestador para generación de reportes.
    Delega responsabilidades específicas a componentes especializados.
    """

    def __init__(self):
        self.corrected_months = CorrectedMonths()
        self.corrected_years = CorrectedYear()
        self.identity_document = IdentityDocument()

        self.birth_date_ranges_den = ConfigLoader.load_birth_date_ranges_den()
        self.rangos_compuestos = ConfigLoader.load_rangos_compuestos()

        self._initialize_components()

    def _initialize_components(self):
        """Inicializa componentes especializados"""
        self.numerator_calculator = NumeratorCalculator()

        self.population_calculator = PopulationCalculator(
            self.birth_date_ranges_den,
            self.rangos_compuestos,
            self.corrected_months,
            self.corrected_years,
            self.identity_document
        )

        self.aggregation_calculator = AggregationCalculator()

        self.report_builder = ReportBuilder(
            self.numerator_calculator,
            self.population_calculator,
            self.aggregation_calculator
        )

    @property
    def conn(self):
        return duckdb_service.conn

    # ============================================================
    # MÉTODO PRINCIPAL
    # ============================================================

    def generate_keyword_age_report(
        self,
        data_source: str,
        filename: str,
        keywords: Optional[List[str]] = None,
        geographic_filters: Dict[str, Any] = None,
        corte_fecha: str = None,
        nt_rpms_data_source: Optional[str] = None,
        column_mappings: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Genera reporte con numerador SIN filtro de edad + ordenamiento por edad.
        Usa keywords para filtrar mappings (ej: DIU vs Subdérmico) y aplica filtros geográficos.
        """
        try:
            print(f"\n{'='*80}")
            print("GENERANDO REPORTE CON NUMERADOR SIN FILTRO DE EDAD")
            print(f"{'='*80}")
            print(f"Fecha de corte: {corte_fecha}")
            if keywords:
                print(f"Keywords solicitadas: {keywords}")

            corte_dt = datetime.strptime(corte_fecha, '%Y-%m-%d')
            mes_limite = corte_dt.month
            anio_corte = corte_dt.year

            print(f"Mes límite: {mes_limite} (hasta {corte_dt.strftime('%B %Y')})")

            nt_rpms_integration = self._initialize_rpms_integration(nt_rpms_data_source)

            geographic_info = self._extract_geographic_filters(geographic_filters)

            if not self._has_geographic_filters(geographic_info):
                print("Sin filtros geográficos - retornando reporte vacío")
                return self._empty_report(filename, corte_fecha, keywords, geographic_filters)

            # Detectar nombres REALES de columnas geográficas en el dataset
            geo_cols = self._detect_geo_columns(data_source)

            # WHERE geográfico robusto (UPPER + TRIM) aplicado a TODO
            where_clause = self._build_where_clause(geographic_info, geo_cols)
            print(f"WHERE geográfico usado:\n  {where_clause}")

            all_columns = self._get_dataset_columns(data_source)
            print(f"Dataset tiene {len(all_columns)} columnas totales")

            matching_columns = ColumnMatcher.find_matching_columns(
                all_columns, column_mappings, keywords
            )

            if not matching_columns:
                self._print_no_columns_found_debug(keywords, all_columns)
                return self._empty_report(filename, corte_fecha, keywords, geographic_filters)

            print(f"\n{len(matching_columns)} columnas encontradas desde mappings")

            report_items = self._process_all_columns(
                matching_columns=matching_columns,
                column_mappings=column_mappings,
                data_source=data_source,
                where_clause=where_clause,
                corte_fecha=corte_fecha,
                anio_corte=anio_corte,
                mes_limite=mes_limite,
                nt_rpms_integration=nt_rpms_integration,
                geographic_info=geographic_info,
                keywords=keywords  # ← se usan para filtrar mappings por keyword (DIU / subdérmico)
            )

            print(f"\n{'='*80}")
            print(f"Reporte generado: {len(report_items)} items")

            report_items_sorted = ReportSorter.sort_items_by_age(report_items)
            print("Items ordenados por edad")
            print(f"{'='*80}\n")

            return {
                'success': True,
                'filename': filename,
                'corte_fecha': corte_fecha,
                'keywords': keywords or [],
                'geographic_filters': geographic_filters or {},
                'data': report_items_sorted,
                'total_rows': len(report_items_sorted),
                'meses_reportados': mes_limite,
                'metodo': 'numerador_sin_filtro_edad_ordenado'
            }

        except Exception as e:
            print(f"\nERROR: {e}")
            import traceback
            traceback.print_exc()
            raise

    # ============================================================
    # INTEGRACIÓN NT_RPMS
    # ============================================================

    def _initialize_rpms_integration(self, nt_rpms_data_source: Optional[str]):
        """Inicializa integración RPMS si está disponible"""
        if nt_rpms_data_source:
            print("NT_RPMS integration inicializada")
            return NTRPMSIntegration(nt_rpms_data_source)
        return None

    # ============================================================
    # FILTROS GEOGRÁFICOS
    # ============================================================

    def _extract_geographic_filters(self, geographic_filters: Dict[str, Any]) -> Dict:
        """Extrae filtros geográficos"""
        depto = geographic_filters.get('departamento') if geographic_filters else None
        muni = geographic_filters.get('municipio') if geographic_filters else None
        ips_name = geographic_filters.get('ips') if geographic_filters else None

        print("Filtros geográficos:")
        print(f"   Departamento: {depto}")
        print(f"   Municipio: {muni}")
        print(f"   IPS: {ips_name}")

        return {'depto': depto, 'muni': muni, 'ips_name': ips_name}

    def _has_geographic_filters(self, geographic_info: Dict) -> bool:
        """Verifica si hay filtros geográficos"""
        return any([
            geographic_info['depto'],
            geographic_info['muni'],
            geographic_info['ips_name']
        ])

    def _detect_geo_columns(self, data_source: str) -> Dict[str, Optional[str]]:
        """
        Detecta nombres reales de columnas geográficas en el dataset,
        tolerando variaciones de escritura.
        """
        describe_query = f"DESCRIBE SELECT * FROM {data_source}"
        columns_result = self.conn.execute(describe_query).fetchall()
        cols = [row[0] for row in columns_result]
        cols_norm = {normalize_text(c): c for c in cols}

        def pick(candidates: List[str]) -> Optional[str]:
            for cand in candidates:
                cand_norm = normalize_text(cand)
                if cand_norm in cols_norm:
                    return cols_norm[cand_norm]
            return None

        detected = {
            'departamento': pick(['Departamento', 'DEPARTAMENTO', 'Depto', 'Departamento Residencia']),
            'municipio': pick(['Municipio', 'MUNICIPIO', 'Mpio']),
            'ips_name': pick(['Nombre IPS', 'IPS', 'Nombre_IPS', 'Nombre de IPS'])
        }

        print("Columnas geográficas detectadas:")
        for k, v in detected.items():
            print(f"   {k} -> {v}")

        return detected

    def _build_where_clause(self, geographic_info: Dict, geo_cols: Dict[str, Optional[str]]) -> str:
        """
        Construye cláusula WHERE para filtros geográficos.
        Usa UPPER + TRIM para hacer la comparación robusta. [web:21][web:22]
        """
        parts = []

        def norm_expr(col_name: str) -> str:
            return f"UPPER(TRIM(CAST(\"{col_name}\" AS VARCHAR)))"

        if geographic_info['depto'] and geo_cols.get('departamento'):
            parts.append(
                f"{norm_expr(geo_cols['departamento'])} = UPPER('{geographic_info['depto']}')"
            )

        if geographic_info['muni'] and geo_cols.get('municipio'):
            parts.append(
                f"{norm_expr(geo_cols['municipio'])} = UPPER('{geographic_info['muni']}')"
            )

        if geographic_info['ips_name'] and geo_cols.get('ips_name'):
            parts.append(
                f"{norm_expr(geo_cols['ips_name'])} = UPPER('{geographic_info['ips_name']}')"
            )

        return " AND ".join(parts) if parts else "1=1"

    # ============================================================
    # UTILIDADES SOBRE DATASET
    # ============================================================

    def _get_dataset_columns(self, data_source: str) -> List[str]:
        """Obtiene columnas del dataset"""
        describe_query = f"DESCRIBE SELECT * FROM {data_source}"
        columns_result = self.conn.execute(describe_query).fetchall()
        return [row[0] for row in columns_result]

    def _print_no_columns_found_debug(self, keywords: List, all_columns: List[str]):
        """Imprime información de debug cuando no se encuentran columnas"""
        print("\nNo se encontraron columnas del mapping en el dataset")
        print(f"   Keywords solicitadas: {keywords}")
        print("\n   Primeras 20 columnas del dataset:")
        for i, col in enumerate(all_columns[:20], 1):
            col_norm = normalize_text(col)
            print(f"      [{i:2}] '{col}' -> '{col_norm}'")

    # ============================================================
    # PROCESAMIENTO DE COLUMNAS / MAPPINGS
    # ============================================================

    def _process_all_columns(
        self,
        matching_columns: List[str],
        column_mappings: Dict,
        data_source: str,
        where_clause: str,
        corte_fecha: str,
        anio_corte: int,
        mes_limite: int,
        nt_rpms_integration,
        geographic_info: Dict,
        keywords: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Procesa todas las columnas encontradas.

        - Filtra mappings por la keyword seleccionada (ej: DIU vs subdermico).
        - Aplica filtro de población disponible.
        - Llama a ReportBuilder pasando la keyword para que NumeratorCalculator
          pueda aplicar el filtro especial de "Método Anticonceptivo".
        """
        report_items = []
        meses_nombres = [
            "enero", "febrero", "marzo", "abril", "mayo", "junio",
            "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"
        ]

        kws_norm = {normalize_text(k) for k in (keywords or [])}

        for col in matching_columns:
            print(f"\nProcesando columna: {col}")

            all_mappings = ColumnMatcher.find_all_mappings_for_column(col, column_mappings)
            if not all_mappings:
                print("   No se encontraron mappings para la columna")
                continue

            # 1) Filtrar mappings por keyword seleccionada (clave para DIU/Subdérmico)
            if kws_norm:
                before = len(all_mappings)
                all_mappings = [
                    m for m in all_mappings
                    if normalize_text(m.get('keyword', '')) in kws_norm
                ]
                print(f"   Mappings filtrados por keyword: {before} -> {len(all_mappings)}")

            if not all_mappings:
                print("   No quedaron mappings después de filtrar por keyword")
                continue

            # 2) Filtrar mappings que tengan población disponible
            valid_mappings = self.population_calculator.filter_mappings_by_population_availability(
                all_mappings, data_source, where_clause, corte_fecha
            )
            if not valid_mappings:
                print("   Ningún mapping tiene población disponible")
                continue
            print(f"   {len(valid_mappings)} mappings con población > 0")

            # 3) Detectar keyword actual para esta columna
            current_keyword = self._detect_keyword_for_column(col, keywords or [], all_mappings)
            print(f"   Keyword usada para esta columna: {current_keyword}")

            for idx, mapping in enumerate(valid_mappings, 1):
                print(f"\n   [{idx}/{len(valid_mappings)}] Procesando mapping...")
                item = self.report_builder.process_single_mapping(
                    mapping=mapping,
                    column_name=col,
                    data_source=data_source,
                    where_clause=where_clause,
                    corte_fecha=corte_fecha,
                    anio_corte=anio_corte,
                    mes_limite=mes_limite,
                    meses_nombres=meses_nombres,
                    nt_rpms_integration=nt_rpms_integration,
                    depto=geographic_info['depto'],
                    muni=geographic_info['muni'],
                    ips_name=geographic_info['ips_name'],
                    keyword=current_keyword
                )
                if item:
                    report_items.append(item)
                    print("      Item generado")

        return report_items

    def _detect_keyword_for_column(
        self,
        column_name: str,
        keywords: List[str],
        mappings: List[Dict]
    ) -> Optional[str]:
        """
        Detecta qué keyword corresponde a una columna específica,
        usando la keyword guardada en el mapping.
        """
        if not keywords or not mappings:
            return None

        kws_norm = [normalize_text(k) for k in keywords]

        for m in mappings:
            mk = normalize_text(m.get('keyword', ''))
            if mk in kws_norm:
                return m.get('keyword')

        return None

    # ============================================================
    # REPORTE VACÍO
    # ============================================================

    def _empty_report(
        self,
        filename: str,
        corte_fecha: str,
        keywords: List,
        geographic_filters: Dict
    ) -> Dict[str, Any]:
        """Construye reporte vacío"""
        return {
            'success': True,
            'filename': filename,
            'corte_fecha': corte_fecha,
            'keywords': keywords or [],
            'geographic_filters': geographic_filters or {},
            'data': [],
            'total_rows': 0,
            'meses_reportados': 0,
            'metodo': 'numerador_sin_filtro_edad_ordenado'
        }
