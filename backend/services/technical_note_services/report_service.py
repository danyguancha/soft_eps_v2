# services/technical_note_services/report_service.py

from typing import Dict, Any, List, Optional
from datetime import datetime

from services.duckdb_service.duckdb_service import duckdb_service
from services.technical_note_services.nt_rpms_integration import NTRPMSIntegration
from services.technical_note_services.report_service_aux.corrected_months import CorrectedMonths
from services.technical_note_services.report_service_aux.corrected_years import CorrectedYear
from services.technical_note_services.report_service_aux.identity_document import IdentityDocument
from services.technical_note_services.report_service_aux.config_loader import ConfigLoader
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
        column_mappings: Optional[Dict[str, Any]] = None,
        regimen: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Genera reporte con numerador SIN filtro de edad + ordenamiento por edad.
        
        🔥 CAMBIO CRÍTICO: Procesa TODOS los mappings individualmente,
        sin deduplicar por columna_usuario. Cada (columna + edad) es un item separado.
        """
        try:
            print(f"\n{'='*80}")
            print("GENERANDO REPORTE CON NUMERADOR SIN FILTRO DE EDAD")
            print(f"{'='*80}")
            print(f"Fecha de corte: {corte_fecha}")
            print(f"Régimen: {regimen if regimen else 'Todos'}")
            if keywords:
                print(f"Keywords solicitadas: {keywords}")

            # Validar régimen si se proporciona
            if regimen and regimen.upper() not in ['SUBSIDIADO', 'CONTRIBUTIVO']:
                raise ValueError(
                    f"Régimen inválido: '{regimen}'. "
                    "Valores permitidos: 'Subsidiado' o 'Contributivo'"
                )

            corte_dt = datetime.strptime(corte_fecha, '%Y-%m-%d')
            mes_limite = corte_dt.month
            anio_corte = corte_dt.year

            print(f"Mes límite: {mes_limite} (hasta {corte_dt.strftime('%B %Y')})")

            nt_rpms_integration = self._initialize_rpms_integration(nt_rpms_data_source)

            geographic_info = self._extract_geographic_filters(geographic_filters)

            if not self._has_geographic_filters(geographic_info):
                print("Sin filtros geográficos - retornando reporte vacío")
                return self._empty_report(filename, corte_fecha, keywords, geographic_filters, regimen)

            # Detectar nombres REALES de columnas geográficas en el dataset
            geo_cols = self._detect_geo_columns(data_source)
            
            # Detectar columna de régimen
            regimen_col = self._detect_regimen_column(data_source)

            # WHERE geográfico + régimen robusto (UPPER + TRIM)
            where_clause = self._build_where_clause(
                geographic_info, 
                geo_cols,
                regimen=regimen,
                regimen_col=regimen_col
            )
            print(f"WHERE completo usado:\n  {where_clause}")

            all_columns = self._get_dataset_columns(data_source)
            print(f"Dataset tiene {len(all_columns)} columnas totales")

            # 🔥 CAMBIO CRÍTICO: Obtener TODOS los mappings relevantes
            # (sin deduplicar por columna)
            all_mappings = self._get_all_relevant_mappings(
                column_mappings, keywords, all_columns
            )

            if not all_mappings:
                self._print_no_columns_found_debug(keywords, all_columns)
                return self._empty_report(filename, corte_fecha, keywords, geographic_filters, regimen)

            print(f"\n{len(all_mappings)} mappings totales para procesar")

            # Mostrar distribución por columna
            self._print_mappings_distribution(all_mappings)

            # 🔥 Procesar TODOS los mappings individualmente
            report_items = self._process_all_mappings(
                mappings=all_mappings,
                data_source=data_source,
                where_clause=where_clause,
                corte_fecha=corte_fecha,
                anio_corte=anio_corte,
                mes_limite=mes_limite,
                nt_rpms_integration=nt_rpms_integration,
                geographic_info=geographic_info,
                keywords=keywords
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
                'regimen': regimen,
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
        """Detecta nombres reales de columnas geográficas en el dataset"""
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

    def _detect_regimen_column(self, data_source: str) -> Optional[str]:
        """Detecta la columna de régimen en el dataset"""
        describe_query = f"DESCRIBE SELECT * FROM {data_source}"
        columns_result = self.conn.execute(describe_query).fetchall()
        cols = [row[0] for row in columns_result]
        cols_norm = {normalize_text(c): c for c in cols}

        candidates = [
            'Régimen',
            'Regimen',
            'Tipo Régimen',
            'Tipo Regimen',
            'Tipo_Regimen',
            'REGIMEN',
            'Regimen Afiliado',
            'Tipo de Régimen',
            'TipoRegimen',
            'Regimen de Afiliacion',
            'Régimen de Afiliación'
        ]

        for cand in candidates:
            cand_norm = normalize_text(cand)
            if cand_norm in cols_norm:
                detected = cols_norm[cand_norm]
                print(f"Columna de régimen detectada: '{detected}'")
                return detected

        print("⚠️ No se detectó columna de régimen en el dataset")
        return None

    def _build_where_clause(
        self, 
        geographic_info: Dict, 
        geo_cols: Dict[str, Optional[str]],
        regimen: Optional[str] = None,
        regimen_col: Optional[str] = None
    ) -> str:
        """Construye cláusula WHERE para filtros geográficos + régimen"""
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

        # Filtro de régimen
        if regimen and regimen_col:
            regimen_upper = regimen.upper().strip()
            parts.append(
                f"{norm_expr(regimen_col)} = '{regimen_upper}'"
            )
            print(f"   Filtro de régimen agregado: {regimen_upper}")
        elif regimen and not regimen_col:
            print(f"   ⚠️ ADVERTENCIA: Se solicitó filtrar por régimen '{regimen}' pero no se encontró la columna")

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
    # 🔥 NUEVOS MÉTODOS PARA NO DEDUPLICAR
    # ============================================================

    def _get_all_relevant_mappings(
        self,
        column_mappings: Dict,
        keywords: Optional[List[str]],
        dataset_columns: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Obtiene TODOS los mappings relevantes sin deduplicar por columna.
        
        Cada mapping (columna + edad) es independiente.
        
        Args:
            column_mappings: Diccionario con mappings completos
            keywords: Keywords para filtrar
            dataset_columns: Columnas disponibles en el dataset
            
        Returns:
            Lista de mappings filtrados
        """
        all_mappings = column_mappings.get('mappings', [])
        
        if not all_mappings:
            print("⚠️ No hay mappings en column_mappings")
            return []
        
        print(f"Analizando mappings...")
        print(f"   Total mappings en configuración: {len(all_mappings)}")
        
        # Normalizar keywords si existen
        kws_norm = {normalize_text(k) for k in (keywords or [])} if keywords else set()
        
        # Dataset columns normalizado
        dataset_cols_norm = {normalize_text(c): c for c in dataset_columns}
        
        # 1. Filtrar por keyword (si aplica)
        if kws_norm:
            filtered_by_kw = self._filter_mappings_by_keyword_flexible(all_mappings, kws_norm)
            print(f"   Después de filtrar por keywords: {len(filtered_by_kw)} mappings")
        else:
            filtered_by_kw = all_mappings
            print(f"   Sin filtro de keywords")
        
        # 2. Filtrar por existencia de columna en dataset
        final_mappings = []
        for m in filtered_by_kw:
            col_usuario = m.get('columna_usuario', '')
            col_norm = normalize_text(col_usuario)
            
            if col_norm in dataset_cols_norm:
                final_mappings.append(m)
        
        print(f"   Mappings con columna en dataset: {len(final_mappings)}")
        
        return final_mappings

    def _print_mappings_distribution(self, mappings: List[Dict[str, Any]]):
        """Muestra distribución de mappings por columna"""
        from collections import defaultdict
        
        by_column = defaultdict(list)
        for m in mappings:
            col = m.get('columna_usuario', '')
            edad = m.get('consolidado', {}).get('edad_aplicable', 'N/A')
            by_column[col].append(edad)
        
        print(f"\n📊 Distribución de mappings por columna:")
        for col, edades in sorted(by_column.items()):
            if len(edades) > 1:
                print(f"   '{col}': {len(edades)} items")
                for edad in edades:
                    print(f"      - Edad: {edad}")
            else:
                print(f"   '{col}': 1 item (Edad: {edades[0]})")

    def _process_all_mappings(
        self,
        mappings: List[Dict[str, Any]],
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
        Procesa TODOS los mappings individualmente.
        
        🔥 CRÍTICO: NO deduplica por columna. Cada mapping es procesado por separado.
        """
        report_items = []
        meses_nombres = [
            "enero", "febrero", "marzo", "abril", "mayo", "junio",
            "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"
        ]
        
        total = len(mappings)
        
        for idx, mapping in enumerate(mappings, 1):
            col_usuario = mapping.get('columna_usuario', '')
            edad_aplicable = mapping.get('consolidado', {}).get('edad_aplicable', 'N/A')
            
            print(f"\n[{idx}/{total}] Procesando: '{col_usuario}' (Edad: {edad_aplicable})")
            
            # 🔥 SIMPLIFICADO: No verificar población previa
            # El report_builder.process_single_mapping ya maneja población = 0 y retorna None
            
            # Detectar keyword para este mapping
            current_keyword = mapping.get('keyword', None)
            
            # Procesar mapping
            item = self.report_builder.process_single_mapping(
                mapping=mapping,
                column_name=col_usuario,
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
                print(f"      ✓ Item generado")
            else:
                print(f"      ⚠️ Item omitido (población = 0 o error)")
        
        return report_items


    def _filter_mappings_by_keyword_flexible(
        self,
        mappings: List[Dict[str, Any]],
        kws_norm: set
    ) -> List[Dict[str, Any]]:
        """Filtra mappings por keyword con lógica flexible"""
        if not kws_norm:
            return mappings
        
        filtered = []
        generic_words = {'VIH', 'PARA', 'DE', 'LA', 'EL', 'EN', 'Y', 'A', 'CON'}
        
        for mapping in mappings:
            mapping_kw = normalize_text(mapping.get('keyword', ''))
            
            if not mapping_kw:
                filtered.append(mapping)
                continue
            
            mapping_words = set(w for w in mapping_kw.split() if len(w) > 2)
            
            matched = False
            for kw_norm in kws_norm:
                kw_words = set(w for w in kw_norm.split() if len(w) > 2)
                
                # Coincidencia exacta
                if kw_norm == mapping_kw:
                    matched = True
                    break
                
                # Contención
                if kw_norm in mapping_kw or mapping_kw in kw_norm:
                    matched = True
                    break
                
                # Palabras compartidas
                common_words = kw_words & mapping_words
                
                if len(common_words) > 0:
                    specific_common_words = common_words - generic_words
                    
                    if len(specific_common_words) > 0:
                        matched = True
                        break
                    
                    elif len(mapping_words) == 1 and mapping_words <= common_words:
                        matched = True
                        break
            
            if matched:
                filtered.append(mapping)
        
        if len(filtered) == 0 and len(mappings) > 0:
            print(f"   ⚠️ ADVERTENCIA: Filtro eliminó todos los mappings")
            print(f"   Keywords buscadas: {kws_norm}")
            print(f"   Keywords en mappings: {[m.get('keyword', 'N/A') for m in mappings]}")
        
        return filtered

    # ============================================================
    # REPORTE VACÍO
    # ============================================================

    def _empty_report(
        self,
        filename: str,
        corte_fecha: str,
        keywords: List,
        geographic_filters: Dict,
        regimen: Optional[str] = None
    ) -> Dict[str, Any]:
        """Construye reporte vacío"""
        return {
            'success': True,
            'filename': filename,
            'corte_fecha': corte_fecha,
            'keywords': keywords or [],
            'geographic_filters': geographic_filters or {},
            'regimen': regimen,
            'data': [],
            'total_rows': 0,
            'meses_reportados': 0,
            'metodo': 'numerador_sin_filtro_edad_ordenado'
        }
