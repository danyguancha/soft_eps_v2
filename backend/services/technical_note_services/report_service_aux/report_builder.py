from typing import Dict, Any, List, Optional


class ReportBuilder:
    """Responsable de construir items individuales del reporte"""
    
    def __init__(
        self,
        numerator_calculator,
        population_calculator,
        aggregation_calculator
    ):
        self.numerator_calculator = numerator_calculator
        self.population_calculator = population_calculator
        self.aggregation_calculator = aggregation_calculator
    
    def process_single_mapping(
        self,
        mapping: Dict[str, Any],
        column_name: str,
        data_source: str,
        where_clause: str,
        corte_fecha: str,
        anio_corte: int,
        mes_limite: int,
        meses_nombres: List[str],
        nt_rpms_integration: Optional[Any],
        depto: str,
        muni: str,
        ips_name: str
    ) -> Optional[Dict[str, Any]]:
        """Procesa un mapping individual y genera un item del reporte"""
        
        try:
            consolidado_info = mapping.get('consolidado', {})
            consulta_proc = consolidado_info.get('consulta_procedimiento', '')
            edad_aplicable = consolidado_info.get('edad_aplicable', '')
            edad_NT_RPMS = consolidado_info.get('edad_NT_RPMS', '')
            
            print(f"      Consulta: {consulta_proc}")
            print(f"      Edad aplicable: {edad_aplicable}")
            print(f"      Edad NT_RPMS: {edad_NT_RPMS}")
            
            rpms_data = self._get_rpms_data(
                nt_rpms_integration, consulta_proc, edad_NT_RPMS,
                depto, muni, ips_name
            )
            
            poblaciones_mensuales = self.population_calculator.get_population_by_predefined_dates(
                data_source=data_source,
                where_clause=where_clause,
                edad_key=edad_aplicable,
                corte_fecha=corte_fecha
            )
            
            poblacion_obj_anual = self._calculate_poblacion_anual(
                poblaciones_mensuales, mes_limite
            )
            
            if poblacion_obj_anual == 0:
                print("      Población = 0, omitiendo item")
                return None
            
            rpms_values = self._calculate_rpms_values(
                rpms_data, poblacion_obj_anual
            )
            
            numeradores_mensuales = self.numerator_calculator.calculate_by_month_simple(
                data_source=data_source,
                column_name=column_name,
                where_clause=where_clause,
                anio_corte=anio_corte,
                mes_limite=mes_limite
            )
            
            mensual_data = self._build_mensual_data(
                meses_nombres, mes_limite, poblaciones_mensuales,
                poblacion_obj_anual, numeradores_mensuales,
                rpms_values['poblacion_susceptible_mensual']
            )
            
            trimestres = self.aggregation_calculator.calculate_trimestres(
                mensual_data, meses_nombres, mes_limite
            )
            semestres = self.aggregation_calculator.calculate_semestres(trimestres)
            anual = self.aggregation_calculator.calculate_anual(semestres)
            
            item = {
                "consulta_procedimiento": consulta_proc or column_name,
                "rango_edad": edad_aplicable,
                "cups": rpms_values['cups'],
                "frecuencia_indicada": rpms_values['frecuencia_indicada'],
                "poblacion_objeto": poblacion_obj_anual,
                "periodo": rpms_values['periodo'],
                "frecuencia_uso_ips": rpms_values['frecuencia_uso_ips'],
                "fecuencia_ajustada_anual": rpms_values['frecuencia_ajustada_anual'],
                "meta": rpms_values['meta'],
                "poblacion_susceptible_anual": rpms_values['poblacion_susceptible_anual'],
                "poblacion_susceptible_mensual": rpms_values['poblacion_susceptible_mensual'],
                "proyeccion_tiempo": rpms_values['proyeccion_tiempo'],
                **{mes: mensual_data[mes] for mes in meses_nombres},
                **trimestres,
                **semestres,
                "anual": anual
            }
            
            return item
            
        except Exception as e:
            print(f"      Error procesando mapping: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _get_rpms_data(
        self,
        nt_rpms_integration,
        consulta_proc: str,
        edad_NT_RPMS: str,
        depto: str,
        muni: str,
        ips_name: str
    ) -> Optional[Dict]:
        """Busca datos RPMS"""
        if not nt_rpms_integration:
            return None
        
        return nt_rpms_integration.find_matching_row(
            consulta_procedimiento=consulta_proc,
            edad_nt_rpms=edad_NT_RPMS,
            departamento=depto,
            municipio=muni,
            nombre_ips=ips_name
        )
    
    def _calculate_poblacion_anual(
        self,
        poblaciones_mensuales: Dict[int, int],
        mes_limite: int
    ) -> int:
        """Calcula población objeto anual"""
        if -1 in poblaciones_mensuales:
            poblacion = poblaciones_mensuales[-1]
            print(f"      Población objeto (años - conteo único): {poblacion}")
        else:
            poblacion = sum(
                poblaciones_mensuales.get(i, 0)
                for i in range(1, mes_limite + 1)
            )
            print(f"      Población objeto (meses - suma): {poblacion}")
        
        return poblacion
    
    def _calculate_rpms_values(
        self,
        rpms_data: Optional[Dict],
        poblacion_obj_anual: int
    ) -> Dict:
        """Calcula valores derivados de RPMS"""
        if rpms_data:
            cups = rpms_data.get('cups', '')
            periodo = rpms_data.get('periodo', 'ANUAL')
            frecuencia_indicada = rpms_data.get('frecuencia_indicada', 0)
            proyeccion_tiempo = rpms_data.get('proyeccion_tiempo', 12)
            frecuencia_uso_ips = self._get_frecuencia_uso_ips(
                frecuencia_indicada, periodo, proyeccion_tiempo
            )
            meta = rpms_data.get('meta', 0)
            frecuencia_ajustada_anual = frecuencia_uso_ips * meta
            poblacion_susceptible_anual = (poblacion_obj_anual * frecuencia_uso_ips) * meta
            poblacion_susceptible_mensual = (
                poblacion_susceptible_anual / proyeccion_tiempo
                if proyeccion_tiempo > 0 else 0
            )
            
            print("      RPMS encontrado:")
            print(f"         Meta: {meta}, Freq: {frecuencia_uso_ips}, Proy: {proyeccion_tiempo}")
            print(f"         Pob susceptible anual: {poblacion_susceptible_anual:.2f}")
            print(f"         Pob susceptible mensual: {poblacion_susceptible_mensual:.2f}")
            
            return {
                'cups': cups,
                'periodo': periodo,
                'frecuencia_indicada': frecuencia_indicada,
                'proyeccion_tiempo': proyeccion_tiempo,
                'frecuencia_uso_ips': frecuencia_uso_ips,
                'meta': meta,
                'frecuencia_ajustada_anual': round(frecuencia_ajustada_anual, 1),
                'poblacion_susceptible_anual': round(poblacion_susceptible_anual, 0),
                'poblacion_susceptible_mensual': round(poblacion_susceptible_mensual, 0)
            }
        else:
            print("      RPMS NO encontrado - valores en 0")
            return {
                'cups': '',
                'periodo': '',
                'frecuencia_indicada': 0,
                'proyeccion_tiempo': 0,
                'frecuencia_uso_ips': 0,
                'meta': 0,
                'frecuencia_ajustada_anual': 0,
                'poblacion_susceptible_anual': 0,
                'poblacion_susceptible_mensual': 0
            }
    
    def _get_frecuencia_uso_ips(
        self,
        frecuencia_indicada: float,
        periodo: str,
        proyeccion_tiempo: int
    ) -> float:
        """Obtiene frecuencia de uso según periodo"""
        periodos = {
            'anual': 12,
            'semestral': 6,
            'trimestral': 4,
            'mensual': 12,
            'bienal': 24,
            'quinquenal': 60,
            'trienal': 36
        }
        
        periodo_key = periodo.lower().strip()
        valor_periodo = periodos.get(periodo_key)
        if valor_periodo is None:
            raise ValueError(f"Periodo no soportado: {periodo}")
        
        return round((frecuencia_indicada / valor_periodo) * proyeccion_tiempo, 1)
    
    def _build_mensual_data(
        self,
        meses_nombres: List[str],
        mes_limite: int,
        poblaciones_mensuales: Dict[int, int],
        poblacion_obj_anual: int,
        numeradores_mensuales: Dict[int, int],
        poblacion_susceptible_mensual: float
    ) -> Dict:
        """Construye datos mensuales"""
        mensual_data = {}
        
        for mes_num in range(1, 13):
            mes_nombre = meses_nombres[mes_num - 1]
            
            if mes_num <= mes_limite:
                if -1 in poblaciones_mensuales:
                    poblacion_objeto_mes = poblacion_obj_anual
                else:
                    poblacion_objeto_mes = poblaciones_mensuales.get(mes_num, 0)
                
                numerador = numeradores_mensuales.get(mes_num, 0)
                denominador = round(poblacion_susceptible_mensual, 0)
                
                mensual_data[mes_nombre] = {
                    "poblacion_objeto": poblacion_objeto_mes,
                    "numerador": numerador,
                    "denominador": denominador
                }
            else:
                mensual_data[mes_nombre] = {
                    "poblacion_objeto": 0,
                    "numerador": 0,
                    "denominador": 0
                }
        
        return mensual_data
