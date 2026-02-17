# services/technical_note_services/report_service_aux/report_builder.py

from typing import Dict, Any, List, Optional
from services.technical_note_services.report_service_aux.semaforization import Semaforization


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
        self.semaforization = Semaforization()  
    
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
        ips_name: str,
        keyword: str = None
    ) -> Optional[Dict[str, Any]]:
        try:
            consolidado_info = mapping.get('consolidado', {})
            consulta_proc = consolidado_info.get('consulta_procedimiento', '')
            edad_aplicable = consolidado_info.get('edad_aplicable', '')
            edad_NT_RPMS = consolidado_info.get('edad_NT_RPMS', '')

            # PASO 1: Obtener datos RPMS
            rpms_data = self._get_rpms_data(nt_rpms_integration, consulta_proc, edad_NT_RPMS, depto, muni, ips_name)
            
            # PASO 2: VERIFICAR SI ESTÁ DESHABILITADO ANTES DE CALCULAR NADA
            if rpms_data and rpms_data.get('habilitado') == False:
                print(f"      🚫 Servicio DESHABILITADO - Generando item con numerador/denominador en 0")
                return self._create_disabled_service_item(
                    consulta_proc or column_name,
                    edad_aplicable,
                    rpms_data,
                    meses_nombres,
                    mes_limite
                )
            
            # PASO 3: Servicio habilitado - Continuar con cálculos normales
            print(f"Servicio HABILITADO - Calculando poblaciones y numeradores")
            
            poblaciones_mensuales = self.population_calculator.get_population_by_predefined_dates(
                data_source=data_source, 
                where_clause=where_clause, 
                edad_key=edad_aplicable, 
                corte_fecha=corte_fecha,
                keyword=keyword 
            )
            
            poblacion_obj_anual = self._calculate_poblacion_anual(poblaciones_mensuales, mes_limite)
            
            if poblacion_obj_anual == 0:
                print(f"      ⚠️ Población = 0, omitiendo item")
                return None

            rpms_values = self._calculate_rpms_values(rpms_data, poblacion_obj_anual)

            # 🔥 MODIFICADO: Pasar edad_aplicable al numerator_calculator
            numeradores_mensuales = self.numerator_calculator.calculate_by_month_simple(
                data_source=data_source,
                column_name=column_name,
                where_clause=where_clause,
                anio_corte=anio_corte,
                mes_limite=mes_limite,
                keyword=keyword,
                corte_fecha=corte_fecha,
                edad_aplicable=edad_aplicable  # ← NUEVO PARÁMETRO
            )

            mensual_data = self._build_mensual_data(
                meses_nombres, mes_limite, poblaciones_mensuales,
                poblacion_obj_anual, numeradores_mensuales,
                rpms_values['poblacion_susceptible_mensual']
            )

            trimestres = self.aggregation_calculator.calculate_trimestres(mensual_data, meses_nombres, mes_limite)
            semestres = self.aggregation_calculator.calculate_semestres(trimestres)
            anual = self.aggregation_calculator.calculate_anual(semestres)

            return {
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
                "habilitado": True,
                **{mes: mensual_data[mes] for mes in meses_nombres},
                **trimestres, 
                **semestres, 
                "anual": anual
            }
        except Exception:
            import traceback
            traceback.print_exc()
            return None
    
    def _create_disabled_service_item(
        self,
        consulta_proc: str,
        edad_aplicable: str,
        rpms_data: Dict[str, Any],
        meses_nombres: List[str],
        mes_limite: int
    ) -> Dict[str, Any]:
        """
        Crea item para servicio DESHABILITADO con semaforización NA (gris).
        """
        # Mensual con todo en 0 y NA
        mensual_data = {}
        for mes_num in range(1, 13):
            mes_nombre = meses_nombres[mes_num - 1]
            mensual_data[mes_nombre] = {
                "poblacion_objeto": 0,
                "numerador": 0,
                "denominador": 0,
                "cobertura": 0.0,
                "semaforizacion": "NA",
                "color": "#808080"  # Gris
            }
        
        # Trimestres con todo en 0 y NA
        trimestres = {}
        for trim_key in ["trim1", "trim2", "trim3", "trim4"]:
            trimestres[trim_key] = {
                "poblacion_objeto": 0,
                "numerador": 0,
                "denominador": 0,
                "cobertura": 0.0,
                "semaforizacion": "NA",
                "color": "#808080"  # Gris
            }
        
        # Semestres con todo en 0 y NA
        semestres = {}
        for sem_key in ["sem1", "sem2"]:
            semestres[sem_key] = {
                "poblacion_objeto": 0,
                "numerador": 0,
                "denominador": 0,
                "cobertura": 0.0,
                "semaforizacion": "NA",
                "color": "#808080"  # Gris
            }
        
        # Anual con todo en 0 y NA
        anual = {
            "poblacion_objeto": 0,
            "numerador": 0,
            "denominador": 0,
            "cobertura": 0.0,
            "semaforizacion": "NA",
            "color": "#808080"  # Gris
        }
        
        return {
            "consulta_procedimiento": consulta_proc,
            "rango_edad": edad_aplicable,
            "cups": rpms_data.get('cups', ''),
            "frecuencia_indicada": 0,
            "poblacion_objeto": 0,
            "periodo": rpms_data.get('periodo', 'ANUAL'),
            "frecuencia_uso_ips": 0,
            "fecuencia_ajustada_anual": 0,
            "meta": 0,
            "poblacion_susceptible_anual": 0,
            "poblacion_susceptible_mensual": 0,
            "proyeccion_tiempo": 0,
            "habilitado": False,
            **{mes: mensual_data[mes] for mes in meses_nombres},
            **trimestres,
            **semestres,
            "anual": anual
        }
    
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
        if rpms_data and rpms_data.get('habilitado', True):
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
            print("      RPMS NO encontrado o deshabilitado - valores en 0")
            return {
                'cups': rpms_data.get('cups', '') if rpms_data else '',
                'periodo': rpms_data.get('periodo', '') if rpms_data else '',
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
        """
        Construye datos mensuales CON cobertura, semaforizacion y color.
        """
        mensual_data = {}
        
        for mes_num in range(1, 13):
            mes_nombre = meses_nombres[mes_num - 1]
            
            if mes_num <= mes_limite:
                # Población objeto del mes
                if -1 in poblaciones_mensuales:
                    poblacion_objeto_mes = poblacion_obj_anual
                else:
                    poblacion_objeto_mes = poblaciones_mensuales.get(mes_num, 0)
                
                # Numerador y denominador
                numerador = numeradores_mensuales.get(mes_num, 0)
                denominador = round(poblacion_susceptible_mensual, 0)
                
                # Calcular cobertura
                if denominador > 0:
                    cobertura = (numerador / denominador) * 100
                else:
                    cobertura = 0.0
                
                # Obtener semaforización
                semaf_result = self.semaforization.calculate_semaforizacion(
                    numerador, 
                    denominador, 
                    cobertura
                )
                
                mensual_data[mes_nombre] = {
                    "poblacion_objeto": poblacion_objeto_mes,
                    "numerador": numerador,
                    "denominador": denominador,
                    "cobertura": round(cobertura, 2),
                    "semaforizacion": semaf_result["estado"],
                    "color": semaf_result["color"]
                }
            else:
                # Meses fuera del límite → todo en 0 con NA
                mensual_data[mes_nombre] = {
                    "poblacion_objeto": 0,
                    "numerador": 0,
                    "denominador": 0,
                    "cobertura": 0.0,
                    "semaforizacion": "NA",
                    "color": "#808080"  # Gris
                }
        
        return mensual_data
