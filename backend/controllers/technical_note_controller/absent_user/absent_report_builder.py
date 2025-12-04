from typing import Any, Dict, List


class AbsentReportBuilder:
    """
    Constructor de reportes de inasistentes.
    Procesa columnas y construye estructura de reporte.
    """
    
    def __init__(self, absent_calculator, column_matcher_class):
        self.absent_calculator = absent_calculator
        self.column_matcher_class = column_matcher_class
        print("✓ AbsentReportBuilder inicializado")
    
    def process_all_columns(
        self,
        matching_columns: List[str],
        column_mappings: Dict[str, Any],
        data_source: str,
        where_clause: str,
        anio_corte: int,
        mes_limite: int
    ) -> List[Dict[str, Any]]:
        """Procesa todas las columnas que coinciden y genera items de reporte."""
        report_items = []
        
        for col in matching_columns:
            print(f"\nProcesando: {col}")
            
            item = self._process_single_column(
                column_name=col,
                column_mappings=column_mappings,
                data_source=data_source,
                where_clause=where_clause,
                anio_corte=anio_corte,
                mes_limite=mes_limite
            )
            
            if item:
                report_items.append(item)
                print(f"   ✓ Item generado: {item['total_inasistentes']} inasistentes")
            else:
                print(f"   ⚠️  No se pudo generar item para: {col}")
        
        return report_items
    
    def _process_single_column(
        self,
        column_name: str,
        column_mappings: Dict[str, Any],
        data_source: str,
        where_clause: str,
        anio_corte: int,
        mes_limite: int
    ) -> Dict[str, Any]:
        """Procesa una columna individual y genera item de reporte."""
        try:
            # Buscar mappings para esta columna
            mappings_list = self._find_mappings_for_column(
                column_name, column_mappings
            )
            
            if not mappings_list:
                print(f"      ⚠️  No se encontraron mappings para: {column_name}")
                return None
            
            # Tomar el primer mapping
            mapping = mappings_list[0]
            
            # Extraer información según la estructura real
            edad_label = mapping.get('edad', '')
            consulta = mapping.get('consulta', '')
            
            if not edad_label:
                print(f"      ⚠️  Mapping sin edad para: {column_name}")
                return None
            
            print(f"   Edad aplicable: {edad_label}")
            print(f"   Consulta: {consulta}")
            
            # Calcular inasistentes
            inasistentes_mensuales = self.absent_calculator.calculate_absent_by_month(
                data_source=data_source,
                column_name=column_name,
                where_clause=where_clause,
                edad_label=edad_label,
                anio_corte=anio_corte,
                mes_limite=mes_limite
            )
            
            # Construir item de reporte
            return self._build_report_item(
                column_name=column_name,
                edad_label=edad_label,
                consulta=consulta,
                inasistentes_mensuales=inasistentes_mensuales
            )
            
        except Exception as e:
            print(f"      ❌ Error procesando {column_name}: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _find_mappings_for_column(
        self,
        column_name: str,
        column_mappings: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Encuentra mappings que corresponden a una columna.
        Estructura real: {'columna_usuario': ..., 'consolidado': {dict_directo}}
        """
        print(f"      Buscando mappings para columna: '{column_name}'")
        
        from utils.text_normalizer import normalize_text
        col_normalized = normalize_text(column_name)
        print(f"         Normalizada: '{col_normalized}'")
        
        mappings_list = []
        
        # Obtener lista de mappings
        all_mappings = column_mappings.get('mappings', [])
        
        if not all_mappings:
            print(f"      ⚠️  No hay mappings en column_mappings")
            return []
        
        print(f"      📋 Analizando {len(all_mappings)} mappings...")
        
        # Buscar coincidencias
        for idx, mapping in enumerate(all_mappings):
            columna_usuario = mapping.get('columna_usuario', '')
            
            if not columna_usuario:
                continue
            
            columna_usuario_normalized = normalize_text(columna_usuario)
            
            # Comparar normalizado
            if col_normalized == columna_usuario_normalized:
                # CAMBIO: consolidado es un DICT directo, no una lista
                consolidado = mapping.get('consolidado', {})
                
                # Verificar que consolidado sea un diccionario
                if not isinstance(consolidado, dict):
                    print(f"         [✗] Match #{idx} pero consolidado no es dict")
                    continue
                
                # Extraer campos directamente del dict
                # Opciones posibles de nombres de campos
                consulta = (
                    consolidado.get('consulta_procedimiento') or 
                    consolidado.get('consulta') or 
                    consolidado.get('actividad') or
                    ''
                )
                
                edad = (
                    consolidado.get('edad_aplicable') or
                    consolidado.get('edad') or
                    consolidado.get('edad_NT_RPMS') or
                    ''
                )
                
                if not edad or not consulta:
                    print(f"         [✗] Match #{idx} pero sin edad ({edad}) o consulta ({consulta})")
                    continue
                
                # Crear mapping simplificado
                mapping_simple = {
                    'columna_usuario': columna_usuario,
                    'edad': edad,  # Este es el que se usa después
                    'consulta': consulta,
                    'keyword': mapping.get('keyword', ''),
                    'edad_NT_RPMS': consolidado.get('edad_NT_RPMS', edad),
                    'consolidado_completo': consolidado
                }
                
                print(f"         [✓] Match #{idx}: Edad '{edad}', Consulta '{consulta[:50]}...'")
                mappings_list.append(mapping_simple)
        
        print(f"      Total mappings encontrados: {len(mappings_list)}")
        
        # Deduplicar por edad
        if mappings_list:
            unique_mappings = {}
            for m in mappings_list:
                edad = m.get('edad', '')
                if edad and edad not in unique_mappings:
                    unique_mappings[edad] = m
            
            mappings_list = list(unique_mappings.values())
            print(f"      Mappings únicos después de deduplicar: {len(mappings_list)}")
        
        return mappings_list


    
    def _build_report_item(
        self,
        column_name: str,
        edad_label: str,
        consulta: str,
        inasistentes_mensuales: Dict[int, List[Dict]]
    ) -> Dict[str, Any]:
        """Construye un item de reporte con estructura completa."""
        meses_nombres = [
            'enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio',
            'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'
        ]
        
        item = {
            'actividad': consulta,
            'columna_evaluada': column_name,
            'rango_edad': edad_label,
            'total_inasistentes': 0
        }
        
        # Agregar datos de cada mes
        for mes_num in range(1, 13):
            mes_nombre = meses_nombres[mes_num - 1]
            inasistentes_list = inasistentes_mensuales.get(mes_num, [])
            
            item[mes_nombre] = {
                'cantidad': len(inasistentes_list),
                'inasistentes': inasistentes_list
            }
            
            item['total_inasistentes'] += len(inasistentes_list)
        
        return item
