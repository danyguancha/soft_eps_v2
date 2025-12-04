from typing import Dict, List, Any, Optional
from services.duckdb_service.duckdb_service import duckdb_service
from utils.text_normalizer import normalize_text
from utils.age_ranges import get_edad_manager
import json
from pathlib import Path


class AbsentCalculator:
    """
    Calculador de inasistentes con modo híbrido:
    - MESES: Usa sistema ORIGINAL con birth_date_ranges_den.json (fechas de nacimiento)
    - AÑOS: Usa sistema NUEVO con columna 'Edad'
    """
    
    def __init__(self):
        """Inicializa el calculador en modo híbrido."""
        # Para AÑOS: gestor de rangos de edad
        self.edad_manager = get_edad_manager()
        
        # Para MESES: sistema original con fechas de nacimiento
        self.birth_date_ranges = self._load_birth_date_ranges_original()
        
        print(f"✓ AbsentCalculator inicializado (modo híbrido)")
        print(f"   Sistema AÑOS (nuevo): {len(self.edad_manager.get_all_labels())} rangos")
        print(f"   Sistema MESES (original): {len(self.birth_date_ranges)} rangos")
    
    def _load_birth_date_ranges_original(self) -> Dict[str, Dict[int, tuple]]:
        """
        Carga el archivo ORIGINAL birth_date_ranges_den.json para MESES.
        Este es el sistema que ya funcionaba antes.
        """
        try:
            config_dir = Path(__file__).parent.parent.parent.parent / 'config'
            json_path = config_dir / 'birth_date_ranges_den.json'
            
            # DEBUG: Mostrar ruta que se está buscando
            print(f"   📂 Buscando archivo en: {json_path}")
            print(f"   📂 Existe: {json_path.exists()}")
            
            if not json_path.exists():
                print(f"   ⚠️  birth_date_ranges_den.json no encontrado en: {json_path}")
                return {}
            
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Convertir a formato interno
            ranges = {}
            for edad_key, meses_dict in data.items():
                ranges[edad_key] = {
                    int(mes): tuple(fechas)
                    for mes, fechas in meses_dict.items()
                }
            
            print(f"   ✓ Rangos de fechas (meses) cargados: {len(ranges)} grupos")
            return ranges
            
        except Exception as e:
            print(f"   ⚠️  Error cargando birth_date_ranges_den.json: {e}")
            import traceback
            traceback.print_exc()
            return {}

    
    @property
    def conn(self):
        """Obtiene conexión DuckDB dinámicamente"""
        return duckdb_service.conn
    
    def calculate_absent_by_month(
        self,
        data_source: str,
        column_name: str,
        where_clause: str,
        edad_label: str,
        anio_corte: int,
        mes_limite: int,
        edad_column: str = "Edad"
    ) -> Dict[int, List[Dict[str, Any]]]:
        """
        Calcula inasistentes mes a mes.
        - Para MESES: usa sistema ORIGINAL (fechas de nacimiento)
        - Para AÑOS: usa sistema NUEVO (columna Edad)
        """
        print(f"\n      📊 Calculando inasistentes para: {column_name}")
        print(f"         Rango edad: '{edad_label}'")
        
        # Determinar si es rango de meses o años
        es_rango_meses = self._es_rango_meses(edad_label)
        
        if es_rango_meses:
            print(f"         ✓ Detectado: RANGO EN MESES - Usando sistema ORIGINAL (fechas)")
            return self._calculate_absent_meses_original(
                data_source=data_source,
                column_name=column_name,
                where_clause=where_clause,
                edad_label=edad_label,
                anio_corte=anio_corte,
                mes_limite=mes_limite
            )
        else:
            print(f"         ✓ Detectado: RANGO EN AÑOS - Usando sistema NUEVO (columna Edad)")
            return self._calculate_absent_anios_nuevo(
                data_source=data_source,
                column_name=column_name,
                where_clause=where_clause,
                edad_label=edad_label,
                anio_corte=anio_corte,
                mes_limite=mes_limite,
                edad_column=edad_column
            )
    
    def _es_rango_meses(self, edad_label: str) -> bool:
        """Determina si un rango es de meses o años."""
        label_lower = edad_label.lower()
        return 'mes' in label_lower or 'meses' in label_lower
    
    def _calculate_absent_meses_original(
        self,
        data_source: str,
        column_name: str,
        where_clause: str,
        edad_label: str,
        anio_corte: int,
        mes_limite: int
    ) -> Dict[int, List[Dict[str, Any]]]:
        """
        SISTEMA ORIGINAL para MESES: usa fechas de nacimiento.
        Este es el código que YA FUNCIONABA antes.
        """
        # Normalizar label para buscar en el diccionario
        # Intentar múltiples normalizaciones
        edad_key_found = None
        
        for possible_key in [
            edad_label,
            edad_label.lower(),
            normalize_text(edad_label),
            edad_label.replace("Meses", "meses"),
            edad_label.replace("Mes", "mes"),
        ]:
            if possible_key in self.birth_date_ranges:
                edad_key_found = possible_key
                break
        
        if not edad_key_found:
            print(f"         ✗ No se encontró rango de fechas para: '{edad_label}'")
            print(f"         Keys disponibles: {list(self.birth_date_ranges.keys())}")
            return self._create_empty_result()
        
        date_ranges = self.birth_date_ranges[edad_key_found]
        print(f"         ✓ Usando rango: '{edad_key_found}'")
        
        col_escaped = f'"{column_name}"'
        inasistentes_mensuales = {}
        
        for mes_num in range(1, 13):
            if mes_num > mes_limite:
                inasistentes_mensuales[mes_num] = []
                continue
            
            if mes_num not in date_ranges:
                inasistentes_mensuales[mes_num] = []
                continue
            
            fecha_inicio, fecha_fin = date_ranges[mes_num]
            
            # Query ORIGINAL que ya funcionaba
            query = f"""
            SELECT 
                "Departamento",
                "Municipio",
                "Nombre IPS",
                "Nro Identificación",
                "Primer Apellido",
                "Segundo Apellido",
                "Primer Nombre",
                "Segundo Nombre",
                "Fecha Nacimiento",
                CAST("Edad" AS INTEGER) as edad_anos,
                {col_escaped}
            FROM {data_source}
            WHERE {where_clause}
              AND "Fecha Nacimiento" IS NOT NULL
              AND strptime("Fecha Nacimiento", '%d/%m/%Y') >= strptime('{fecha_inicio}', '%d/%m/%Y')
              AND strptime("Fecha Nacimiento", '%d/%m/%Y') <= strptime('{fecha_fin}', '%d/%m/%Y')
              AND ({col_escaped} IS NULL 
                   OR TRIM(CAST({col_escaped} AS VARCHAR)) = ''
                   OR UPPER(TRIM(CAST({col_escaped} AS VARCHAR))) = 'NO')
            ORDER BY "Primer Apellido", "Primer Nombre"
            LIMIT 10000
            """
            
            try:
                result = self.conn.execute(query).fetchall()
                
                inasistentes = [
                    self._process_absent_row(row, column_name, mes_num)
                    for row in result
                ]
                
                inasistentes_mensuales[mes_num] = inasistentes
                
                if inasistentes:
                    print(f"         Mes {mes_num}: {len(inasistentes)} inasistentes")
            
            except Exception as e:
                print(f"         ✗ Error mes {mes_num}: {str(e)[:100]}")
                inasistentes_mensuales[mes_num] = []
        
        total = sum(len(v) for v in inasistentes_mensuales.values())
        print(f"         ✅ Total inasistentes (meses): {total}")
        
        return inasistentes_mensuales
    
    def _calculate_absent_anios_nuevo(
        self,
        data_source: str,
        column_name: str,
        where_clause: str,
        edad_label: str,
        anio_corte: int,
        mes_limite: int,
        edad_column: str = "Edad"
    ) -> Dict[int, List[Dict[str, Any]]]:
        """
        SISTEMA NUEVO para AÑOS: usa columna 'Edad'.
        Esta es la funcionalidad que acabas de agregar.
        """
        # Obtener información del rango de edad
        filter_info = self.edad_manager.get_edad_filter_for_range(edad_label)
        
        if not filter_info:
            print(f"         ✗ No se encontró rango de edad para: '{edad_label}'")
            return self._create_empty_result()
        
        tipo_filtro, valores = filter_info
        
        # Construir filtro de edad según el tipo
        edad_filter = self._build_edad_filter(tipo_filtro, valores, edad_column)
        
        if not edad_filter:
            print(f"         ✗ No se pudo construir filtro de edad")
            return self._create_empty_result()
        
        print(f"         ✓ Filtro edad: {edad_filter}")
        
        col_escaped = f'"{column_name}"'
        
        # Query para años usando columna Edad
        query = f"""
        SELECT 
            "Departamento",
            "Municipio",
            "Nombre IPS",
            "Nro Identificación",
            "Primer Apellido",
            "Segundo Apellido",
            "Primer Nombre",
            "Segundo Nombre",
            "Fecha Nacimiento",
            CAST("{edad_column}" AS INTEGER) as edad_anos,
            {col_escaped}
        FROM {data_source}
        WHERE {where_clause}
          AND {edad_filter}
          AND ({col_escaped} IS NULL 
               OR TRIM(CAST({col_escaped} AS VARCHAR)) = ''
               OR UPPER(TRIM(CAST({col_escaped} AS VARCHAR))) = 'NO')
        ORDER BY "Primer Apellido", "Primer Nombre"
        LIMIT 10000
        """
        
        try:
            result = self.conn.execute(query).fetchall()
            
            inasistentes = [
                self._process_absent_row(row, column_name, 1)
                for row in result
            ]
            
            # Para años, poner todos en mes 1
            inasistentes_mensuales = {}
            for mes_num in range(1, 13):
                if mes_num == 1 and mes_num <= mes_limite:
                    inasistentes_mensuales[mes_num] = inasistentes
                else:
                    inasistentes_mensuales[mes_num] = []
            
            if inasistentes:
                print(f"         ✓ Total inasistentes (años): {len(inasistentes)}")
        
        except Exception as e:
            print(f"         ✗ Error: {str(e)[:100]}")
            import traceback
            traceback.print_exc()
            inasistentes_mensuales = self._create_empty_result()
        
        return inasistentes_mensuales
    
    def _build_edad_filter(
        self,
        tipo_filtro: str,
        valores: Any,
        edad_column: str
    ) -> Optional[str]:
        """Construye filtro SQL para la columna Edad según el tipo de rango."""
        col_escaped = f'"{edad_column}"'
        
        try:
            if tipo_filtro == 'anios_exactos':
                edad = valores[0]
                return f"{col_escaped} = {edad}"
            
            elif tipo_filtro == 'anios_lista':
                if not valores:
                    return None
                edades_str = ', '.join(str(v) for v in valores)
                return f"{col_escaped} IN ({edades_str})"
            
            elif tipo_filtro == 'meses_rango':
                return f"{col_escaped} = 0"
            
            elif tipo_filtro == 'meses_y_anios':
                meses = valores.get('meses', [])
                anios = valores.get('anios', [])
                
                conditions = []
                
                if meses:
                    conditions.append(f"{col_escaped} = 0")
                
                if anios:
                    edades_str = ', '.join(str(v) for v in anios)
                    conditions.append(f"{col_escaped} IN ({edades_str})")
                
                if conditions:
                    return f"({' OR '.join(conditions)})"
                return None
            
            else:
                print(f"         ⚠️  Tipo de filtro no soportado: {tipo_filtro}")
                return None
                
        except Exception as e:
            print(f"         ✗ Error construyendo filtro: {e}")
            return None
    
    def _process_absent_row(
        self,
        row: tuple,
        column_name: str,
        mes_num: int
    ) -> Dict[str, Any]:
        """Procesa una fila de resultado y la convierte en diccionario de inasistente."""
        return {
            "departamento": str(row[0]).strip() if row[0] else "",
            "municipio": str(row[1]).strip() if row[1] else "",
            "nombre_ips": str(row[2]).strip() if row[2] else "",
            "nro_identificacion": str(row[3]).strip() if row[3] else "",
            "primer_apellido": str(row[4]).strip() if row[4] else "",
            "segundo_apellido": str(row[5]).strip() if row[5] else "",
            "primer_nombre": str(row[6]).strip() if row[6] else "",
            "segundo_nombre": str(row[7]).strip() if row[7] else "",
            "fecha_nacimiento": str(row[8]).strip() if row[8] else "",
            "edad_anos": int(row[9]) if row[9] is not None else 0,
            "actividad_valor": str(row[10]).strip() if row[10] else "VACÍO",
            "columna_evaluada": column_name,
            "mes_correspondiente": mes_num
        }
    
    def _create_empty_result(self) -> Dict[int, List]:
        """Crea resultado vacío para todos los meses."""
        return {mes: [] for mes in range(1, 13)}
    
    def validate_edad_column(self, data_source: str, edad_column: str = "Edad") -> bool:
        """Valida que la columna de edad existe y tiene datos válidos."""
        query = f"""
        SELECT 
            COUNT(*) as total,
            COUNT("{edad_column}") as con_edad,
            COUNT(CASE WHEN CAST("{edad_column}" AS INTEGER) >= 0 
                       AND CAST("{edad_column}" AS INTEGER) <= 120 
                       THEN 1 END) as edades_validas
        FROM {data_source}
        LIMIT 1
        """
        
        try:
            result = self.conn.execute(query).fetchone()
            total, con_edad, validas = result
            
            if total == 0:
                return False
            
            porcentaje_validas = (validas / total) * 100
            return porcentaje_validas > 80
            
        except Exception as e:
            print(f"✗ Error validando columna edad: {e}")
            return False
