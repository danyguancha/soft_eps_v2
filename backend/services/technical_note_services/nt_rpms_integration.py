# services/technical_note_services/nt_rpms_integration.py

from typing import Optional, Dict, Any, List
from services.duckdb_service.duckdb_service import duckdb_service
from utils.text_normalizer import normalize_text  # 🔥 IMPORTAR


class NTRPMSIntegration:
    """Integración con datos consolidados de NT RPMS"""
    
    def __init__(self, data_source: str):
        """
        Inicializa integración con NT RPMS
        
        Args:
            data_source: Fuente de datos (tabla, vista o read_parquet(...))
        """
        self.data_source = data_source
        self.table_name = None
        
        print(f"✓ NT_RPMS Integration inicializando...")
        print(f"   data_source recibido: {data_source[:80]}...")
        
        # Detectar si es read_parquet() y crear vista temporal
        if 'read_parquet' in data_source.lower():
            print(f"   🔍 Detectado formato read_parquet(), creando vista temporal...")
            import hashlib
            import random
            temp_suffix = hashlib.md5(str(random.random()).encode()).hexdigest()[:8]
            self.table_name = f"nt_rpms_temp_{temp_suffix}"
            
            try:
                create_view_sql = f"CREATE OR REPLACE TEMP VIEW {self.table_name} AS SELECT * FROM {data_source}"
                duckdb_service.conn.execute(create_view_sql)
                print(f"   ✅ Vista temporal creada: {self.table_name}")
            except Exception as e:
                print(f"   ⚠️ Error creando vista temporal: {e}")
                self.table_name = data_source
        else:
            self.table_name = data_source
        
        # Verificar que la tabla tiene datos
        try:
            count_query = f"SELECT COUNT(*) FROM {self.table_name}"
            total_rows = duckdb_service.conn.execute(count_query).fetchone()[0]
            print(f"   ✅ Tabla NT_RPMS: {total_rows:,} filas")
        except Exception as e:
            print(f"   ⚠️ Error verificando NT_RPMS: {e}")
    
    def find_matching_row(
        self,
        consulta_procedimiento: str,  # 🔥 CORREGIDO
        edad_nt_rpms: str,
        departamento: str = None,
        municipio: str = None,
        nombre_ips: str = None  # 🔥 CORREGIDO
    ) -> Optional[Dict[str, Any]]:
        """
        🔥 MEJORADO: Busca fila que coincida con consulta/procedimiento y edad con normalización de texto
        
        Args:
            consulta_procedimiento: Nombre de consulta/procedimiento
            edad_nt_rpms: Edad en formato NT_RPMS (ej: "12, 14 y 16 Años")
            departamento: Filtro por departamento (opcional)
            municipio: Filtro por municipio (opcional)
            nombre_ips: Filtro por nombre de IPS (opcional)
        
        Returns:
            Dict con datos de NT_RPMS o None si no se encuentra
        """
        try:
            print(f"\n      🔍 BUSCANDO EN NT_RPMS (OPTIMIZADO):")
            print(f"         Consulta: '{consulta_procedimiento[:60]}...'")
            print(f"         Edad NT_RPMS: '{edad_nt_rpms}'")
            print(f"         Filtros geo: Depto={departamento}, Muni={municipio}, IPS={nombre_ips}")
            
            # 🔥 NORMALIZAR todos los textos para comparación
            consulta_norm = normalize_text(consulta_procedimiento)
            edad_norm = normalize_text(edad_nt_rpms)
            
            # 🔥 NORMALIZAR filtros geográficos
            depto_norm = normalize_text(departamento) if departamento else None
            muni_norm = normalize_text(municipio) if municipio else None
            ips_norm = normalize_text(nombre_ips) if nombre_ips else None
            
            print(f"      📝 Textos normalizados:")
            print(f"         Consulta: '{consulta_norm[:60]}...'")
            print(f"         Edad: '{edad_norm}'")
            if depto_norm:
                print(f"         Depto: '{depto_norm}'")
            if muni_norm:
                print(f"         Muni: '{muni_norm}'")
            if ips_norm:
                print(f"         IPS: '{ips_norm}'")
            
            # Construir query SQL con normalización
            conditions = []
            
            # Filtro de consulta (normalizado)
            conditions.append(f"""
                UPPER(
                    TRIM(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        regexp_replace(
                                            regexp_replace(
                                                CAST(consultas_procedimientos AS VARCHAR),
                                                '[áàäâ]', 'A', 'g'
                                            ),
                                            '[éèëê]', 'E', 'g'
                                        ),
                                        '[íìïî]', 'I', 'g'
                                    ),
                                    '[óòöô]', 'O', 'g'
                                ),
                                '[úùüû]', 'U', 'g'
                            ),
                            '[ñÑ]', 'N', 'g'
                        )
                    )
                ) = '{consulta_norm}'
            """)
            
            # Filtro de edad (normalizado)
            conditions.append(f"""
                UPPER(
                    TRIM(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        regexp_replace(
                                            regexp_replace(
                                                CAST(frecuencia_edad AS VARCHAR),
                                                '[áàäâ]', 'A', 'g'
                                            ),
                                            '[éèëê]', 'E', 'g'
                                        ),
                                        '[íìïî]', 'I', 'g'
                                    ),
                                    '[óòöô]', 'O', 'g'
                                ),
                                '[úùüû]', 'U', 'g'
                            ),
                            '[ñÑ]', 'N', 'g'
                        )
                    )
                ) = '{edad_norm}'
            """)
            
            # 🔥 Filtros geográficos con normalización
            if depto_norm:
                conditions.append(f"""
                    UPPER(
                        TRIM(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        regexp_replace(
                                            regexp_replace(
                                                regexp_replace(
                                                    CAST(departamento AS VARCHAR),
                                                    '[áàäâ]', 'A', 'g'
                                                ),
                                                '[éèëê]', 'E', 'g'
                                            ),
                                            '[íìïî]', 'I', 'g'
                                        ),
                                        '[óòöô]', 'O', 'g'
                                    ),
                                    '[úùüû]', 'U', 'g'
                                ),
                                '[ñÑ]', 'N', 'g'
                            )
                        )
                    ) = '{depto_norm}'
                """)
            
            if muni_norm:
                conditions.append(f"""
                    UPPER(
                        TRIM(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        regexp_replace(
                                            regexp_replace(
                                                regexp_replace(
                                                    CAST(municipio AS VARCHAR),
                                                    '[áàäâ]', 'A', 'g'
                                                ),
                                                '[éèëê]', 'E', 'g'
                                            ),
                                            '[íìïî]', 'I', 'g'
                                        ),
                                        '[óòöô]', 'O', 'g'
                                    ),
                                    '[úùüû]', 'U', 'g'
                                ),
                                '[ñÑ]', 'N', 'g'
                            )
                        )
                    ) = '{muni_norm}'
                """)
            
            if ips_norm:
                conditions.append(f"""
                    UPPER(
                        TRIM(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        regexp_replace(
                                            regexp_replace(
                                                regexp_replace(
                                                    CAST(nombre_ips AS VARCHAR),
                                                    '[áàäâ]', 'A', 'g'
                                                ),
                                                '[éèëê]', 'E', 'g'
                                            ),
                                            '[íìïî]', 'I', 'g'
                                        ),
                                        '[óòöô]', 'O', 'g'
                                    ),
                                    '[úùüû]', 'U', 'g'
                                ),
                                '[ñÑ]', 'N', 'g'
                            )
                        )
                    ) = '{ips_norm}'
                """)
            
            # Construir query completa
            query = f"""
            SELECT 
                consultas_procedimientos,
                frecuencia_edad,
                meta,
                frecuencia_indicada,
                frecuencia_uso,
                proyeccion_tiempo,
                nombre_ips,
                departamento,
                municipio
            FROM {self.table_name}
            WHERE {" AND ".join(conditions)}
            LIMIT 1
            """
            
            # Ejecutar query
            result = duckdb_service.conn.execute(query).fetchone()
            
            if result:
                print(f"      ✅ Match encontrado CON filtros geográficos")
                return {
                    'consultas_procedimientos': result[0],
                    'frecuencia_edad': result[1],
                    'meta': float(result[2]) if result[2] else 0.0,
                    'frecuencia_indicada': float(result[3]) if result[3] else 0.0,
                    'frecuencia_uso': float(result[4]) if result[4] else 0.0,
                    'proyeccion_tiempo': int(result[5]) if result[5] else 12,
                    'nombre_ips': result[6],
                    'departamento': result[7],
                    'municipio': result[8]
                }
            
            # 🔥 Si no se encuentra con filtros geográficos, intentar sin ellos
            print(f"      🔄 Reintentando SIN filtros geográficos...")
            
            query_sin_geo = f"""
            SELECT 
                consultas_procedimientos,
                frecuencia_edad,
                meta,
                frecuencia_indicada,
                frecuencia_uso,
                proyeccion_tiempo,
                nombre_ips,
                departamento,
                municipio
            FROM {self.table_name}
            WHERE 
                UPPER(
                    TRIM(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        regexp_replace(
                                            regexp_replace(
                                                CAST(consultas_procedimientos AS VARCHAR),
                                                '[áàäâ]', 'A', 'g'
                                            ),
                                            '[éèëê]', 'E', 'g'
                                        ),
                                        '[íìïî]', 'I', 'g'
                                    ),
                                    '[óòöô]', 'O', 'g'
                                ),
                                '[úùüû]', 'U', 'g'
                            ),
                            '[ñÑ]', 'N', 'g'
                        )
                    )
                ) = '{consulta_norm}'
                AND UPPER(
                    TRIM(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        regexp_replace(
                                            regexp_replace(
                                                CAST(frecuencia_edad AS VARCHAR),
                                                '[áàäâ]', 'A', 'g'
                                            ),
                                            '[éèëê]', 'E', 'g'
                                        ),
                                        '[íìïî]', 'I', 'g'
                                    ),
                                    '[óòöô]', 'O', 'g'
                                ),
                                '[úùüû]', 'U', 'g'
                            ),
                            '[ñÑ]', 'N', 'g'
                        )
                    )
                ) = '{edad_norm}'
            LIMIT 1
            """
            
            result_sin_geo = duckdb_service.conn.execute(query_sin_geo).fetchone()
            
            if result_sin_geo:
                print(f"      ✅ Match encontrado SIN filtros geográficos")
                return {
                    'consultas_procedimientos': result_sin_geo[0],
                    'frecuencia_edad': result_sin_geo[1],
                    'meta': float(result_sin_geo[2]) if result_sin_geo[2] else 0.0,
                    'frecuencia_indicada': float(result_sin_geo[3]) if result_sin_geo[3] else 0.0,
                    'frecuencia_uso': float(result_sin_geo[4]) if result_sin_geo[4] else 0.0,
                    'proyeccion_tiempo': int(result_sin_geo[5]) if result_sin_geo[5] else 12,
                    'nombre_ips': result_sin_geo[6],
                    'departamento': result_sin_geo[7],
                    'municipio': result_sin_geo[8]
                }
            
            print(f"      ❌ No se encontró ningún match")
            return None
            
        except Exception as e:
            print(f"      ⚠️ Error buscando en NT_RPMS: {e}")
            import traceback
            traceback.print_exc()
            return None

    
    def calculate_extended_metrics(
        self,
        poblacion_objeto: int,
        nt_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Calcula métricas extendidas basadas en datos de NT_RPMS
        
        Args:
            poblacion_objeto: Población objetivo (conteo de personas)
            nt_data: Datos de NT_RPMS (meta, frecuencia, proyección)
        
        Returns:
            Dict con métricas calculadas
        """
        try:
            meta = nt_data.get('meta', 0.0)
            frecuencia = nt_data.get('frecuencia_indicada', 1.0)
            proyeccion = nt_data.get('proyeccion_tiempo', 12)
            
            # Población susceptible = población * meta
            poblacion_susceptible = poblacion_objeto * meta
            
            # Denominador mensual = (población susceptible * frecuencia) / proyección
            if proyeccion > 0:
                denominador_mensual = (poblacion_susceptible * frecuencia) / proyeccion
            else:
                denominador_mensual = 0
            
            return {
                'poblacion_susceptible': round(poblacion_susceptible, 2),
                'valor_mensual': round(denominador_mensual, 0),
                'meta': meta,
                'frecuencia_uso': nt_data.get('frecuencia_uso', 0.0),
                'proyeccion_tiempo': proyeccion
            }
            
        except Exception as e:
            print(f"Error calculando métricas: {e}")
            return {
                'poblacion_susceptible': 0,
                'valor_mensual': 0,
                'meta': 0.0,
                'frecuencia_uso': 0.0,
                'proyeccion_tiempo': 12
            }
