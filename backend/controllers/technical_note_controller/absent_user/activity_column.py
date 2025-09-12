

import re


class ActivityColumn:
    def is_activity_column(self, column_name: str) -> bool:
        """Determina si una columna representa una actividad/consulta - VERSIÓN COMPLETA"""
        col_lower = column_name.lower()
        
        # Patrones que indican columnas de actividades
        activity_patterns = [
            # PATRONES EXISTENTES
            r'consulta.*\d+.*mes',           # "Consulta por X 1 mes"
            r'consulta.*\d+.*año',           # "Consulta por X 2 años"
            r'fecha.*consulta',              # "Fecha más reciente de Consulta"
            r'control.*\d+',                 # "Control de X 12 meses"
            r'valoracion.*\d+',              # "Valoración X 6 meses"
            r'seguimiento.*\d+',             # "Seguimiento X 18 meses"
            r'tamizaje.*\d+',                # "Tamizaje X 24 meses"
            
            # PATRONES PARA VACUNACIÓN
            r'esquema.*vacun',               # "Esquema de vacunación"
            r'vacunacion.*regular',          # "Vacunación Regular"
            r'vacunacion.*covid',            # "Vacunación COVID"
            r'vacunacion.*completo',         # "Vacunación Completo"
            r'fecha.*vacun',                 # "Fecha más reciente de Vacunación"
            r'dosis.*vacun',                 # "Dosis de Vacunación"
            r'aplicacion.*vacuna',           # "Aplicación de vacuna"
            r'esquema.*inmun',               # "Esquema de inmunización"
            
            # NUEVOS PATRONES PARA PLACA DENTAL Y ODONTOLOGÍA
            r'remocion.*placa',              # "Remoción de placa"
            r'placa.*bacteriana',            # "Placa bacteriana"
            r'placa.*dental',                # "Placa dental"
            r'placa.*detectada',             # "Placa detectada"
            r'fecha.*remocion.*placa',       # "Fecha de remoción de placa"
            r'limpieza.*dental',             # "Limpieza dental"
            r'higiene.*oral',                # "Higiene oral"
            r'profilaxis.*dental',           # "Profilaxis dental"
            r'detartraje',                   # "Detartraje"
            r'placa(?!.*resultado)',         # "Placa" (pero no resultados de placa)
            
            # PATRONES GENÉRICOS MEJORADOS
            r'consulta(?!.*estado)',        # Cualquier consulta que no sea estado
            r'fecha.*reciente.*consulta',    # Fechas de consulta reciente
            r'atencion.*\d+',                # "Atención X meses"
            r'seguimiento(?!.*resultado)',   # Seguimientos que no sean resultados
            r'fecha.*reciente(?!.*resultado)', # Fechas recientes que no sean resultados
        ]
        
        # Excluir patrones que no son actividades
        exclude_patterns = [
            r'estado(?!.*esquema)',          # Estados (excepto "Estado de esquema")
            r'diagnostico(?!\s+\w+\s+\d)',  # Diagnósticos (excepto con números)
            r'resultado(?!.*consulta)',      # Resultados (excepto de consulta)
            r'clasificacion(?!.*actividad)', # Clasificaciones
            r'tipo(?!.*consulta)',           # Tipos (excepto tipo de consulta)
            r'codigo(?!.*actividad)',        # Códigos
            r'observacion(?!.*actividad)',   # Observaciones generales
            r'observaciones(?!.*consulta)',  # Observaciones que no sean de consulta
        ]
        
        # Verificar patrones de actividades
        for pattern in activity_patterns:
            if re.search(pattern, col_lower):
                # Verificar que no sea un patrón excluido
                is_excluded = any(re.search(exclude_pattern, col_lower) for exclude_pattern in exclude_patterns)
                if not is_excluded:
                    return True
        
        return False