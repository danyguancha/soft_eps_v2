// interfaces/IAbsentUser.tsx - Nuevas interfaces para sistema dinámico

export interface InasistenteRecord {
  departamento: string;
  municipio: string;
  nombre_ips: string;
  nro_identificacion: string;
  primer_apellido: string;
  segundo_apellido: string;
  primer_nombre: string;
  segundo_nombre: string;
  fecha_nacimiento: string;
  edad_anos: number | null;
  edad_meses: number | null;
  actividad_valor: string;           // NUEVO: Valor de la actividad específica
  columna_evaluada: string;          // NUEVO: Nombre de la columna evaluada
}

export interface InasistentesStatistics {
  total_inasistentes: number;
  departamentos_afectados: number;
  municipios_afectados: number;
  ips_afectadas: number;
}

// NUEVA INTERFAZ: Reporte por actividad individual
export interface ActivityReport {
  actividad: string;
  inasistentes: InasistenteRecord[];
  statistics: InasistentesStatistics;
}

// NUEVA INTERFAZ: Resumen general de todas las actividades
export interface ResumenGeneral {
  total_actividades_evaluadas: number;
  total_inasistentes_global: number;
  departamentos_afectados: number;
  municipios_afectados: number;
  ips_afectadas: number;
  actividades_con_inasistentes: number;
  actividades_sin_inasistentes: number;
}

// INTERFAZ PRINCIPAL ACTUALIZADA
export interface InasistentesReportResponse {
  success: boolean;
  filename: string;
  corte_fecha: string;
  metodo: string;                    // NUEVO: "DESCUBRIMIENTO_DINAMICO"
  filtros_aplicados: {
    selected_months: number[];
    selected_years: number[];
    selected_keywords: string[];     // ACTUALIZADO: Ahora requerido
    departamento?: string;
    municipio?: string;
    ips?: string;
  };
  columnas_descubiertas: Record<string, string[]>;  // NUEVO: Columnas encontradas por palabra clave
  inasistentes_por_actividad: ActivityReport[];     // NUEVO: Array de reportes por actividad
  resumen_general: ResumenGeneral;                  // NUEVO: Resumen general de todas las actividades
  engine: string;
  error?: string;
}
