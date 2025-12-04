// interfaces/IAbsentUser.ts

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
    actividad_valor: string;
    columna_evaluada: string;
    mes_correspondiente: number;
}

export interface MesData {
    cantidad: number;
    inasistentes: InasistenteRecord[];
}

export interface ActivityReport {
    actividad: string;
    rango_edad: string;
    total_inasistentes: number;
    statistics: {
        total_inasistentes: number;
    };
    enero?: MesData;
    febrero?: MesData;
    marzo?: MesData;
    abril?: MesData;
    mayo?: MesData;
    junio?: MesData;
    julio?: MesData;
    agosto?: MesData;
    septiembre?: MesData;
    octubre?: MesData;
    noviembre?: MesData;
    diciembre?: MesData;
}

export interface InasistentesReportResponse {
    success: boolean;
    filename: string;
    corte_fecha: string;
    metodo: string;
    filtros_aplicados: {
        keywords: string[];
        departamento?: string;
        municipio?: string;
        ips?: string;
    };
    inasistentes_por_actividad: ActivityReport[];
    resumen_general: {
        total_actividades_evaluadas: number;
        total_inasistentes_global: number;
        actividades_con_inasistentes: number;
        actividades_sin_inasistentes: number;
    };
    engine: string;
    error?: string;
}

export interface InasistentesFilters {
    departamento?: string;
    municipio?: string;
    ips?: string;
}
