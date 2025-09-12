

export interface AgeRanges {
  years: number[];
  months: number[];
}
export interface AgeStatistics {
  total_registros: number;
  registros_con_fecha_nacimiento: number;
  registros_con_edad: number;
  rango_años: {
    min: number;
    max: number;
  };
  rango_meses: {
    min: number;
    max: number;
  };
}

export interface AgeRangesResponse {
  success: boolean;
  filename: string;
  corte_fecha: string;
  age_ranges: AgeRanges;
  statistics: AgeStatistics;
  engine: string;
  error?: string;
}