import type { FilterCondition } from "../types/api.types";

export interface TechnicalFileInfo {
  filename: string;
  display_name: string;
  description: string;
  extension: string;
  file_size: number;
  file_path: string;
  columns: string[];
  total_rows: number;
}


export interface TechnicalFileData {
  success: boolean;
  filename: string;
  display_name: string;
  description: string;
  data: Record<string, any>[];
  columns: string[];
  pagination: {
    current_page: number;
    page_size: number;
    total_rows: number;
    total_pages: number;
    rows_in_page: number;
    start_row: number;
    end_row: number;
    has_next: boolean;
    has_prev: boolean;
    showing: string;
    original_total?: number;
    filtered?: boolean;
    is_last_page?: boolean;
    is_first_page?: boolean;
  };
  file_info: {
    extension: string;
    file_size: number;
    sheet_name: string;
    encoding_used: string;
    total_columns: number;
    processing_method: string;
  };
  filters_applied?: FilterCondition[];
  search_applied?: string;
  sort_applied?: {
    column?: string;
    order?: string;
  };
}


export interface TechnicalFileMetadata {
  filename: string;
  display_name: string;
  description: string;
  total_rows: number;
  total_columns: number;
  columns: string[];
  file_size: number;
  extension: string;
  encoding: string;
  separator?: string;
  sheets?: string[];
  recommended_page_size: number;
}


export interface ColumnUniqueValues {
  filename: string;
  column_name: string;
  unique_values: string[];
  total_unique: number;
  limited: boolean;
  limit_applied: number;
}


export interface KeywordAgeReportItem {
  color: any;
  descripcion: string;
  column: string;
  keyword: string;
  age_range: string;
  count: number;
  numerador?: number;
  denominador?: number;
  cobertura_porcentaje?: number;
  sin_datos?: number;
  metodo?: string;
  age_range_extracted?: {
    min_age: number;
    max_age: number;
    unit: string;
    sql_filter: string;
  };
  corte_fecha?: string;
  semaforizacion: string;
}


export interface TemporalMonth {
  month: number;
  month_name: string;
  count: number;
  numerador?: number;
  denominador?: number;
  pct?: number;
  cobertura_porcentaje?: number;
  semaforizacion?: string;
  color?: string;
  color_name?: string;
  descripcion?: string;
}


export interface TemporalYear {
  year: number;
  total: number;
  total_num?: number;
  total_den?: number;
  pct?: number;
  semaforizacion?: string;
  color?: string;
  color_name?: string;
  descripcion?: string;
  months: Record<string, TemporalMonth>;
}


export interface TemporalColumnData {
  column: string;
  keyword: string;
  age_range: string;
  years: Record<string, TemporalYear>;
}


export interface GeographicFilters {
  departamento?: string | null;
  municipio?: string | null;
  ips?: string | null;
}


export interface GeographicValuesResponse {
  success: boolean;
  filename: string;
  geo_type: string;
  values: string[];
  total_values: number;
  filters_applied: Record<string, string>;
  engine: string;
}


export interface TotalsByKeyword {
  count: number;
  numerador?: number;
  denominador?: number;
  actividades?: number;
  cobertura_promedio?: number;
}


export interface GlobalStatistics {
  total_actividades: number;
  total_denominador_global: number;
  total_numerador_global: number;
  total_sin_datos_global: number;
  cobertura_global_porcentaje: number;
  actividades_100_pct_cobertura: number;
  actividades_menos_50_pct_cobertura: number;
  mejor_cobertura: number;
  peor_cobertura: number;
  cobertura_promedio: number;
}


export interface KeywordAgeReport {
  success: boolean;
  filename: string;
  corte_fecha: string;
  rules: {
    keywords: string[];
  };
  geographic_filters: GeographicFilters & {
    filter_type?: string;
  };
  items: KeywordAgeReportItem[];
  totals_by_keyword: Record<string, TotalsByKeyword>;
  temporal_data: Record<string, TemporalColumnData>;
  global_statistics?: GlobalStatistics;
  metodo?: string;
  version?: string;
  caracteristicas?: string[];
  ultra_fast?: boolean;
  engine?: string;
  data_source_used?: string;
  message?: string;
  temporal_columns?: number;
}


// ========== NUEVAS INTERFACES PARA CACHE ==========

export interface DirectoryStatus {
  exists: boolean;
  file_count: number;
  size_mb: number;
}


export interface CacheStatusResponse {
  success: boolean;
  directories: {
    duckdb_storage: DirectoryStatus;
    metadata_cache: DirectoryStatus;
    parquet_cache: DirectoryStatus;
    technical_note: DirectoryStatus;
  };
  memory_state: {
    loaded_tables_count: number;
    loaded_technical_files_count: number;
    duckdb_available: boolean;
  };
  timestamp: string;
}


export interface CleanupCacheResponse {
  success: boolean;
  message: string;
  cleaned_directories: string[];
  tables_cleared: number;
  technical_files_cleared: number;
  errors: string[] | null;
  timestamp: string;
}