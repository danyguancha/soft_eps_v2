// interfaces/ITechnicalNote.ts - INTERFACES ACTUALIZADAS
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
  total_items: number;
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


// ========== INTERFACES PARA CACHE ==========


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
    extract_info_nt: DirectoryStatus;
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
  summary: {
    total_files_deleted: number;
    total_files_preserved: number;
    directories_processed: number;
  };
  detailed_results: Array<{
    directory: string;
    files_deleted: string[];
    files_preserved: string[];
    subdirs_deleted: string[];
    errors: string[];
  }>;
  tables_cleared: number;
  technical_files_cleared: number;
  errors: string[] | null;
  excluded_files_config: Record<string, string[]>;
  timestamp: string;
}


// ========== INTERFACES PARA NT RPMS - ACTUALIZADAS ==========


/**
 * Request para procesar desde carpeta compartida en red
 */
export interface NTRPMSNetworkPathRequest {
  network_path: string; // Ejemplo: "\\192.168.1.100\NT_RPMS_Share"
}


/**
 * Request para procesar desde carpeta local del servidor
 */
export interface NTRPMSLocalPathRequest {
  folder_path: string; // Ejemplo: "C:\archivos\NT_RPMS"
}


/**
 * [DEPRECATED] Request genérico (mantener por compatibilidad)
 */
export interface NTRPMSProcessRequest {
  folder_path: string;
}


/**
 * Información de red cuando se procesa desde carpeta compartida
 */
export interface NetworkInfo {
  original_path: string;           // Ruta original ingresada
  resolved_path: string;            // Ruta normalizada/resuelta
  access_type: 'network_share' | 'local'; // Tipo de acceso
  total_files_in_folder?: number;   // Total archivos en carpeta
  excel_files_found?: number;       // Archivos Excel encontrados
}


/**
 * Información de tiempo de procesamiento
 */
export interface ProcessingTiming {
  extraction_time: number;  // Segundos de extracción Excel->CSV
  conversion_time: number;  // Segundos de conversión CSV->Parquet
  total_time: number;       // Tiempo total del proceso
}


/**
 * Información de compresión
 */
export interface CompressionInfo {
  original_size_mb: number;     // Tamaño CSV en MB
  parquet_size_mb: number;      // Tamaño Parquet en MB
  compression_ratio: number;    // Porcentaje de compresión
}


/**
 * Resumen de extracción desde archivos Excel
 */
export interface ExtractionSummary {
  archivos_procesados: number;      // Archivos procesados exitosamente
  archivos_con_errores: number;     // Archivos con errores
  total_registros: number;          // Total registros consolidados
  errores?: Array<[string, string]>; // Lista de [archivo, error]
}


/**
 * Respuesta completa del procesamiento NT RPMS
 */
export interface NTRPMSProcessResponse {
  success: boolean;
  message?: string;
  error?: string;
  suggestion?: string;
  
  // Rutas de archivos generados
  csv_path: string;
  parquet_path: string;
  
  // Información de datos
  total_rows: number;
  total_columns: number;
  columns: string[];
  
  // Información de tiempo
  timing: ProcessingTiming;
  
  // Información de compresión
  compression_info: CompressionInfo;
  
  // Resumen de extracción
  extraction_summary: ExtractionSummary;
  
  // Información de red (solo para process-network)
  network_info?: NetworkInfo;
  
  // Metadata adicional
  from_cache: boolean;
  file_hash: string;
  file_id: string;
  has_geographic_enrichment: boolean;
  
  // Errores (si los hay)
  errors?: string[];
  
  timestamp?: string;
  
  // [DEPRECATED] Campos antiguos (mantener por compatibilidad)
  processing_time_seconds?: number;
  files_processed?: number;
  consolidation_details?: {
    source_folder: string;
    files_found: number;
    files_successfully_processed: number;
    files_with_errors: number;
    total_records_consolidated: number;
  };
}


/**
 * Información de archivo NT RPMS disponible
 */
export interface NTRPMSFileInfo {
  filename: string;
  display_name: string;
  total_rows: number;
  total_columns: number;
  columns: string[];
  parquet_path: string;
  csv_path: string;
  is_available: boolean;
  file_size_mb?: number;
  created_at?: string;
  last_modified?: string;
}


/**
 * Información de archivo NT RPMS procesado (para lista)
 */
export interface NTRPMSProcessedFile {
  filename: string;
  csv_path: string;
  parquet_path: string | null;
  has_parquet: boolean;
  size_mb: number;
  created: string;      // ISO timestamp
  modified: string;     // ISO timestamp
}


/**
 * Respuesta de lista de archivos procesados
 */
export interface NTRPMSListResponse {
  success: boolean;
  files: NTRPMSProcessedFile[];
  count: number;
  message?: string;
}


/**
 * Respuesta de eliminación de archivo NT RPMS
 */
export interface NTRPMSDeleteResponse {
  success: boolean;
  message: string;
  files_deleted?: string[];
}


/**
 * Respuesta de estado de procesamiento por hash
 */
export interface NTRPMSStatusResponse {
  success: boolean;
  file_hash: string;
  parquet_path: string;
  metadata: {
    total_rows?: number;
    total_columns?: number;
    created_at?: string;
    file_size_mb?: number;
  };
  error?: string;
}


/**
 * Opciones de validación de ruta de red
 */
export interface NetworkPathValidation {
  is_valid: boolean;
  normalized_path: string;
  is_network_path: boolean;
  exists: boolean;
  is_directory: boolean;
  has_read_permission: boolean;
  total_files?: number;
  excel_files?: number;
  error_message?: string;
  suggestions?: string[];
}


// ========== TIPOS AUXILIARES ==========


/**
 * Tipo para modo de procesamiento
 */
export type NTRPMSProcessMode = 'network' | 'local' | 'auto';


/**
 * Tipo para estado de procesamiento
 */
export type ProcessingStatus = 
  | 'idle' 
  | 'validating' 
  | 'extracting' 
  | 'converting' 
  | 'completed' 
  | 'error';


/**
 * Tipo para nivel de semáforo
 */
export type SemaforoLevel = 
  | 'Óptimo' 
  | 'Aceptable' 
  | 'Deficiente' 
  | 'Muy Deficiente' 
  | 'NA';


/**
 * Configuración de procesamiento NT RPMS
 */
export interface NTRPMSProcessConfig {
  mode: NTRPMSProcessMode;
  path: string;
  validate_before_process?: boolean;
  enrich_geographic_data?: boolean;
  timeout_seconds?: number;
  retry_on_error?: boolean;
  max_retries?: number;
}


/**
 * Estado del procesamiento NT RPMS (para UI)
 */
export interface NTRPMSProcessingState {
  status: ProcessingStatus;
  progress_percentage: number;
  current_step: string;
  files_processed: number;
  total_files: number;
  elapsed_time_seconds: number;
  estimated_remaining_seconds?: number;
  error_message?: string;
  suggestions?: string[];
}



