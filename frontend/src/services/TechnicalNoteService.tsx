// services/TechnicalNoteService.tsx - ✅ CON FECHA DE CORTE OBLIGATORIA
import api from '../Api';
import type { InasistentesReportResponse } from '../interfaces/IAbsentUser';
import type { AgeRangesResponse } from '../interfaces/IAge';
import type { FilterCondition } from '../types/api.types';

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

// ✅ INTERFACES ACTUALIZADAS PARA NUMERADOR/DENOMINADOR
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

// ✅ INTERFACES PARA FILTROS GEOGRÁFICOS
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
  corte_fecha: string; // ✅ OBLIGATORIO
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

// 🆕 INTERFACES PARA REPORTES AVANZADOS

export interface AdvancedGeographicFilters {
  departamento?: string;
  municipio?: string;
  ips?: string;
}

export interface AdvancedReportRequest {
  data_source: string;
  filename: string;
  keywords?: string[];
  min_count?: number;
  include_temporal?: boolean;
  geographic_filters?: AdvancedGeographicFilters;
  corte_fecha: string;
}

export interface AdvancedExportOptions {
  export_csv?: boolean;
  export_pdf?: boolean;
  include_temporal?: boolean;
}

export interface AdvancedReportResponse {
  success: boolean;
  message: string;
  report_id?: string;
  data?: KeywordAgeReport;
  execution_time_seconds?: number;
}

export interface AdvancedExportResponse {
  success: boolean;
  message: string;
  files: Record<string, string>;
  download_links: Record<string, string>;
}

export interface AdvancedReportHistoryItem {
  report_id: string;
  filename: string;
  data_source: string;
  keywords: string[];
  created_at: string;
  items_count: number;
  has_temporal: boolean;
  has_semaforization?: boolean;
  corte_fecha?: string; // ✅ AGREGAR
}

export interface AdvancedReportsHistoryResponse {
  success: boolean;
  reports: AdvancedReportHistoryItem[];
  total_reports: number;
  limit: number;
  offset: number;
}

export class TechnicalNoteService {

  static async getAvailableFiles(): Promise<TechnicalFileInfo[]> {
    try {
      console.log('🌐 API Request: GET /technical-note/available');
      const response = await api.get('/technical-note/available', {
        timeout: 10000
      });
      console.log(`✅ ${response.data?.length || 0} archivos disponibles`);
      return response.data;
    } catch (error) {
      console.error('❌ Error obteniendo archivos técnicos:', error);
      throw error;
    }
  }

  static async getFileData(
    filename: string,
    page: number = 1,
    pageSize: number = 1000,
    sheetName?: string,
    filters?: FilterCondition[],
    search?: string,
    sortBy?: string,
    sortOrder?: 'asc' | 'desc'
  ): Promise<TechnicalFileData> {
    try {
      const params = new URLSearchParams({
        page: page.toString(),
        page_size: pageSize.toString(),
        ...(sheetName && { sheet_name: sheetName }),
        ...(search && { search: search.trim() }),
        ...(sortBy && { sort_by: sortBy }),
        ...(sortOrder && { sort_order: sortOrder }),
        ...(filters && filters.length > 0 && {
          filters: JSON.stringify(filters)
        })
      });

      console.log(`🌐 API Request con filtros estilo Excel: GET /technical-note/${filename}?${params}`);

      const response = await api.get(`/technical-note/data/${filename}?${params}`, {
        timeout: 45000
      });

      console.log('✅ Respuesta con filtros estilo Excel:', {
        status: response.status,
        rowsInPage: response.data?.pagination?.rows_in_page,
        totalFiltered: response.data?.pagination?.total_rows,
        originalTotal: response.data?.pagination?.original_total,
        filtered: response.data?.pagination?.filtered
      });

      return response.data;
    } catch (error) {
      console.error(`❌ Error con filtros estilo Excel:`, error);
      throw error;
    }
  }

  static async getColumnUniqueValues(
    filename: string,
    columnName: string,
    sheetName?: string,
    limit: number = 1000
  ): Promise<ColumnUniqueValues> {
    try {
      const params = new URLSearchParams({
        ...(sheetName && { sheet_name: sheetName }),
        limit: limit.toString()
      });

      console.log(`🔍 Obteniendo valores únicos estilo Excel: ${filename} - ${columnName}`);

      const response = await api.get(
        `/technical-note/unique-values/${filename}/${columnName}?${params}`,
        {
          timeout: 15000
        }
      );

      console.log(`✅ Valores únicos estilo Excel obtenidos: ${response.data.total_unique} para ${columnName}`);

      return response.data;
    } catch (error) {
      console.error(`❌ Error obteniendo valores únicos de ${columnName}:`, error);
      throw error;
    }
  }

  // ✅ MÉTODOS PARA FILTROS GEOGRÁFICOS
  static async getGeographicValues(
    filename: string,
    geoType: 'departamentos' | 'municipios' | 'ips',
    filters: GeographicFilters = {}
  ): Promise<GeographicValuesResponse> {
    try {
      const params = new URLSearchParams();

      if (filters.departamento) {
        params.append('departamento', filters.departamento);
      }
      if (filters.municipio) {
        params.append('municipio', filters.municipio);
      }

      const url = `/technical-note/geographic/${filename}/${geoType}${params.toString() ? `?${params}` : ''}`;

      console.log(`🗺️ Obteniendo ${geoType}: GET ${url}`);

      const response = await api.get(url, {
        timeout: 15000
      });

      console.log(`✅ ${geoType} obtenidos: ${response.data?.values?.length || 0} valores`);

      return response.data;
    } catch (error) {
      console.error(`❌ Error obteniendo ${geoType}:`, error);
      throw error;
    }
  }

  /**
   * ✅ MÉTODO PRINCIPAL: Reporte con numerador/denominador
   * @param filename - Nombre del archivo a analizar
   * @param cutoffDate - Fecha de corte OBLIGATORIA en formato YYYY-MM-DD
   * @param keywords - Palabras clave para filtrar columnas
   * @param minCount - Conteo mínimo para incluir en resultados
   * @param includeTemporal - Incluir análisis temporal
   * @param geographicFilters - Filtros geográficos opcionales
   */
  static async getKeywordAgeReport(
    filename: string,
    cutoffDate: string, // ✅ PARÁMETRO REORDENADO Y OBLIGATORIO
    keywords?: string[],
    minCount: number = 0,
    includeTemporal: boolean = true,
    geographicFilters: GeographicFilters = {}
  ): Promise<KeywordAgeReport> {
    // ✅ VALIDACIÓN: Fecha de corte obligatoria
    if (!cutoffDate) {
      throw new Error('❌ Fecha de corte es obligatoria para generar el reporte');
    }

    try {
      const params = new URLSearchParams({
        corte_fecha: cutoffDate, // ✅ PRIMER PARÁMETRO
        ...(keywords && keywords.length > 0 && { keywords: keywords.join(',') }),
        min_count: minCount.toString(),
        include_temporal: includeTemporal.toString()
      });

      // ✅ AGREGAR FILTROS GEOGRÁFICOS
      if (geographicFilters.departamento) {
        params.append('departamento', geographicFilters.departamento);
      }
      if (geographicFilters.municipio) {
        params.append('municipio', geographicFilters.municipio);
      }
      if (geographicFilters.ips) {
        params.append('ips', geographicFilters.ips);
      }

      console.log(`📊 Generando reporte con fecha de corte: ${cutoffDate}`);

      const response = await api.get(
        `/technical-note/report/${filename}?${params}`,
        {
          timeout: 45000
        }
      );

      const itemsCount = response.data.items?.length || 0;
      const temporalCount = response.data.temporal_columns || 0;
      
      const globalStats = response.data.global_statistics || {};
      const totalDenominador = globalStats.total_denominador_global || 0;
      const totalNumerador = globalStats.total_numerador_global || 0;
      const coberturaGlobal = globalStats.cobertura_global_porcentaje || 0;
      const metodo = response.data.metodo || 'No especificado';
      
      console.log(`✅ Reporte numerador/denominador obtenido:`);
      console.log(`   📊 ${itemsCount} actividades encontradas`);
      console.log(`   📊 DENOMINADOR GLOBAL: ${totalDenominador.toLocaleString()}`);
      console.log(`   ✅ NUMERADOR GLOBAL: ${totalNumerador.toLocaleString()}`);
      console.log(`   📈 COBERTURA GLOBAL: ${coberturaGlobal}%`);
      console.log(`   🎯 Método: ${metodo}`);
      console.log(`   🗓️ Fecha corte: ${cutoffDate}`);
      
      if (globalStats.actividades_100_pct_cobertura !== undefined) {
        console.log(`   ✅ Actividades 100%: ${globalStats.actividades_100_pct_cobertura}`);
        console.log(`   ⚠️ Actividades <50%: ${globalStats.actividades_menos_50_pct_cobertura}`);
      }

      return response.data;
    } catch (error) {
      console.error(`❌ Error obteniendo reporte numerador/denominador de ${filename}:`, error);
      throw error;
    }
  }

  static async getFileMetadata(filename: string): Promise<TechnicalFileMetadata> {
    try {
      console.log(`📋 Obteniendo metadatos: ${filename}`);

      const response = await api.get(`/technical-note/metadata/${filename}`, {
        timeout: 15000
      });

      console.log(`✅ Metadatos obtenidos: ${response.data.total_rows?.toLocaleString()} filas`);

      return response.data;
    } catch (error) {
      console.error(`Error obteniendo metadatos de ${filename}:`, error);
      throw error;
    }
  }

  static async getFileColumns(filename: string): Promise<{
    filename: string;
    columns: string[];
    total_columns: number;
    display_name: string;
  }> {
    try {
      console.log(`📋 Obteniendo columnas: ${filename}`);

      const response = await api.get(`/technical-note/columns/${filename}`, {
        timeout: 10000
      });

      return response.data;
    } catch (error) {
      console.error(`Error obteniendo columnas de ${filename}:`, error);
      throw error;
    }
  }

  // ✅ MÉTODOS DE CONVENIENCIA PARA FILTROS GEOGRÁFICOS
  static async getDepartamentos(filename: string): Promise<string[]> {
    try {
      const result = await this.getGeographicValues(filename, 'departamentos');
      return result.success ? result.values : [];
    } catch (error) {
      console.error('Error obteniendo departamentos:', error);
      return [];
    }
  }

  static async getMunicipios(filename: string, departamento: string): Promise<string[]> {
    try {
      const result = await this.getGeographicValues(filename, 'municipios', { departamento });
      return result.success ? result.values : [];
    } catch (error) {
      console.error('Error obteniendo municipios:', error);
      return [];
    }
  }

  static async getIps(filename: string, departamento: string, municipio: string): Promise<string[]> {
    try {
      const result = await this.getGeographicValues(filename, 'ips', { departamento, municipio });
      return result.success ? result.values : [];
    } catch (error) {
      console.error('Error obteniendo IPS:', error);
      return [];
    }
  }

  // Métodos de utilidad
  static formatFileSize(bytes: number): string {
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    if (bytes === 0) return '0 Bytes';
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return Math.round(bytes / Math.pow(1024, i) * 100) / 100 + ' ' + sizes[i];
  }

  static isLargeFile(totalRows: number): boolean {
    return totalRows > 10000;
  }

  static getRecommendedPageSize(totalRows: number): number {
    if (totalRows <= 1000) return 100;
    if (totalRows <= 10000) return 500;
    if (totalRows <= 50000) return 1000;
    return 1500;
  }

  static calculateTotalPages(totalRows: number, pageSize: number): number {
    return Math.ceil(totalRows / pageSize);
  }

  /**
   * ✅ MÉTODO DE RANGOS DE EDAD
   * @param filename - Nombre del archivo
   * @param cutoffDate - Fecha de corte OBLIGATORIA en formato YYYY-MM-DD
   */
  static async getAgeRanges(
    filename: string,
    cutoffDate: string // ✅ OBLIGATORIO
  ): Promise<AgeRangesResponse> {
    // ✅ VALIDACIÓN
    if (!cutoffDate) {
      throw new Error('❌ Fecha de corte es obligatoria para obtener rangos de edad');
    }

    try {
      console.log(`📅 Obteniendo rangos de edades: ${filename} con corte ${cutoffDate}`);

      const params = new URLSearchParams({
        corte_fecha: cutoffDate
      });

      const response = await api.get(
        `/technical-note/age-ranges/${filename}?${params}`,
        {
          timeout: 30000
        }
      );

      const yearsCount = response.data.age_ranges?.years?.length || 0;
      const monthsCount = response.data.age_ranges?.months?.length || 0;

      console.log(`✅ Rangos obtenidos: ${yearsCount} años, ${monthsCount} meses (corte: ${cutoffDate})`);

      return response.data;
    } catch (error) {
      console.error(`❌ Error obteniendo rangos de ${filename}:`, error);
      throw error;
    }
  }

  /**
   * ✅ MÉTODO DE REPORTE DE INASISTENTES
   * @param filename - Nombre del archivo
   * @param cutoffDate - Fecha de corte OBLIGATORIA en formato YYYY-MM-DD
   * @param selectedMonths - Meses seleccionados para filtrar
   * @param selectedYears - Años seleccionados para filtrar
   * @param selectedKeywords - Palabras clave para buscar actividades
   * @param geographicFilters - Filtros geográficos opcionales
   */
  static async getInasistentesReport(
    filename: string,
    cutoffDate: string, // ✅ SEGUNDO PARÁMETRO OBLIGATORIO
    selectedMonths: number[],
    selectedYears: number[] = [],
    selectedKeywords: string[] = [],
    geographicFilters: GeographicFilters = {}
  ): Promise<InasistentesReportResponse> {
    // ✅ VALIDACIÓN
    if (!cutoffDate) {
      throw new Error('❌ Fecha de corte es obligatoria para generar reporte de inasistentes');
    }

    try {
      console.log(`🏥 Generando reporte DINÁMICO de inasistentes: ${filename}`);
      console.log(`📅 Filtros edad:`, { selectedMonths, selectedYears });
      console.log(`🔑 Palabras clave:`, selectedKeywords);
      console.log(`🗺️ Filtros geo:`, geographicFilters);
      console.log(`🗓️ Fecha corte: ${cutoffDate}`);

      const requestBody = {
        selectedMonths,
        selectedYears,
        selectedKeywords,
        departamento: geographicFilters.departamento,
        municipio: geographicFilters.municipio,
        ips: geographicFilters.ips
      };

      const params = new URLSearchParams({
        corte_fecha: cutoffDate
      });

      const response = await api.post(
        `/technical-note/inasistentes-report/${filename}?${params}`,
        requestBody,
        {
          timeout: 60000
        }
      );

      const totalInasistentes = response.data.resumen_general?.total_inasistentes_global || 0;
      const totalActividades = response.data.resumen_general?.total_actividades_evaluadas || 0;
      const actividadesConInasistentes = response.data.resumen_general?.actividades_con_inasistentes || 0;

      console.log(`✅ Reporte dinámico obtenido:`);
      console.log(`   👥 ${totalInasistentes} inasistentes totales`);
      console.log(`   📋 ${totalActividades} actividades evaluadas`);
      console.log(`   🎯 ${actividadesConInasistentes} actividades con inasistencias`);
      console.log(`   🗓️ Fecha corte: ${cutoffDate}`);

      if (response.data.columnas_descubiertas) {
        console.log(`🔍 Columnas descubiertas:`, response.data.columnas_descubiertas);
      }

      return response.data;
    } catch (error) {
      console.error(`❌ Error obteniendo reporte dinámico de ${filename}:`, error);
      throw error;
    }
  }

  /**
   * ✅ MÉTODO DE EXPORTACIÓN CSV
   * @param filename - Nombre del archivo
   * @param cutoffDate - Fecha de corte OBLIGATORIA en formato YYYY-MM-DD
   * @param selectedMonths - Meses seleccionados
   * @param selectedYears - Años seleccionados
   * @param selectedKeywords - Palabras clave
   * @param geographicFilters - Filtros geográficos
   */
  static async exportInasistentesCSV(
    filename: string,
    cutoffDate: string, // ✅ SEGUNDO PARÁMETRO OBLIGATORIO
    selectedMonths: number[],
    selectedYears: number[] = [],
    selectedKeywords: string[] = [],
    geographicFilters: GeographicFilters = {}
  ): Promise<Blob> {
    // ✅ VALIDACIÓN
    if (!cutoffDate) {
      throw new Error('❌ Fecha de corte es obligatoria para exportar CSV');
    }

    try {
      console.log(`📥 Exportando reporte CSV: ${filename}`);
      console.log(`🗓️ Fecha corte: ${cutoffDate}`);

      const requestBody = {
        selectedMonths,
        selectedYears,
        selectedKeywords,
        departamento: geographicFilters.departamento,
        municipio: geographicFilters.municipio,
        ips: geographicFilters.ips
      };

      const params = new URLSearchParams({
        corte_fecha: cutoffDate
      });

      const response = await api.post(
        `/technical-note/inasistentes-report/${filename}/export-csv?${params}`,
        requestBody,
        {
          timeout: 120000,
          responseType: 'blob',
          headers: {
            'Accept': 'text/csv; charset=utf-8'
          }
        }
      );

      console.log(`✅ CSV exportado exitosamente (fecha: ${cutoffDate})`);
      return response.data;

    } catch (error) {
      console.error(`❌ Error exportando CSV de ${filename}:`, error);
      throw error;
    }
  }

  // ===========================
  // 🆕 MÉTODOS DE REPORTES AVANZADOS CON EXPORTACIÓN
  // ===========================

  /**
   * 🆕 GENERAR REPORTE AVANZADO CON SEMAFORIZACIÓN
   * @param request - Configuración del reporte (DEBE incluir corte_fecha)
   */
  static async generateAdvancedReport(
    request: AdvancedReportRequest
  ): Promise<AdvancedReportResponse> {
    // ✅ VALIDACIÓN
    if (!request.corte_fecha) {
      throw new Error('❌ Fecha de corte es obligatoria en AdvancedReportRequest');
    }

    try {
      console.log('🚀 Generando reporte avanzado con fecha:', request.corte_fecha);

      const response = await api.post(
        '/technical-note/reports/generate',
        request,
        {
          timeout: 60000
        }
      );

      const result = response.data;
      
      console.log(`✅ Reporte avanzado generado:`);
      console.log(`   📊 Items: ${result.data?.items?.length || 0}`);
      console.log(`   ⏱️ Tiempo: ${result.execution_time_seconds?.toFixed(2)}s`);
      console.log(`   🆔 ID: ${result.report_id}`);
      console.log(`   🗓️ Fecha corte: ${request.corte_fecha}`);
      
      const globalStats = result.data?.global_statistics;
      if (globalStats) {
        console.log(`   📊 DENOMINADOR: ${globalStats.total_denominador_global?.toLocaleString()}`);
        console.log(`   ✅ NUMERADOR: ${globalStats.total_numerador_global?.toLocaleString()}`);
        console.log(`   📈 COBERTURA: ${globalStats.cobertura_global_porcentaje}%`);
      }

      return result;
    } catch (error) {
      console.error('❌ Error generando reporte avanzado:', error);
      throw error;
    }
  }

  /**
   * 🚀 GENERAR Y EXPORTAR REPORTE AVANZADO
   * @param request - Configuración del reporte (DEBE incluir corte_fecha)
   * @param exportOptions - Opciones de exportación
   */
  static async generateAndExportAdvancedReport(
    request: AdvancedReportRequest,
    exportOptions: AdvancedExportOptions
  ): Promise<AdvancedExportResponse> {
    // ✅ VALIDACIÓN
    if (!request.corte_fecha) {
      throw new Error('❌ Fecha de corte es obligatoria para exportar reporte avanzado');
    }

    try {
      console.log('🚀 Generando y exportando reporte avanzado:', { 
        ...request, 
        fecha: request.corte_fecha,
        exportOptions 
      });

      const response = await api.post(
        '/technical-note/reports/generate-and-export',
        {
          ...request,
          ...exportOptions
        },
        {
          timeout: 120000
        }
      );

      const result = response.data;
      
      console.log(`✅ Reporte avanzado generado y exportado:`);
      console.log(`   📄 Archivos: ${Object.keys(result.files || {}).join(', ')}`);
      console.log(`   🔗 Enlaces:`, result.download_links);
      console.log(`   🗓️ Fecha corte: ${request.corte_fecha}`);

      if (!result.success) {
        throw new Error(result.message || 'Error en la generación del reporte');
      }

      return result;
    } catch (error) {
      console.error('❌ Error generando y exportando reporte avanzado:', error);
      throw error;
    }
  }

  /**
   * 📤 EXPORTAR REPORTE AVANZADO EXISTENTE
   */
  static async exportAdvancedReport(
    reportId: string,
    exportOptions: AdvancedExportOptions
  ): Promise<AdvancedExportResponse> {
    try {
      console.log(`📤 Exportando reporte avanzado existente: ${reportId}`, exportOptions);

      const response = await api.post(
        `/technical-note/reports/export/${reportId}`,
        exportOptions,
        {
          timeout: 90000
        }
      );

      const result = response.data;
      
      console.log(`✅ Reporte avanzado ${reportId} exportado:`);
      console.log(`   📄 Archivos: ${Object.keys(result.files || {}).join(', ')}`);

      return result;
    } catch (error) {
      console.error(`❌ Error exportando reporte avanzado ${reportId}:`, error);
      throw error;
    }
  }

  /**
   * 📥 DESCARGAR ARCHIVO DE REPORTE AVANZADO
   */
  static async downloadAdvancedReportFile(fileId: string): Promise<Blob> {
    try {
      console.log(`📥 Descargando archivo de reporte avanzado: ${fileId}`);

      const response = await api.get(
        `/technical-note/reports/download/${fileId}`,
        {
          responseType: 'blob',
          timeout: 60000
        }
      );

      console.log(`✅ Archivo de reporte avanzado descargado: ${fileId}`);
      return response.data;
    } catch (error) {
      console.error(`❌ Error descargando archivo ${fileId}:`, error);
      throw error;
    }
  }

  /**
   * 📋 OBTENER HISTORIAL DE REPORTES AVANZADOS
   */
  static async getAdvancedReportsHistory(
    limit: number = 20,
    offset: number = 0
  ): Promise<AdvancedReportsHistoryResponse> {
    try {
      console.log(`📋 Obteniendo historial de reportes avanzados (${limit}, ${offset})`);

      const params = new URLSearchParams({
        limit: limit.toString(),
        offset: offset.toString()
      });

      const response = await api.get(
        `/technical-note/reports/history?${params}`,
        {
          timeout: 15000
        }
      );

      const result = response.data;
      
      console.log(`✅ Historial obtenido: ${result.reports?.length || 0} reportes`);

      return result;
    } catch (error) {
      console.error('❌ Error obteniendo historial de reportes avanzados:', error);
      throw error;
    }
  }

  /**
   * 🗑️ ELIMINAR REPORTE AVANZADO
   */
  static async deleteAdvancedReport(reportId: string): Promise<{ success: boolean; message: string }> {
    try {
      console.log(`🗑️ Eliminando reporte avanzado: ${reportId}`);

      const response = await api.delete(
        `/technical-note/reports/reports/${reportId}`,
        {
          timeout: 10000
        }
      );

      console.log(`✅ Reporte avanzado ${reportId} eliminado`);
      return response.data;
    } catch (error) {
      console.error(`❌ Error eliminando reporte avanzado ${reportId}:`, error);
      throw error;
    }
  }

  /**
   * 🏥 VERIFICAR ESTADO DEL SERVICIO DE REPORTES AVANZADOS
   */
  static async getAdvancedReportsHealth(): Promise<{
    status: string;
    service: string;
    version: string;
    timestamp: string;
    active_reports: number;
    temp_files: number;
    features: string[];
  }> {
    try {
      const response = await api.get(
        '/technical-note/reports/health',
        {
          timeout: 5000
        }
      );

      return response.data;
    } catch (error) {
      console.error('❌ Error verificando estado de reportes avanzados:', error);
      throw error;
    }
  }

  // ===========================
  // 🛠️ MÉTODOS AUXILIARES PARA EXPORTACIÓN Y DESCARGA
  // ===========================

  /**
   * 📥 DESCARGAR ARCHIVO DESDE BLOB CON NOMBRE ESPECÍFICO
   */
  static downloadBlobAsFile(blob: Blob, filename: string): void {
    try {
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(url);
      
      console.log(`✅ Archivo descargado: ${filename}`);
    } catch (error) {
      console.error(`❌ Error descargando archivo ${filename}:`, error);
      throw error;
    }
  }

  /**
   * 📥 DESCARGAR ARCHIVO DESDE URL DE DESCARGA
   */
  static async downloadFromLink(downloadLink: string, filename?: string): Promise<void> {
    try {
      console.log(`📥 Descargando desde enlace: ${downloadLink}`);

      const response = await api.get(downloadLink, {
        responseType: 'blob',
        timeout: 60000
      });

      const contentDisposition = response.headers['content-disposition'];
      let finalFilename = filename;
      
      if (!finalFilename && contentDisposition) {
        const match = contentDisposition.match(/filename[^;=\n]*=(['"]?)([^'"\n]*?)\1/);
        if (match && match[2]) {
          finalFilename = match[2];
        }
      }

      if (!finalFilename) {
        finalFilename = `archivo_${new Date().getTime()}`;
      }

      this.downloadBlobAsFile(response.data, finalFilename);
    } catch (error) {
      console.error(`❌ Error descargando desde enlace ${downloadLink}:`, error);
      throw error;
    }
  }

  /**
   * 🚀 GENERAR, EXPORTAR Y DESCARGAR REPORTE COMPLETO
   * @param request - Configuración del reporte (DEBE incluir corte_fecha)
   * @param exportOptions - Opciones de exportación
   */
  static async generateExportAndDownloadReport(
    request: AdvancedReportRequest,
    exportOptions: AdvancedExportOptions = { export_csv: true, export_pdf: true }
  ): Promise<void> {
    // ✅ VALIDACIÓN
    if (!request.corte_fecha) {
      throw new Error('❌ Fecha de corte es obligatoria para el proceso completo');
    }

    try {
      console.log('🚀 Proceso completo con fecha:', request.corte_fecha);
      
      const exportResult = await this.generateAndExportAdvancedReport(request, exportOptions);
      
      if (!exportResult.success) {
        throw new Error(exportResult.message || 'Error en la exportación');
      }

      const downloadPromises = Object.entries(exportResult.download_links).map(
        async ([format, link]) => {
          const filename = `${request.filename}_${format.toUpperCase()}_${request.corte_fecha}.${format}`;
          await this.downloadFromLink(link, filename);
        }
      );

      await Promise.all(downloadPromises);
      
      console.log(`✅ Proceso completo finalizado: ${Object.keys(exportResult.download_links).length} archivos descargados`);
      
    } catch (error) {
      console.error('❌ Error en proceso completo de reporte:', error);
      throw error;
    }
  }

  /**
   * 📊 OBTENER RESUMEN DE CAPACIDADES DE REPORTES
   */
  static getAdvancedReportingCapabilities(): {
    features: string[];
    formats: string[];
    semaforization_levels: string[];
    temporal_analysis: boolean;
    excel_logic: boolean;
    requires_cutoff_date: boolean; // ✅ NUEVO
  } {
    return {
      features: [
        'Fecha de corte obligatoria para cálculo preciso de edades',
        'Semaforización automática por desempeño',
        'Numeradores y denominadores por rango de edad',
        'Análisis temporal mensual y anual',
        'Lógica Excel para cálculo de denominadores',
        'Filtros geográficos avanzados',
        'Exportación CSV con codificación Latin1',
        'Exportación PDF personalizable'
      ],
      formats: ['CSV', 'PDF'],
      semaforization_levels: ['Óptimo', 'Aceptable', 'Deficiente', 'Muy Deficiente', 'NA'],
      temporal_analysis: true,
      excel_logic: true,
      requires_cutoff_date: true // ✅ NUEVO
    };
  }

  // ===========================
  // 🔄 MÉTODOS AUXILIARES ADICIONALES
  // ===========================

  /**
   * 📥 DESCARGAR MÚLTIPLES ARCHIVOS DE FORMA SECUENCIAL
   */
  static async downloadMultipleFiles(
    downloadLinks: Record<string, string>,
    baseFilename: string = 'reporte',
    cutoffDate?: string, // ✅ OPCIONAL: Incluir en nombre de archivo
    delayBetweenDownloads: number = 1000
  ): Promise<void> {
    try {
      console.log(`📥 Descargando ${Object.keys(downloadLinks).length} archivos`);
      
      for (const [format, link] of Object.entries(downloadLinks)) {
        const dateSuffix = cutoffDate ? `_${cutoffDate}` : `_${new Date().toISOString().split('T')[0]}`;
        const filename = `${baseFilename}_${format.toUpperCase()}${dateSuffix}.${format}`;
        
        await this.downloadFromLink(link, filename);
        
        if (delayBetweenDownloads > 0) {
          await new Promise(resolve => setTimeout(resolve, delayBetweenDownloads));
        }
      }
      
      console.log(`✅ Todas las descargas completadas`);
    } catch (error) {
      console.error('❌ Error descargando múltiples archivos:', error);
      throw error;
    }
  }

  /**
   * 📊 VALIDAR ESTRUCTURA DE REPORTE AVANZADO
   */
  static validateAdvancedReportRequest(request: AdvancedReportRequest): {
    isValid: boolean;
    errors: string[];
  } {
    const errors: string[] = [];

    if (!request.data_source || request.data_source.trim() === '') {
      errors.push('data_source es requerido');
    }

    if (!request.filename || request.filename.trim() === '') {
      errors.push('filename es requerido');
    }

    // ✅ VALIDACIÓN OBLIGATORIA DE FECHA
    if (!request.corte_fecha || request.corte_fecha.trim() === '') {
      errors.push('corte_fecha es requerido');
    } else {
      const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
      if (!dateRegex.test(request.corte_fecha)) {
        errors.push('corte_fecha debe tener formato YYYY-MM-DD');
      }
    }

    if (request.min_count !== undefined && request.min_count < 0) {
      errors.push('min_count debe ser mayor o igual a 0');
    }

    if (request.keywords && request.keywords.length === 0) {
      errors.push('keywords no puede ser un array vacío, usar undefined si no hay keywords');
    }

    return {
      isValid: errors.length === 0,
      errors
    };
  }

  /**
   * 🔧 CONSTRUIR URL DE DESCARGA COMPLETA
   */
  static buildDownloadUrl(baseUrl: string, fileId: string): string {
    return `${baseUrl}/technical-note/reports/download/${fileId}`;
  }

  /**
   * 📊 FORMATEAR ESTADÍSTICAS GLOBALES PARA MOSTRAR
   */
  static formatGlobalStatistics(globalStats: GlobalStatistics): {
    denominador: string;
    numerador: string;
    cobertura: string;
    actividades_optimas: string;
    actividades_deficientes: string;
    mejor_cobertura: string;
    peor_cobertura: string;
  } {
    return {
      denominador: globalStats.total_denominador_global?.toLocaleString() || '0',
      numerador: globalStats.total_numerador_global?.toLocaleString() || '0',
      cobertura: `${globalStats.cobertura_global_porcentaje?.toFixed(1) || '0.0'}%`,
      actividades_optimas: globalStats.actividades_100_pct_cobertura?.toString() || '0',
      actividades_deficientes: globalStats.actividades_menos_50_pct_cobertura?.toString() || '0',
      mejor_cobertura: `${globalStats.mejor_cobertura?.toFixed(1) || '0.0'}%`,
      peor_cobertura: `${globalStats.peor_cobertura?.toFixed(1) || '0.0'}%`
    };
  }

  /**
   * 🎨 OBTENER COLOR DE SEMAFORIZACIÓN
   */
  static getSemaforoColor(estado: string): string {
    const colores = {
      'Óptimo': '#4CAF50',
      'Aceptable': '#FF9800', 
      'Deficiente': '#FF5722',
      'Muy Deficiente': '#F44336',
      'NA': '#9E9E9E'
    };
    return colores[estado as keyof typeof colores] || '#9E9E9E';
  }

  /**
   * 📈 CALCULAR TENDENCIA DE COBERTURA
   */
  static calculateCoverageTrend(temporalData: TemporalColumnData[]): {
    trend: 'ascending' | 'descending' | 'stable';
    variation: number;
  } {
    if (!temporalData || temporalData.length === 0) {
      return { trend: 'stable', variation: 0 };
    }

    const coverages: number[] = [];
    
    temporalData.forEach(item => {
      Object.values(item.years).forEach(year => {
        if (year.pct !== undefined) {
          coverages.push(year.pct);
        }
      });
    });

    if (coverages.length < 2) {
      return { trend: 'stable', variation: 0 };
    }

    const firstValue = coverages[0];
    const lastValue = coverages[coverages.length - 1];
    const variation = lastValue - firstValue;

    let trend: 'ascending' | 'descending' | 'stable' = 'stable';
    
    if (variation > 2) {
      trend = 'ascending';
    } else if (variation < -2) {
      trend = 'descending';
    }

    return { trend, variation: Math.abs(variation) };
  }
}

export const TechnicalNoteHelpers = {
  formatFileSize: TechnicalNoteService.formatFileSize,
  isLargeFile: TechnicalNoteService.isLargeFile,
  getRecommendedPageSize: TechnicalNoteService.getRecommendedPageSize,
  calculateTotalPages: TechnicalNoteService.calculateTotalPages,
  downloadBlobAsFile: TechnicalNoteService.downloadBlobAsFile,
  downloadFromLink: TechnicalNoteService.downloadFromLink,
  getAdvancedReportingCapabilities: TechnicalNoteService.getAdvancedReportingCapabilities,
  downloadMultipleFiles: TechnicalNoteService.downloadMultipleFiles,
  validateAdvancedReportRequest: TechnicalNoteService.validateAdvancedReportRequest,
  buildDownloadUrl: TechnicalNoteService.buildDownloadUrl,
  formatGlobalStatistics: TechnicalNoteService.formatGlobalStatistics,
  getSemaforoColor: TechnicalNoteService.getSemaforoColor,
  calculateCoverageTrend: TechnicalNoteService.calculateCoverageTrend
};
