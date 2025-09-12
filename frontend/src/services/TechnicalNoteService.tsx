// services/TechnicalNoteService.tsx - ✅ CON FILTROS GEOGRÁFICOS COMPLETOS
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

// ✅ INTERFACES PARA REPORTE PALABRA CLAVE + EDAD
export interface KeywordAgeItem {
  column: string;
  keyword: string;
  age_range: string;
  count: number;
}

export interface TemporalMonth {
  month: number;
  month_name: string;
  count: number;
}

export interface TemporalYear {
  year: number;
  total: number;
  months: Record<string, TemporalMonth>;
}

export interface TemporalColumnData {
  column: string;
  keyword: string;
  age_range: string;
  years: Record<string, TemporalYear>;
}

// ✅ NUEVAS INTERFACES PARA FILTROS GEOGRÁFICOS
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

export interface KeywordAgeReport {
  success: boolean;
  filename: string;
  rules: {
    keywords: string[];
  };
  geographic_filters?: GeographicFilters; // ✅ NUEVO
  items: KeywordAgeItem[];
  totals_by_keyword: Record<string, number>;
  temporal_data?: Record<string, TemporalColumnData>;
  ultra_fast: boolean;
  engine: string;
  temporal_columns?: number;
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

  // ✅ NUEVOS MÉTODOS PARA FILTROS GEOGRÁFICOS
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

  // ✅ MÉTODO ACTUALIZADO: Reporte con filtros geográficos
  static async getKeywordAgeReport(
    filename: string,
    keywords?: string[],
    minCount: number = 0,
    includeTemporal: boolean = true,
    geographicFilters: GeographicFilters = {}
  ): Promise<KeywordAgeReport> {
    try {
      const params = new URLSearchParams({
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

      const response = await api.get(
        `/technical-note/report/${filename}?${params}`,
        {
          timeout: 45000
        }
      );

      const itemsCount = response.data.items?.length || 0;
      const temporalCount = response.data.temporal_columns || 0;
      console.log(`✅ Reporte geográfico obtenido: ${itemsCount} elementos, ${temporalCount} con datos temporales`);

      return response.data;
    } catch (error) {
      console.error(`❌ Error obteniendo reporte geográfico de ${filename}:`, error);
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

  // ✅ NUEVOS MÉTODOS DE CONVENIENCIA PARA FILTROS GEOGRÁFICOS
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

  //edad
  static async getAgeRanges(
    filename: string,
    corteFecha: string = "2025-07-31"
  ): Promise<AgeRangesResponse> {
    try {
      console.log(`📅 Obteniendo rangos de edades: ${filename} con corte ${corteFecha}`);

      const params = new URLSearchParams({
        corte_fecha: corteFecha
      });

      const response = await api.get(
        `/technical-note/age-ranges/${filename}?${params}`,
        {
          timeout: 30000
        }
      );

      const yearsCount = response.data.age_ranges?.years?.length || 0;
      const monthsCount = response.data.age_ranges?.months?.length || 0;

      console.log(`✅ Rangos obtenidos: ${yearsCount} años, ${monthsCount} meses`);

      return response.data;
    } catch (error) {
      console.error(`❌ Error obteniendo rangos de ${filename}:`, error);
      throw error;
    }
  }

  //inasistentes
  // services/TechnicalNoteService.tsx

  static async getInasistentesReport(
    filename: string,
    selectedMonths: number[],
    selectedYears: number[] = [],
    selectedKeywords: string[] = [],
    corteFecha: string = "2025-07-31",
    geographicFilters: GeographicFilters = {}
  ): Promise<InasistentesReportResponse> {
    try {
      console.log(`🏥 Generando reporte DINÁMICO de inasistentes: ${filename}`);
      console.log(`📅 Filtros edad:`, { selectedMonths, selectedYears });
      console.log(`🔑 Palabras clave:`, selectedKeywords);
      console.log(`🗺️ Filtros geo:`, geographicFilters);

      const requestBody = {
        selectedMonths,
        selectedYears,
        selectedKeywords,
        departamento: geographicFilters.departamento,
        municipio: geographicFilters.municipio,
        ips: geographicFilters.ips
      };

      const params = new URLSearchParams({
        corte_fecha: corteFecha
      });

      const response = await api.post(
        `/technical-note/inasistentes-report/${filename}?${params}`,
        requestBody,
        {
          timeout: 60000
        }
      );

      // ✅ LOGS ACTUALIZADOS PARA NUEVA ESTRUCTURA
      const totalInasistentes = response.data.resumen_general?.total_inasistentes_global || 0;
      const totalActividades = response.data.resumen_general?.total_actividades_evaluadas || 0;
      const actividadesConInasistentes = response.data.resumen_general?.actividades_con_inasistentes || 0;

      console.log(`✅ Reporte dinámico obtenido:`);
      console.log(`   👥 ${totalInasistentes} inasistentes totales`);
      console.log(`   📋 ${totalActividades} actividades evaluadas`);
      console.log(`   🎯 ${actividadesConInasistentes} actividades con inasistencias`);

      // Log de columnas descubiertas
      if (response.data.columnas_descubiertas) {
        console.log(`🔍 Columnas descubiertas:`, response.data.columnas_descubiertas);
      }

      return response.data;
    } catch (error) {
      console.error(`❌ Error obteniendo reporte dinámico de ${filename}:`, error);
      throw error;
    }
  }

  //Exportar inasistentes a csv


  static async exportInasistentesCSV(
    filename: string,
    selectedMonths: number[],
    selectedYears: number[] = [],
    selectedKeywords: string[] = [],
    corteFecha: string = "2025-07-31",
    geographicFilters: GeographicFilters = {}
  ): Promise<Blob> {
    try {
      console.log(`📥 Exportando reporte CSV con caracteres especiales: ${filename}`);

      const requestBody = {
        selectedMonths,
        selectedYears,
        selectedKeywords,
        departamento: geographicFilters.departamento,
        municipio: geographicFilters.municipio,
        ips: geographicFilters.ips
      };

      const params = new URLSearchParams({
        corte_fecha: corteFecha
      });

      const response = await api.post(
        `/technical-note/inasistentes-report/${filename}/export-csv?${params}`,
        requestBody,
        {
          timeout: 120000,
          responseType: 'blob', // ✅ CRÍTICO: Blob para preservar encoding
          headers: {
            'Accept': 'text/csv; charset=utf-8' // ✅ Especificar encoding esperado
          }
        }
      );

      console.log(`✅ CSV con caracteres especiales exportado exitosamente`);
      return response.data;

    } catch (error) {
      console.error(`❌ Error exportando CSV de ${filename}:`, error);
      throw error;
    }
  }





}

export const TechnicalNoteHelpers = {
  formatFileSize: TechnicalNoteService.formatFileSize,
  isLargeFile: TechnicalNoteService.isLargeFile,
  getRecommendedPageSize: TechnicalNoteService.getRecommendedPageSize,
  calculateTotalPages: TechnicalNoteService.calculateTotalPages
};



