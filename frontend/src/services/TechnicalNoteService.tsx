// services/TechnicalNoteService.tsx - REFACTORIZADO CON LIMPIEZA DE CACHE
import api from '../Api';
import type { InasistentesReportResponse } from '../interfaces/IAbsentUser';
import type { AgeRangesResponse } from '../interfaces/IAge';
import type { CacheStatusResponse, CleanupCacheResponse, ColumnUniqueValues, GeographicFilters, 
  GeographicValuesResponse, KeywordAgeReport, TechnicalFileData, TechnicalFileInfo, TechnicalFileMetadata } from '../interfaces/ITechnicalNote';
import type { FilterCondition } from '../types/api.types';




export class TechnicalNoteService {
  static async cleanupAllCache(): Promise<CleanupCacheResponse> {
    try {
      console.log('Limpiando cache del backend...');

      const response = await api.post('/technical-note/cache/cleanup-all', {}, {
        timeout: 30000
      });

      const data = response.data as CleanupCacheResponse;

      if (data.success) {
        console.log('Cache limpiado exitosamente:');
        console.log(`${data.cleaned_directories.length} directorios limpiados`);
        console.log(`${data.tables_cleared} tablas eliminadas`);
        console.log(`${data.technical_files_cleared} archivos técnicos eliminados`);
      } else {
        console.warn('Cache limpiado:', data.errors);
      }

      return data;
    } catch (error) {
      console.error('Error limpiando cache:', error);
      throw error;
    }
  }


  /**
   * Obtiene el estado actual del cache (útil para debugging)
   */
  static async getCacheStatus(): Promise<CacheStatusResponse> {
    try {
      console.log('Obteniendo estado del cache...');

      const response = await api.get('/technical-note/cache/status', {
        timeout: 10000
      });

      const data = response.data as CacheStatusResponse;

      console.log('Estado del cache obtenido:');
      Object.entries(data.directories).forEach(([dir, status]) => {
        console.log(`${dir}: ${status.file_count} archivos (${status.size_mb} MB)`);
      });
      console.log(`Tablas en memoria: ${data.memory_state.loaded_tables_count}`);
      console.log(`Archivos técnicos: ${data.memory_state.loaded_technical_files_count}`);

      return data;
    } catch (error) {
      console.error('Error obteniendo estado del cache:', error);
      throw error;
    }
  }


  // ========== ENDPOINTS EXISTENTES ==========

  static async getAvailableFiles(): Promise<TechnicalFileInfo[]> {
    try {
      console.log('GET /technical-note/available');
      const response = await api.get('/technical-note/available', { timeout: 10000 });
      console.log(`${response.data?.length || 0} archivos disponibles`);
      return response.data;
    } catch (error) {
      console.error('Error obteniendo archivos:', error);
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
        ...(filters && filters.length > 0 && { filters: JSON.stringify(filters) })
      });

      console.log(`GET /technical-note/data/${filename}?${params}`);

      const response = await api.get(`/technical-note/data/${filename}?${params}`, {
        timeout: 45000
      });

      console.log('Respuesta:', {
        status: response.status,
        rowsInPage: response.data?.pagination?.rows_in_page,
        totalFiltered: response.data?.pagination?.total_rows
      });

      return response.data;
    } catch (error) {
      console.error(`Error obteniendo datos:`, error);
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

      console.log(`Obteniendo valores únicos: ${filename} - ${columnName}`);

      const response = await api.get(
        `/technical-note/unique-values/${filename}/${columnName}?${params}`,
        { timeout: 15000 }
      );

      console.log(`Valores únicos obtenidos: ${response.data.total_unique} para ${columnName}`);

      return response.data;
    } catch (error) {
      console.error(`Error obteniendo valores únicos:`, error);
      throw error;
    }
  }


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

      const response = await api.get(url, { timeout: 15000 });

      console.log(`${geoType} obtenidos: ${response.data?.values?.length || 0} valores`);

      return response.data;
    } catch (error) {
      console.error(`Error obteniendo ${geoType}:`, error);
      throw error;
    }
  }


  static async getKeywordAgeReport(
    filename: string,
    cutoffDate: string,
    keywords?: string[],
    minCount: number = 0,
    includeTemporal: boolean = true,
    geographicFilters: GeographicFilters = {}
  ): Promise<KeywordAgeReport> {
    if (!cutoffDate) {
      throw new Error('Fecha de corte es obligatoria');
    }

    try {
      const params = new URLSearchParams({
        corte_fecha: cutoffDate,
        ...(keywords && keywords.length > 0 && { keywords: keywords.join(',') }),
        min_count: minCount.toString(),
        include_temporal: includeTemporal.toString()
      });

      if (geographicFilters.departamento) {
        params.append('departamento', geographicFilters.departamento);
      }
      if (geographicFilters.municipio) {
        params.append('municipio', geographicFilters.municipio);
      }
      if (geographicFilters.ips) {
        params.append('ips', geographicFilters.ips);
      }

      console.log(`Generando reporte con fecha: ${cutoffDate}`);

      const response = await api.get(
        `/technical-note/report/${filename}?${params}`,
        { timeout: 45000 }
      );

      const itemsCount = response.data.items?.length || 0;
      const globalStats = response.data.global_statistics || {};
      const totalDenominador = globalStats.total_denominador_global || 0;
      const totalNumerador = globalStats.total_numerador_global || 0;
      const coberturaGlobal = globalStats.cobertura_global_porcentaje || 0;

      console.log(`Reporte obtenido:`);
      console.log(`${itemsCount} actividades`);
      console.log(`DENOMINADOR: ${totalDenominador.toLocaleString()}`);
      console.log(`NUMERADOR: ${totalNumerador.toLocaleString()}`);
      console.log(`COBERTURA: ${coberturaGlobal}%`);
      console.log(`Fecha: ${cutoffDate}`);

      return response.data;
    } catch (error) {
      console.error(`Error obteniendo reporte:`, error);
      throw error;
    }
  }


  static async getFileMetadata(filename: string): Promise<TechnicalFileMetadata> {
    try {
      console.log(`Obteniendo metadatos: ${filename}`);

      const response = await api.get(`/technical-note/metadata/${filename}`, {
        timeout: 15000
      });

      console.log(`Metadatos obtenidos: ${response.data.total_rows?.toLocaleString()} filas`);

      return response.data;
    } catch (error) {
      console.error(`Error obteniendo metadatos:`, error);
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
      console.log(`Obteniendo columnas: ${filename}`);

      const response = await api.get(`/technical-note/columns/${filename}`, {
        timeout: 10000
      });

      return response.data;
    } catch (error) {
      console.error(`Error obteniendo columnas:`, error);
      throw error;
    }
  }


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


  static async getAgeRanges(
    filename: string,
    cutoffDate: string
  ): Promise<AgeRangesResponse> {
    if (!cutoffDate) {
      throw new Error('Fecha de corte es obligatoria');
    }

    try {
      console.log(`Obteniendo rangos de edades: ${filename} con corte ${cutoffDate}`);

      const params = new URLSearchParams({
        corte_fecha: cutoffDate
      });

      const response = await api.get(
        `/technical-note/age-ranges/${filename}?${params}`,
        { timeout: 30000 }
      );

      const yearsCount = response.data.age_ranges?.years?.length || 0;
      const monthsCount = response.data.age_ranges?.months?.length || 0;

      console.log(`Rangos obtenidos: ${yearsCount} años, ${monthsCount} meses (corte: ${cutoffDate})`);

      return response.data;
    } catch (error) {
      console.error(`Error obteniendo rangos:`, error);
      throw error;
    }
  }


  static async getInasistentesReport(
    filename: string,
    cutoffDate: string,
    selectedMonths: number[],
    selectedYears: number[] = [],
    selectedKeywords: string[] = [],
    geographicFilters: GeographicFilters = {}
  ): Promise<InasistentesReportResponse> {
    if (!cutoffDate) {
      throw new Error('Fecha de corte es obligatoria');
    }

    try {
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
        { timeout: 60000 }
      );

      const totalInasistentes = response.data.resumen_general?.total_inasistentes_global || 0;
      const totalActividades = response.data.resumen_general?.total_actividades_evaluadas || 0;

      console.log(`Reporte obtenido:`);
      console.log(`${totalInasistentes} inasistentes totales`);
      console.log(`${totalActividades} actividades evaluadas`);
      console.log(`Fecha corte: ${cutoffDate}`);

      return response.data;
    } catch (error) {
      console.error(`Error obteniendo reporte inasistentes:`, error);
      throw error;
    }
  }


  static async exportInasistentesCSV(
    filename: string,
    cutoffDate: string,
    selectedMonths: number[],
    selectedYears: number[] = [],
    selectedKeywords: string[] = [],
    geographicFilters: GeographicFilters = {}
  ): Promise<Blob> {
    if (!cutoffDate) {
      throw new Error('Fecha de corte es obligatoria');
    }

    try {
      console.log(`Exportando CSV: ${filename}`);
      console.log(`Fecha corte: ${cutoffDate}`);

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

      console.log(`CSV exportado exitosamente (fecha: ${cutoffDate})`);
      return response.data;

    } catch (error) {
      console.error(`Error exportando CSV:`, error);
      throw error;
    }
  }


  // ===========================
  // MÉTODOS DE UTILIDAD
  // ===========================

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
      console.log(`Archivo descargado: ${filename}`);
    } catch (error) {
      console.error(`Error descargando:`, error);
      throw error;
    }
  }


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
      console.error(`Error descargando desde enlace:`, error);
      throw error;
    }
  }


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
}


export const TechnicalNoteHelpers = {
  formatFileSize: TechnicalNoteService.formatFileSize,
  isLargeFile: TechnicalNoteService.isLargeFile,
  getRecommendedPageSize: TechnicalNoteService.getRecommendedPageSize,
  calculateTotalPages: TechnicalNoteService.calculateTotalPages,
  downloadBlobAsFile: TechnicalNoteService.downloadBlobAsFile,
  downloadFromLink: TechnicalNoteService.downloadFromLink,
  getSemaforoColor: TechnicalNoteService.getSemaforoColor,
  cleanupCache: TechnicalNoteService.cleanupAllCache,
  getCacheStatus: TechnicalNoteService.getCacheStatus
};
