// services/TechnicalNoteService.tsx - CÓDIGO COMPLETO
import api from '../Api';
import type { InasistentesReportResponse } from '../interfaces/IAbsentUser';
import type { AgeRangesResponse } from '../interfaces/IAge';
import type {
  CacheStatusResponse,
  CleanupCacheResponse,
  ColumnUniqueValues,
  GeographicFilters,
  GeographicValuesResponse,
  KeywordAgeReport,
  NTRPMSProcessRequest,
  NTRPMSProcessResponse,
  NTRPMSFileInfo,
  TechnicalFileData,
  TechnicalFileInfo,
  TechnicalFileMetadata
} from '../interfaces/ITechnicalNote';
import type { FilterCondition } from '../types/api.types';

export class TechnicalNoteService {

  // ========================================
  // NT RPMS METHODS
  // ========================================

  static async processNTRPMSFolder(folderPath: string): Promise<NTRPMSProcessResponse> {
    try {
      console.log('='.repeat(60));
      console.log('PROCESANDO NT RPMS');
      console.log('='.repeat(60));
      console.log(`📁 Carpeta: ${folderPath}`);

      const requestBody: NTRPMSProcessRequest = {
        folder_path: folderPath
      };

      const response = await api.post<NTRPMSProcessResponse>(
        '/technical-note/nt-rpms/process',
        requestBody,
        {
          timeout: 300000,
          headers: {
            'Content-Type': 'application/json'
          }
        }
      );

      const data = response.data;

      if (!data) {
        throw new Error('No se recibió respuesta del servidor');
      }

      console.log('📦 Respuesta recibida:', JSON.stringify(data, null, 2));

      if (data.success) {
        const filesProcessed = data.files_processed || 0;
        const totalRows = data.total_rows || 0;
        const totalColumns = data.total_columns || 0;
        const processingTime = data.processing_time_seconds; // Puede ser undefined

        console.log('✓ Procesamiento exitoso:');
        console.log(`  - Archivos procesados: ${filesProcessed}`);
        console.log(`  - Registros totales: ${totalRows.toLocaleString()}`);
        console.log(`  - Columnas: ${totalColumns}`);
        console.log(`  - CSV: ${data.csv_path || 'N/A'}`);
        console.log(`  - Parquet: ${data.parquet_path || 'N/A'}`);

        // VALIDAR antes de usar .toFixed()
        if (processingTime !== undefined && processingTime !== null) {
          console.log(`  - Tiempo: ${processingTime.toFixed(2)}s`);
        } else {
          console.log(`  - Tiempo: No disponible`);
        }

        if (data.consolidation_details) {
          const details = data.consolidation_details;
          console.log(`  - Archivos encontrados: ${details.files_found || 0}`);
          console.log(`  - Exitosos: ${details.files_successfully_processed || 0}`);
          console.log(`  - Errores: ${details.files_with_errors || 0}`);
        }
      } else {
        console.warn('⚠️ Procesamiento completado con advertencias');
        if (data.errors && data.errors.length > 0) {
          console.warn('Errores:');
          data.errors.forEach(err => console.warn(`  - ${err}`));
        }
      }

      return data;
    } catch (error: any) {
      console.error('✗ Error completo capturado:', error);
      console.error('✗ Error.response:', error.response);
      console.error('✗ Error.response.data:', error.response?.data);
      console.error('✗ Error.message:', error.message);

      let errorMessage = 'Error desconocido al procesar archivos';

      if (error.response?.data?.detail) {
        errorMessage = error.response.data.detail;
        console.error('📌 Error extraído de response.data.detail:', errorMessage);
      }
      else if (typeof error.response?.data === 'string') {
        errorMessage = error.response.data;
        console.error('📌 Error extraído de response.data (string):', errorMessage);
      }
      else if (error.response?.data?.message) {
        errorMessage = error.response.data.message;
        console.error('📌 Error extraído de response.data.message:', errorMessage);
      }
      else if (error.message) {
        errorMessage = error.message;
        console.error('📌 Error extraído de error.message:', errorMessage);
      }
      else if (typeof error === 'string') {
        errorMessage = error;
        console.error('📌 Error como string:', errorMessage);
      }

      console.error('✗ Mensaje final de error:', errorMessage);
      throw new Error(errorMessage);
    }
  }

  static async getNTRPMSFileInfo(): Promise<NTRPMSFileInfo | null> {
    try {
      console.log('🔍 Verificando archivo NT RPMS consolidado...');

      const response = await api.get<NTRPMSFileInfo>(
        '/technical-note/nt-rpms/file-info',
        { timeout: 10000 }
      );

      if (response.data.is_available) {
        console.log('✓ Archivo NT RPMS disponible:');
        console.log(`  - ${response.data.total_rows.toLocaleString()} registros`);
        console.log(`  - ${response.data.total_columns} columnas`);
        return response.data;
      }

      console.log('⚠️ No hay archivo NT RPMS consolidado disponible');
      return null;
    } catch (error: any) {
      if (error.response?.status === 404) {
        console.log('⚠️ No hay archivo NT RPMS consolidado');
        return null;
      }

      console.error('Error verificando NT RPMS:', error);
      throw error;
    }
  }

  static async deleteNTRPMSFile(): Promise<{ success: boolean; message: string }> {
    try {
      console.log('🗑️ Eliminando archivo NT RPMS...');

      const response = await api.delete<{ success: boolean; message: string }>(
        '/technical-note/nt-rpms/delete',
        { timeout: 15000 }
      );

      if (response.data.success) {
        console.log('✓ Archivo NT RPMS eliminado exitosamente');
      }

      return response.data;
    } catch (error: any) {
      console.error('✗ Error eliminando NT RPMS:', error);
      throw new Error(
        error.response?.data?.detail || 'Error eliminando archivo NT RPMS'
      );
    }
  }

  // ========================================
  // CACHE MANAGEMENT METHODS
  // ========================================

  static async cleanupAllCache(): Promise<CleanupCacheResponse> {
    try {
      console.log('🧹 Limpiando cache del backend...');

      const response = await api.post<CleanupCacheResponse>(
        '/technical-note/cache/cleanup-all',
        {},
        { timeout: 30000 }
      );

      const data = response.data;

      if (data.success) {
        console.log('✓ Cache limpiado exitosamente:');
        console.log(`  - Tablas limpiadas: ${data.tables_cleared}`);
        console.log(`  - Archivos técnicos limpiados: ${data.technical_files_cleared}`);
      } else {
        console.warn('⚠️ Cache limpiado con errores:', data.errors);
      }

      return data;
    } catch (error) {
      console.error('✗ Error limpiando cache:', error);
      throw error;
    }
  }

  static async getCacheStatus(): Promise<CacheStatusResponse> {
    try {
      console.log('📊 Obteniendo estado del cache...');

      const response = await api.get<CacheStatusResponse>(
        '/technical-note/cache/status',
        { timeout: 10000 }
      );

      const data = response.data;

      console.log('Estado del cache:');
      Object.entries(data.directories).forEach(([dir, status]) => {
        console.log(`  ${dir}: ${status.file_count} archivos (${status.size_mb.toFixed(2)} MB)`);
      });
      console.log(`  Tablas en memoria: ${data.memory_state.loaded_tables_count}`);
      console.log(`  Archivos técnicos: ${data.memory_state.loaded_technical_files_count}`);

      return data;
    } catch (error) {
      console.error('✗ Error obteniendo estado del cache:', error);
      throw error;
    }
  }

  // ========================================
  // FILE MANAGEMENT METHODS
  // ========================================

  static async getAvailableFiles(): Promise<TechnicalFileInfo[]> {
    try {
      console.log('📂 GET /technical-note/available');

      const response = await api.get<TechnicalFileInfo[]>(
        '/technical-note/available',
        { timeout: 10000 }
      );

      console.log(`✓ ${response.data?.length || 0} archivos disponibles`);
      return response.data;
    } catch (error) {
      console.error('✗ Error obteniendo archivos:', error);
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

      console.log(`📄 GET /technical-note/data/${filename}?${params}`);

      const response = await api.get<TechnicalFileData>(
        `/technical-note/data/${filename}?${params}`,
        { timeout: 45000 }
      );

      console.log('✓ Datos obtenidos:', {
        status: response.status,
        rowsInPage: response.data?.pagination?.rows_in_page,
        totalFiltered: response.data?.pagination?.total_rows
      });

      return response.data;
    } catch (error) {
      console.error('✗ Error obteniendo datos:', error);
      throw error;
    }
  }

  static async getFileMetadata(filename: string): Promise<TechnicalFileMetadata> {
    try {
      console.log(`📋 Obteniendo metadatos: ${filename}`);

      const response = await api.get<TechnicalFileMetadata>(
        `/technical-note/metadata/${filename}`,
        { timeout: 15000 }
      );

      console.log(`✓ Metadatos obtenidos: ${response.data.total_rows?.toLocaleString()} filas`);

      return response.data;
    } catch (error) {
      console.error('✗ Error obteniendo metadatos:', error);
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
      console.log(`📊 Obteniendo columnas: ${filename}`);

      const response = await api.get(
        `/technical-note/columns/${filename}`,
        { timeout: 10000 }
      );

      return response.data;
    } catch (error) {
      console.error('✗ Error obteniendo columnas:', error);
      throw error;
    }
  }

  // ========================================
  // COLUMN VALUES METHODS
  // ========================================

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

      console.log(`🔍 Obteniendo valores únicos: ${filename} - ${columnName}`);

      const response = await api.get<ColumnUniqueValues>(
        `/technical-note/unique-values/${filename}/${columnName}?${params}`,
        { timeout: 15000 }
      );

      console.log(`✓ ${response.data.total_unique} valores únicos para ${columnName}`);

      return response.data;
    } catch (error) {
      console.error('✗ Error obteniendo valores únicos:', error);
      throw error;
    }
  }

  // ========================================
  // GEOGRAPHIC METHODS
  // ========================================

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

      const url = `/technical-note/geographic/${filename}/${geoType}${params.toString() ? `?${params}` : ''
        }`;

      console.log(`🗺️ Obteniendo ${geoType}: GET ${url}`);

      const response = await api.get<GeographicValuesResponse>(url, { timeout: 15000 });

      console.log(`✓ ${geoType} obtenidos: ${response.data?.values?.length || 0} valores`);

      return response.data;
    } catch (error) {
      console.error(`✗ Error obteniendo ${geoType}:`, error);
      throw error;
    }
  }

  static async getDepartamentos(filename: string): Promise<string[]> {
    try {
      const result = await this.getGeographicValues(filename, 'departamentos');
      return result.success ? result.values : [];
    } catch (error) {
      console.error('✗ Error obteniendo departamentos:', error);
      return [];
    }
  }

  static async getMunicipios(filename: string, departamento: string): Promise<string[]> {
    try {
      const result = await this.getGeographicValues(filename, 'municipios', { departamento });
      return result.success ? result.values : [];
    } catch (error) {
      console.error('✗ Error obteniendo municipios:', error);
      return [];
    }
  }

  static async getIps(filename: string, departamento: string, municipio: string): Promise<string[]> {
    try {
      const result = await this.getGeographicValues(filename, 'ips', {
        departamento,
        municipio
      });
      return result.success ? result.values : [];
    } catch (error) {
      console.error('✗ Error obteniendo IPS:', error);
      return [];
    }
  }

  // ========================================
  // REPORT METHODS
  // ========================================

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

      console.log(`📊 Generando reporte con fecha: ${cutoffDate}`);

      const response = await api.get<KeywordAgeReport>(
        `/technical-note/report/${filename}?${params}`,
        { timeout: 45000 }
      );

      return response.data;
    } catch (error) {
      console.error('✗ Error obteniendo reporte:', error);
      throw error;
    }
  }

  // ========================================
  // AGE METHODS
  // ========================================

  static async getAgeRanges(
    filename: string,
    cutoffDate: string
  ): Promise<AgeRangesResponse> {
    if (!cutoffDate) {
      throw new Error('Fecha de corte es obligatoria');
    }

    try {
      console.log(`👶 Obteniendo rangos de edades: ${filename} con corte ${cutoffDate}`);

      const params = new URLSearchParams({
        corte_fecha: cutoffDate
      });

      const response = await api.get<AgeRangesResponse>(
        `/technical-note/age-ranges/${filename}?${params}`,
        { timeout: 30000 }
      );

      const yearsCount = response.data.age_ranges?.years?.length || 0;
      const monthsCount = response.data.age_ranges?.months?.length || 0;

      console.log(`✓ Rangos obtenidos: ${yearsCount} años, ${monthsCount} meses (corte: ${cutoffDate})`);

      return response.data;
    } catch (error) {
      console.error('✗ Error obteniendo rangos:', error);
      throw error;
    }
  }

  // ========================================
  // ABSENT USERS METHODS
  // ========================================

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

      console.log(`📋 Generando reporte inasistentes (fecha: ${cutoffDate})...`);

      const response = await api.post<InasistentesReportResponse>(
        `/technical-note/inasistentes-report/${filename}?${params}`,
        requestBody,
        { timeout: 60000 }
      );

      const data = response.data;
      const totalInasistentes = data.resumen_general?.total_inasistentes_global || 0;
      const totalActividades = data.resumen_general?.total_actividades_evaluadas || 0;

      console.log('✓ Reporte obtenido:');
      console.log(`  - Inasistentes: ${totalInasistentes}`);
      console.log(`  - Actividades: ${totalActividades}`);
      console.log(`  - Fecha: ${cutoffDate}`);

      return data;
    } catch (error) {
      console.error('✗ Error obteniendo reporte inasistentes:', error);
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
      console.log(`📥 Exportando CSV: ${filename} (fecha: ${cutoffDate})`);

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

      console.log(`✓ CSV exportado exitosamente (fecha: ${cutoffDate})`);
      return response.data;
    } catch (error) {
      console.error('✗ Error exportando CSV:', error);
      throw error;
    }
  }

  // ========================================
  // UTILITY METHODS
  // ========================================

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
      console.log(`✓ Archivo descargado: ${filename}`);
    } catch (error) {
      console.error('✗ Error descargando:', error);
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
      console.error('✗ Error descargando desde enlace:', error);
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

// ========================================
// EXPORT HELPERS
// ========================================

export const TechnicalNoteHelpers = {
  formatFileSize: TechnicalNoteService.formatFileSize,
  isLargeFile: TechnicalNoteService.isLargeFile,
  getRecommendedPageSize: TechnicalNoteService.getRecommendedPageSize,
  calculateTotalPages: TechnicalNoteService.calculateTotalPages,
  downloadBlobAsFile: TechnicalNoteService.downloadBlobAsFile,
  downloadFromLink: TechnicalNoteService.downloadFromLink,
  getSemaforoColor: TechnicalNoteService.getSemaforoColor,
  cleanupCache: TechnicalNoteService.cleanupAllCache,
  getCacheStatus: TechnicalNoteService.getCacheStatus,
  processNTRPMS: TechnicalNoteService.processNTRPMSFolder,
  getNTRPMSInfo: TechnicalNoteService.getNTRPMSFileInfo,
  deleteNTRPMS: TechnicalNoteService.deleteNTRPMSFile
};

export default TechnicalNoteService;
