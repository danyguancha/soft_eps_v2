// services/TechnicalNoteService.tsx
import api from '../Api';
import type { InasistentesReportResponse } from '../interfaces/IAbsentUser';
import type { AgeRangesResponse } from '../interfaces/IAge';
import type {
  ColumnUniqueValues,
  GeographicFilters,
  GeographicValuesResponse,
  KeywordAgeReport,
  NTRPMSProcessResponse,
  TechnicalFileData,
  TechnicalFileInfo,
  TechnicalFileMetadata
} from '../interfaces/ITechnicalNote';
import type { FilterCondition } from '../types/api.types';


export class TechnicalNoteService {

  // ========================================
  // NT RPMS METHODS
  // ========================================

  static async processNTRPMSFromNetwork(networkPath: string): Promise<NTRPMSProcessResponse> {
    try {
      const response = await api.post<NTRPMSProcessResponse>(
        '/technical-note/nt-rpms/process-network',
        { network_path: networkPath },
        { timeout: 300000 }
      );
      const data = response.data;
      if (!data) throw new Error('No se recibió respuesta del servidor');
      if (data.success) this._logProcessingResult(data);
      return data;
    } catch (error: any) {
      return this._handleProcessingError(error, 'desde red');
    }
  }

  static async processNTRPMSFromLocal(folderPath: string): Promise<NTRPMSProcessResponse> {
    try {
      const response = await api.post<NTRPMSProcessResponse>(
        '/technical-note/nt-rpms/process-local',
        { folder_path: folderPath },
        { timeout: 300000 }
      );
      const data = response.data;
      if (!data) throw new Error('No se recibió respuesta del servidor');
      if (data.success) this._logProcessingResult(data);
      return data;
    } catch (error: any) {
      return this._handleProcessingError(error, 'local');
    }
  }

  /** @deprecated Usar processNTRPMSFromNetwork o processNTRPMSFromLocal */
  static async processNTRPMSFolder(folderPath: string): Promise<NTRPMSProcessResponse> {
    console.warn('⚠️ processNTRPMSFolder está deprecado.');
    return this.isNetworkPath(folderPath)
      ? this.processNTRPMSFromNetwork(folderPath)
      : this.processNTRPMSFromLocal(folderPath);
  }

  static async listProcessedNTRPMS(): Promise<{
    success: boolean;
    files: Array<{
      filename: string;
      csv_path: string;
      parquet_path: string | null;
      has_parquet: boolean;
      size_mb: number;
      created: string;
      modified: string;
    }>;
    count: number;
  }> {
    try {
      const response = await api.get('/technical-note/nt-rpms/list-processed', { timeout: 10000 });
      return response.data;
    } catch (error: any) {
      console.error('✗ Error listando archivos NT RPMS:', error);
      throw error;
    }
  }

  // ─── Helpers privados NT RPMS ────────────────────────────────────────────

  private static _logProcessingResult(data: NTRPMSProcessResponse): void {
    for (const key of ['rpms', 'rmpn'] as const) {
      const sheet = (data as any)[key];
      if (!sheet) continue;
      const label = sheet.label ?? key.toUpperCase();
      if (sheet.success) {
        console.log(`✓ [${label}] ${sheet.total_rows?.toLocaleString()} filas | CSV: ${sheet.csv_path ?? 'N/A'} | Parquet: ${sheet.parquet_path ?? 'N/A'}`);
        if (sheet.timing) {
          console.log(`  Tiempo total: ${sheet.timing.total_time?.toFixed(2)}s`);
        }
        if (sheet.compression_info) {
          console.log(`  Compresión: ${sheet.compression_info.original_size_mb?.toFixed(2)} MB → ${sheet.compression_info.parquet_size_mb?.toFixed(2)} MB`);
        }
        if (sheet.extraction_summary?.errores?.length) {
          sheet.extraction_summary.errores.forEach(([file, err]: [string, string]) =>
            console.warn(`  ⚠ ${file}: ${err}`)
          );
        }
      } else {
        console.warn(`✗ [${label}] Error: ${sheet.error ?? 'desconocido'}`);
      }
    }
    if (data.total_time) {
      console.log(`⏱ Tiempo total procesamiento: ${data.total_time}`);
    }
  }

  private static _handleProcessingError(error: any, source: string): never {
    const detail = error.response?.data?.detail;
    let errorMessage: string;
    let suggestion: string | undefined;

    if (detail && typeof detail === 'object' && detail.error) {
      errorMessage = detail.error;
      suggestion   = detail.suggestion;
    } else if (typeof detail === 'string') {
      errorMessage = detail;
    } else if (error.response?.data?.message) {
      errorMessage = error.response.data.message;
    } else {
      errorMessage = error.message ?? `Error desconocido al procesar archivos ${source}`;
    }

    console.error(`✗ Error procesando ${source}:`, errorMessage);
    if (suggestion) console.error('💡 Sugerencia:', suggestion);

    throw new Error(suggestion ? `${errorMessage}\n\n💡 Sugerencia:\n${suggestion}` : errorMessage);
  }


  // ========================================
  // FILE MANAGEMENT METHODS
  // ========================================

  static async getAvailableFiles(): Promise<TechnicalFileInfo[]> {
    try {
      const response = await api.get<TechnicalFileInfo[]>('/technical-note/available', { timeout: 10000 });
      return response.data;
    } catch (error) {
      console.error('✗ Error obteniendo archivos disponibles:', error);
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
        ...(sheetName                        && { sheet_name: sheetName }),
        ...(search                           && { search: search.trim() }),
        ...(sortBy                           && { sort_by: sortBy }),
        ...(sortOrder                        && { sort_order: sortOrder }),
        ...(filters?.length                  && { filters: JSON.stringify(filters) })
      });

      const response = await api.get<TechnicalFileData>(
        `/technical-note/data/${filename}?${params}`,
        { timeout: 45000 }
      );
      return response.data;
    } catch (error) {
      console.error('✗ Error obteniendo datos:', error);
      throw error;
    }
  }

  static async getFileMetadata(filename: string): Promise<TechnicalFileMetadata> {
    try {
      const response = await api.get<TechnicalFileMetadata>(
        `/technical-note/metadata/${filename}`,
        { timeout: 15000 }
      );
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
      const response = await api.get(`/technical-note/columns/${filename}`, { timeout: 10000 });
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
      const response = await api.get<ColumnUniqueValues>(
        `/technical-note/unique-values/${filename}/${columnName}?${params}`,
        { timeout: 15000 }
      );
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
      if (filters.departamento) params.append('departamento', filters.departamento);
      if (filters.municipio)    params.append('municipio',    filters.municipio);

      const url = `/technical-note/geographic/${filename}/${geoType}${params.toString() ? `?${params}` : ''}`;
      const response = await api.get<GeographicValuesResponse>(url, { timeout: 15000 });
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
    } catch {
      return [];
    }
  }

  static async getMunicipios(filename: string, departamento: string): Promise<string[]> {
    try {
      const result = await this.getGeographicValues(filename, 'municipios', { departamento });
      return result.success ? result.values : [];
    } catch {
      return [];
    }
  }

  static async getIps(filename: string, departamento: string, municipio: string): Promise<string[]> {
    try {
      const result = await this.getGeographicValues(filename, 'ips', { departamento, municipio });
      return result.success ? result.values : [];
    } catch {
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
    geographicFilters: GeographicFilters = {},
    regimen?: 'Subsidiado' | 'Contributivo'
  ): Promise<KeywordAgeReport> {
    if (!cutoffDate) throw new Error('Fecha de corte es obligatoria');

    try {
      const params = new URLSearchParams({
        corte_fecha:      cutoffDate,
        min_count:        minCount.toString(),
        include_temporal: includeTemporal.toString(),
        ...(keywords?.length && { keywords: keywords.join(',') })
      });

      if (geographicFilters.departamento) params.append('departamento', geographicFilters.departamento);
      if (geographicFilters.municipio)    params.append('municipio',    geographicFilters.municipio);
      if (geographicFilters.ips)          params.append('ips',          geographicFilters.ips);
      if (regimen)                        params.append('regimen',      regimen);

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

  static async getAgeRanges(filename: string, cutoffDate: string): Promise<AgeRangesResponse> {
    if (!cutoffDate) throw new Error('Fecha de corte es obligatoria');

    try {
      const params = new URLSearchParams({ corte_fecha: cutoffDate });
      const response = await api.get<AgeRangesResponse>(
        `/technical-note/age-ranges/${filename}?${params}`,
        { timeout: 30000 }
      );
      return response.data;
    } catch (error) {
      console.error('✗ Error obteniendo rangos de edad:', error);
      throw error;
    }
  }


  // ========================================
  // ABSENT USERS METHODS
  // ========================================

  static async getInasistentesReport(
    filename: string,
    cutoffDate: string,
    keywords: string[] = ['medicina'],
    geographicFilters: GeographicFilters = {},
    regimen?: 'Subsidiado' | 'Contributivo'
  ): Promise<InasistentesReportResponse> {
    if (!cutoffDate) throw new Error('Fecha de corte es obligatoria');

    try {
      const body: Record<string, any> = { selectedKeywords: keywords };
      if (geographicFilters.departamento) body.departamento = geographicFilters.departamento;
      if (geographicFilters.municipio)    body.municipio    = geographicFilters.municipio;
      if (geographicFilters.ips)          body.ips          = geographicFilters.ips;

      const params = new URLSearchParams({ corte_fecha: cutoffDate });
      if (regimen) params.append('regimen', regimen);

      const response = await api.post<InasistentesReportResponse>(
        `/technical-note/inasistentes-report/${filename}?${params}`,
        body,
        { timeout: 60000 }
      );
      return response.data;
    } catch (error: any) {
      console.error('✗ Error generando reporte de inasistentes:', error.response?.data ?? error);
      throw error;
    }
  }

  static async exportInasistentesCSV(
    filename: string,
    cutoffDate: string,
    keywords: string[] = ['medicina'],
    geographicFilters: GeographicFilters = {},
    regimen?: 'Subsidiado' | 'Contributivo'
  ): Promise<Blob> {
    if (!cutoffDate) throw new Error('Fecha de corte es obligatoria');

    try {
      const body: Record<string, any> = { selectedKeywords: keywords };
      if (geographicFilters.departamento) body.departamento = geographicFilters.departamento;
      if (geographicFilters.municipio)    body.municipio    = geographicFilters.municipio;
      if (geographicFilters.ips)          body.ips          = geographicFilters.ips;

      const params = new URLSearchParams({ corte_fecha: cutoffDate });
      if (regimen) params.append('regimen', regimen);

      const response = await api.post(
        `/technical-note/inasistentes-report/${filename}/export-csv?${params}`,
        body,
        { timeout: 120000, responseType: 'blob', headers: { Accept: 'text/csv; charset=utf-8' } }
      );
      return response.data;
    } catch (error: any) {
      console.error('✗ Error exportando CSV de inasistentes:', error.response?.data ?? error);
      throw error;
    }
  }


  // ========================================
  // EXPORT / DOWNLOAD METHODS
  // ========================================

  static async exportCurrentReport(
    reportData: any,
    filename: string = 'reporte',
    exportOptions: { export_csv?: boolean; export_pdf?: boolean; include_detailed?: boolean } = {}
  ): Promise<any> {
    try {
      const response = await api.post(
        '/technical-note/reports/export-current',
        { report_data: reportData, filename, export_options: exportOptions },
        { timeout: 60000 }
      );
      return response.data;
    } catch (error) {
      console.error('✗ Error exportando reporte:', error);
      throw error;
    }
  }

  static async downloadReportFile(fileId: string): Promise<Blob> {
    try {
      const response = await api.get(
        `/technical-note/reports/download/${fileId}`,
        { responseType: 'blob', timeout: 60000 }
      );
      return response.data;
    } catch (error) {
      console.error('✗ Error descargando archivo:', error);
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
    return Math.round((bytes / Math.pow(1024, i)) * 100) / 100 + ' ' + sizes[i];
  }

  static isLargeFile(totalRows: number): boolean { return totalRows > 10000; }

  static getRecommendedPageSize(totalRows: number): number {
    if (totalRows <= 1000)  return 100;
    if (totalRows <= 10000) return 500;
    if (totalRows <= 50000) return 1000;
    return 1500;
  }

  static calculateTotalPages(totalRows: number, pageSize: number): number {
    return Math.ceil(totalRows / pageSize);
  }

  static downloadBlobAsFile(blob: Blob, filename: string): void {
    const url  = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    window.URL.revokeObjectURL(url);
  }

  static async downloadFromLink(downloadLink: string, filename?: string): Promise<void> {
    try {
      const response = await api.get(downloadLink, { responseType: 'blob', timeout: 60000 });
      const cd = response.headers['content-disposition'];
      const match = cd?.match(/filename[^;=\n]*=(['"]?)([^'"\n]*?)\1/);
      const finalName = filename ?? match?.[2] ?? `archivo_${Date.now()}`;
      this.downloadBlobAsFile(response.data, finalName);
    } catch (error) {
      console.error('✗ Error descargando desde enlace:', error);
      throw error;
    }
  }

  static getSemaforoColor(estado: string): string {
    const colores: Record<string, string> = {
      'Óptimo':        '#4CAF50',
      'Aceptable':     '#FF9800',
      'Deficiente':    '#FF5722',
      'Muy Deficiente':'#F44336',
      'NA':            '#9E9E9E'
    };
    return colores[estado] ?? '#9E9E9E';
  }

  static isNetworkPath(path: string): boolean {
    return path.startsWith('\\\\') || path.startsWith('//');
  }

  static normalizeNetworkPath(path: string): string {
    let normalized = path.replace(/\//g, '\\');
    if (!normalized.startsWith('\\\\')) {
      normalized = (normalized.startsWith('\\') ? '\\' : '\\\\') + normalized;
    }
    return normalized.trim();
  }
}


// ========================================
// HELPERS EXPORT
// ========================================
export const TechnicalNoteHelpers = {
  formatFileSize:          TechnicalNoteService.formatFileSize.bind(TechnicalNoteService),
  isLargeFile:             TechnicalNoteService.isLargeFile.bind(TechnicalNoteService),
  getRecommendedPageSize:  TechnicalNoteService.getRecommendedPageSize.bind(TechnicalNoteService),
  calculateTotalPages:     TechnicalNoteService.calculateTotalPages.bind(TechnicalNoteService),
  downloadBlobAsFile:      TechnicalNoteService.downloadBlobAsFile.bind(TechnicalNoteService),
  downloadFromLink:        TechnicalNoteService.downloadFromLink.bind(TechnicalNoteService),
  getSemaforoColor:        TechnicalNoteService.getSemaforoColor.bind(TechnicalNoteService),
  processNTRPMSFromNetwork:TechnicalNoteService.processNTRPMSFromNetwork.bind(TechnicalNoteService),
  processNTRPMSFromLocal:  TechnicalNoteService.processNTRPMSFromLocal.bind(TechnicalNoteService),
  processNTRPMS:           TechnicalNoteService.processNTRPMSFolder.bind(TechnicalNoteService),
  listProcessedNTRPMS:     TechnicalNoteService.listProcessedNTRPMS.bind(TechnicalNoteService),
  isNetworkPath:           TechnicalNoteService.isNetworkPath.bind(TechnicalNoteService),
  normalizeNetworkPath:    TechnicalNoteService.normalizeNetworkPath.bind(TechnicalNoteService),
};

export default TechnicalNoteService;