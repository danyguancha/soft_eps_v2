// services/TechnicalNoteService.tsx - CÓDIGO COMPLETO CON SOPORTE PARA RED
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
  // NT RPMS METHODS - ACTUALIZADOS PARA RED
  // ========================================

  /**
   * Procesa archivos NT RPMS desde una carpeta compartida en red
   * @param networkPath - Ruta UNC (ej: \\192.168.1.100\NT_RPMS_Share)
   * @returns Respuesta con información del procesamiento
   */
  static async processNTRPMSFromNetwork(networkPath: string): Promise<NTRPMSProcessResponse> {
    try {
      console.log('='.repeat(60));
      console.log('PROCESANDO NT RPMS DESDE RED');
      console.log('='.repeat(60));
      console.log(`🌐 Ruta de red: ${networkPath}`);

      const requestBody = {
        network_path: networkPath
      };

      const response = await api.post<NTRPMSProcessResponse>(
        '/technical-note/nt-rpms/process-network',
        requestBody,
        {
          timeout: 300000, // 5 minutos
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
        this._logSuccessfulProcessing(data);
      } else {
        console.warn('⚠️ Procesamiento completado con advertencias');
        if (data.errors && data.errors.length > 0) {
          console.warn('Errores:');
          data.errors.forEach(err => console.warn(`  - ${err}`));
        }
      }

      return data;
    } catch (error: any) {
      return this._handleProcessingError(error, 'desde red');
    }
  }

  /**
   * Procesa archivos NT RPMS desde una carpeta local del servidor
   * @param folderPath - Ruta local en el servidor
   * @returns Respuesta con información del procesamiento
   */
  static async processNTRPMSFromLocal(folderPath: string): Promise<NTRPMSProcessResponse> {
    try {
      console.log('='.repeat(60));
      console.log('PROCESANDO NT RPMS LOCAL');
      console.log('='.repeat(60));
      console.log(`📁 Carpeta local: ${folderPath}`);

      const requestBody = {
        folder_path: folderPath
      };

      const response = await api.post<NTRPMSProcessResponse>(
        '/technical-note/nt-rpms/process-local',
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
        this._logSuccessfulProcessing(data);
      } else {
        console.warn('⚠️ Procesamiento completado con advertencias');
        if (data.errors && data.errors.length > 0) {
          console.warn('Errores:');
          data.errors.forEach(err => console.warn(`  - ${err}`));
        }
      }

      return data;
    } catch (error: any) {
      return this._handleProcessingError(error, 'local');
    }
  }

  /**
   * [DEPRECATED] Usar processNTRPMSFromNetwork o processNTRPMSFromLocal
   */
  static async processNTRPMSFolder(folderPath: string): Promise<NTRPMSProcessResponse> {
    console.warn('⚠️ processNTRPMSFolder está deprecado. Usa processNTRPMSFromNetwork o processNTRPMSFromLocal');
    
    // Detectar si es ruta de red o local
    if (folderPath.startsWith('\\\\') || folderPath.startsWith('//')) {
      return this.processNTRPMSFromNetwork(folderPath);
    } else {
      return this.processNTRPMSFromLocal(folderPath);
    }
  }

  /**
   * Método auxiliar para loggear procesamiento exitoso
   */
  private static _logSuccessfulProcessing(data: NTRPMSProcessResponse): void {
    const filesProcessed = data.extraction_summary?.archivos_procesados || 0;
    const totalRows = data.total_rows || 0;
    const totalColumns = data.total_columns || 0;
    const timing = data.timing;

    console.log('✓ Procesamiento exitoso:');
    console.log(`  - Archivos procesados: ${filesProcessed}`);
    console.log(`  - Registros totales: ${totalRows.toLocaleString()}`);
    console.log(`  - Columnas: ${totalColumns}`);
    console.log(`  - CSV: ${data.csv_path || 'N/A'}`);
    console.log(`  - Parquet: ${data.parquet_path || 'N/A'}`);

    if (timing) {
      console.log(`  - Tiempo extracción: ${timing.extraction_time?.toFixed(2)}s`);
      console.log(`  - Tiempo conversión: ${timing.conversion_time?.toFixed(2)}s`);
      console.log(`  - Tiempo total: ${timing.total_time?.toFixed(2)}s`);
    }

    // Info de red si está disponible
    if (data.network_info) {
      console.log(`  - Carpeta origen: ${data.network_info.original_path}`);
      console.log(`  - Tipo acceso: ${data.network_info.access_type}`);
      console.log(`  - Archivos Excel encontrados: ${data.network_info.excel_files_found}`);
    }

    // Info de compresión
    if (data.compression_info) {
      console.log(`  - Tamaño CSV: ${data.compression_info.original_size_mb?.toFixed(2)} MB`);
      console.log(`  - Tamaño Parquet: ${data.compression_info.parquet_size_mb?.toFixed(2)} MB`);
      console.log(`  - Ratio compresión: ${data.compression_info.compression_ratio?.toFixed(1)}%`);
    }

    if (data.extraction_summary) {
      const summary = data.extraction_summary;
      console.log(`  - Archivos con errores: ${summary.archivos_con_errores || 0}`);
      if (summary.errores && summary.errores.length > 0) {
        console.log('  Errores específicos:');
        summary.errores.forEach(([file, error]) => {
          console.log(`    • ${file}: ${error}`);
        });
      }
    }
  }

  /**
   * Método auxiliar para manejar errores de procesamiento
   */
  private static _handleProcessingError(error: any, source: string): never {
    console.error(`✗ Error completo capturado (${source}):`, error);
    console.error('✗ Error.response:', error.response);
    console.error('✗ Error.response.data:', error.response?.data);
    console.error('✗ Error.message:', error.message);

    let errorMessage = `Error desconocido al procesar archivos ${source}`;
    let suggestion: string | undefined;

    // Extraer mensaje de error y sugerencias
    if (error.response?.data?.detail) {
      const detail = error.response.data.detail;
      
      // Si detail es un objeto con error y suggestion
      if (typeof detail === 'object' && detail.error) {
        errorMessage = detail.error;
        suggestion = detail.suggestion;
        console.error('📌 Error:', errorMessage);
        if (suggestion) {
          console.error('💡 Sugerencia:', suggestion);
        }
      } 
      // Si detail es string directo
      else if (typeof detail === 'string') {
        errorMessage = detail;
        console.error('📌 Error extraído de response.data.detail:', errorMessage);
      }
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
    
    // Construir mensaje completo con sugerencia si existe
    const fullMessage = suggestion 
      ? `${errorMessage}\n\n💡 Sugerencia:\n${suggestion}`
      : errorMessage;
    
    throw new Error(fullMessage);
  }

  /**
   * Obtiene información del archivo NT RPMS consolidado disponible
   */
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

  /**
   * Elimina el archivo NT RPMS consolidado
   */
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

  /**
   * Obtiene lista de archivos NT RPMS procesados
   */
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
      console.log('📋 Listando archivos NT RPMS procesados...');

      const response = await api.get(
        '/technical-note/nt-rpms/list-processed',
        { timeout: 10000 }
      );

      console.log(`✓ ${response.data.count} archivos procesados encontrados`);

      return response.data;
    } catch (error: any) {
      console.error('✗ Error listando archivos NT RPMS:', error);
      throw error;
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

      const url = `/technical-note/geographic/${filename}/${geoType}${
        params.toString() ? `?${params}` : ''
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
    keywords: string[] = ['medicina'],
    geographicFilters: GeographicFilters = {}
  ): Promise<InasistentesReportResponse> {
    if (!cutoffDate) {
      throw new Error('Fecha de corte es obligatoria');
    }

    try {
      console.log('📋 Generando reporte de inasistentes...');
      console.log(`   - Archivo: ${filename}`);
      console.log(`   - Fecha corte: ${cutoffDate}`);
      console.log(`   - Keywords:`, keywords);
      console.log(`   - Filtros geográficos:`, geographicFilters);

      const requestBody: any = {
        selectedKeywords: keywords
      };

      if (geographicFilters.departamento) {
        requestBody.departamento = geographicFilters.departamento;
      }
      if (geographicFilters.municipio) {
        requestBody.municipio = geographicFilters.municipio;
      }
      if (geographicFilters.ips) {
        requestBody.ips = geographicFilters.ips;
      }

      const params = new URLSearchParams({
        corte_fecha: cutoffDate
      });

      console.log('📤 Request body:', JSON.stringify(requestBody, null, 2));

      const response = await api.post<InasistentesReportResponse>(
        `/technical-note/inasistentes-report/${filename}?${params}`,
        requestBody,
        { timeout: 60000 }
      );

      const data = response.data;

      if (data.success) {
        const totalInasistentes = data.resumen_general?.total_inasistentes_global || 0;
        const totalActividades = data.resumen_general?.total_actividades_evaluadas || 0;

        console.log('✓ Reporte generado exitosamente:');
        console.log(`  - Total inasistentes: ${totalInasistentes}`);
        console.log(`  - Total actividades: ${totalActividades}`);
        console.log(`  - Fecha corte: ${cutoffDate}`);
      } else {
        console.warn('⚠️ Reporte generado con advertencias');
      }

      return data;
    } catch (error: any) {
      console.error('✗ Error generando reporte de inasistentes:', error);
      console.error('✗ Error response:', error.response?.data);
      throw error;
    }
  }

  static async exportInasistentesCSV(
    filename: string,
    cutoffDate: string,
    keywords: string[] = ['medicina'],
    geographicFilters: GeographicFilters = {}
  ): Promise<Blob> {
    if (!cutoffDate) {
      throw new Error('Fecha de corte es obligatoria');
    }

    try {
      console.log('📥 Exportando CSV de inasistentes...');
      console.log(`   - Archivo: ${filename}`);
      console.log(`   - Fecha corte: ${cutoffDate}`);
      console.log(`   - Keywords:`, keywords);

      const requestBody: any = {
        selectedKeywords: keywords
      };

      if (geographicFilters.departamento) {
        requestBody.departamento = geographicFilters.departamento;
      }
      if (geographicFilters.municipio) {
        requestBody.municipio = geographicFilters.municipio;
      }
      if (geographicFilters.ips) {
        requestBody.ips = geographicFilters.ips;
      }

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

      console.log(`✓ CSV exportado exitosamente (${response.data.size} bytes)`);
      return response.data;
    } catch (error: any) {
      console.error('✗ Error exportando CSV:', error);
      console.error('✗ Error response:', error.response?.data);
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

  /**
   * Valida si una ruta es de red (UNC)
   */
  static isNetworkPath(path: string): boolean {
    return path.startsWith('\\\\') || path.startsWith('//');
  }

  /**
   * Normaliza una ruta de red para el formato correcto
   */
  static normalizeNetworkPath(path: string): string {
    // Convertir forward slashes a backslashes
    let normalized = path.replace(/\//g, '\\');
    
    // Asegurar que empiece con \\
    if (!normalized.startsWith('\\\\')) {
      if (normalized.startsWith('\\')) {
        normalized = '\\' + normalized;
      } else {
        normalized = '\\\\' + normalized;
      }
    }
    
    return normalized.trim();
  }
}

// Exportar helpers
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
  
  // Métodos NT RPMS actualizados
  processNTRPMSFromNetwork: TechnicalNoteService.processNTRPMSFromNetwork,
  processNTRPMSFromLocal: TechnicalNoteService.processNTRPMSFromLocal,
  processNTRPMS: TechnicalNoteService.processNTRPMSFolder, // Deprecado pero mantenido
  getNTRPMSInfo: TechnicalNoteService.getNTRPMSFileInfo,
  deleteNTRPMS: TechnicalNoteService.deleteNTRPMSFile,
  listProcessedNTRPMS: TechnicalNoteService.listProcessedNTRPMS,
  
  // Utilidades de red
  isNetworkPath: TechnicalNoteService.isNetworkPath,
  normalizeNetworkPath: TechnicalNoteService.normalizeNetworkPath
};

export default TechnicalNoteService;
