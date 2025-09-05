// services/TechnicalNoteService.tsx
import api from '../Api';
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

// ✅ NUEVA INTERFAZ PARA VALORES ÚNICOS ESTILO EXCEL
export interface ColumnUniqueValues {
  filename: string;
  column_name: string;
  unique_values: string[];
  total_unique: number;
  limited: boolean;
  limit_applied: number;
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

  // ✅ NUEVO: Obtener valores únicos de una columna (estilo Excel)
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
}

export const TechnicalNoteHelpers = {
  formatFileSize: TechnicalNoteService.formatFileSize,
  isLargeFile: TechnicalNoteService.isLargeFile,
  getRecommendedPageSize: TechnicalNoteService.getRecommendedPageSize,
  calculateTotalPages: TechnicalNoteService.calculateTotalPages
};
