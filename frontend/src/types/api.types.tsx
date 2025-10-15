// src/types/api.types.ts

// src/types/api.types.ts
import type { ReactNode } from 'react';

// Tipos base
export interface FileInfo {
  file_id: string;
  original_name: string;
  columns: string[];
  sheets?: string[];
  total_rows: number;
  file_extension?: string;
}

export interface FilterCondition {
  column: string;
  operator: 'equals' | 'contains' | 'starts_with' | 'ends_with' | 'gt' | 'lt' | 'gte' | 'lte' | 'in' | 'not_in' | 'is_null' | 'is_not_null';
  value?: any;
  values?: string[];
}

export interface SortCondition {
  column: string;
  direction: 'asc' | 'desc';
}

export interface PaginatedResponse<T> {
  data: T[];
  total: number;
  page: number;
  page_size: number;
  total_pages: number;
  has_next: boolean;
  has_previous: boolean;
}

// ✅ Nuevas interfaces para el sistema de tabs extensible
export interface FileData {
  file_id: string;
  original_name: string;
  columns: string[];
  sheets?: string[];
  total_rows: number;
}

export type TabKey = 'welcome' | 'upload' | 'transform' | 'chat' | 'export' | 'cross' | 'technical_note';

export interface TabProps {
  fileData?: FileData | null;  
  isMobile: boolean;
  isTablet: boolean;  
  onTabChange: (tab: string) => void;
  onOpenCrossModal?: () => void;
  crossResult?: any;
  crossTableState?: CrossTableState;
  processedCrossData?: any[];
  crossDataTotal?: number;
  onCrossPaginationChange?: (page: number, size: number) => void;
  onCrossFiltersChange?: (filters: FilterCondition[]) => void;
  onCrossSortChange?: (sorting: SortCondition[]) => void;
  onCrossSearch?: (searchTerm: string) => void;
  onExportCrossResult?: (format: 'csv' | 'xlsx') => void;
  onClearCrossResult?: () => void;
  // Cualquier prop adicional que los tabs necesiten
  [key: string]: any;
}

export interface TabConfig {
  key: string;
  label: string;
  icon: ReactNode;
  component: React.ComponentType<TabProps>;
  requiresFile?: boolean;
}

// ✅ Estados de UI para el componente principal
export interface UIState {
  activeTab: TabKey;
  collapsed: boolean;
  currentPage: number;
  pageSize: number;
  filters: FilterCondition[];
  sorting: SortCondition[];
  searchTerm: string;
  chatDrawerVisible: boolean;
  transformModalVisible: boolean;
  selectedTransform: string;
  mobileMenuVisible: boolean;
  crossModalVisible: boolean;
}

// ✅ Estado específico para la tabla del cruce
export interface CrossTableState {
  currentPage: number;
  pageSize: number;
  filters: FilterCondition[];
  sorting: SortCondition[];
  searchTerm: string;
}

// Request types
export interface DataRequest {
  file_id: string;
  sheet_name?: string;
  filters?: FilterCondition[];
  sort?: SortCondition[];
  page: number;
  page_size: number;
  search?: string;
}

export interface TransformRequest {
  file_id: string;
  operation: 'concatenate' | 'split_column' | 'replace_values' | 'create_calculated' | 'rename_column' | 'delete_column' | 'fill_null' | 'to_uppercase' | 'to_lowercase' | 'extract_substring';
  params: Record<string, any>;
}

export interface ExportRequest {
  file_id: string;
  sheet_name?: string;
  filters?: FilterCondition[];
  sort?: SortCondition[];
  search?: string;
  format: 'csv' | 'excel' | 'json';
  filename?: string;
  include_headers: boolean;
  selected_columns?: string[];
}

export interface DeleteRowsRequest {
  file_id: string;
  sheet_name?: string;
  row_indices: number[];
}

export interface DeleteRowsByFilterRequest {
  file_id: string;
  sheet_name?: string;
  filters: FilterCondition[];
}

export interface AIRequest {
  question: string;
  file_context?: string;
}

// Response types
export interface FileUploadResponse extends FileInfo {
  message: string;
  filename: string;
  id: string;
}

export interface TransformResponse {
  message: string;
  new_columns: string[];
  total_rows: number;
}

export interface ExportResponse {
  message: string;
  filename: string;
  file_path: string;
  rows_exported: number;
  format: string;
}

export interface DeleteResponse {
  message: string;
  rows_deleted: number;
  remaining_rows: number;
}

export interface AIResponse {
  response: string;
}

// Cruce - tipos para funcionalidad de cruce de archivos
export interface CrossRequest {
  file1_key: string;
  file2_key: string;
  file1_sheet?: string;
  file2_sheet?: string;
  key_column_file1: string;
  key_column_file2: string;
  cross_type: 'inner' | 'left' | 'right' | 'outer';
  columns_to_include?: {
    file1_columns: string[];
    file2_columns: string[];
  };
}

export interface CrossPreviewRequest extends CrossRequest {
  limit?: number;
}

export interface CrossResult {
  data: any[];
  columns: string[];
  total_rows: number;
  file1_matched: number;
  file2_matched: number;
  cross_type: string;
}

export interface CrossPreviewResult extends CrossResult {
  preview: boolean;
  sample_size: number;
  file1_rows_sampled: number;
  file2_rows_sampled: number;
  result_rows: number;
  sample_data: any[];
}

// ✅ Utilidades de validación de tipos
export const isValidCurrentData = (d: any): d is { data: any[] } =>
  d && typeof d === 'object' && Array.isArray(d.data);

// ✅ Tipos auxiliares para extensibilidad
export type TabComponentType = React.ComponentType<TabProps>;
export type TabRegistryType = Record<string, TabConfig>;

// ✅ Props para componentes específicos
export interface WelcomeComponentProps {
  isMobile: boolean;
}

export interface TabRendererProps extends TabProps {
  activeTab: string;
}

export interface ErrorBoundaryState {
  hasError: boolean;
  error: Error | null;
}

// ✅ Props para hooks personalizados
export interface UseFileOperationsReturn {
  files: FileData[] | null;
  currentFile: FileData | null;
  currentData: PaginatedResponse<Record<string, any>> | null;  // ✅ Cambio aquí
  loading: boolean;
  error: string | null;
  setCurrentFile: (file: FileData | null) => void;
  setError: (error: string | null) => void;
  loadFiles: () => Promise<void>;
  loadFileData: (request: DataRequest) => Promise<PaginatedResponse<Record<string, any>>>;
  deleteFile: (fileId: string) => Promise<void>;
  handleUploadSuccess: (res: any) => Promise<void>;
  handleDeleteRows: (indices: number[]) => Promise<void>;
  handleExport: (format: 'csv' | 'excel' | 'json') => Promise<void>;
  handleFileUploadedFromTransform: (fileInfo: FileInfo) => Promise<void>;
}

export interface UseCrossDataReturn {
  crossResult: any;
  crossTableState: CrossTableState;
  processedCrossData: any[];
  crossDataTotal: number;
  handleCrossPaginationChange: (page: number, size: number) => void;
  handleCrossFiltersChange: (filters: FilterCondition[]) => void;
  handleCrossSortChange: (sorting: SortCondition[]) => void;
  handleCrossSearch: (searchTerm: string) => void;
  handleCrossComplete: (result: any) => Promise<void>;
  handleExportCrossResult: (format: 'csv' | 'xlsx') => Promise<void>;
  handleClearCrossResult: () => Promise<void>;
}
