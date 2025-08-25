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
  values?: any[];
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
