// services/CrossService.ts
import api from '../Api';

export interface FileCrossRequest {
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

export interface CrossPreviewRequest extends FileCrossRequest {
  limit?: number;
}

export class CrossService {
  
  static async crossFiles(request: FileCrossRequest): Promise<any> {
    const response = await api.post('/cross', request);
    return response.data;
  }

  static async getFileColumnsForCross(fileId: string, sheetName?: string): Promise<any> {
    const params = sheetName ? `?sheet_name=${sheetName}` : '';
    const response = await api.get(`/cross/columns/${fileId}${params}`);
    return response.data;
  }
}
