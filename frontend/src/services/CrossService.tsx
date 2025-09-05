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
    const response = await api.post('/cross', request, {
      timeout: 300000 // ✅ 5 minutos para archivos grandes
    });
    return response.data;
  }

  // ✅ NUEVO: Método para descarga de archivos grandes
  static async crossFilesDownload(request: FileCrossRequest): Promise<void> {
    try {
      console.log('🚀 Iniciando descarga de cruce para archivo grande...');
      
      const response = await api.post('/cross-download', request, {
        responseType: 'blob', // ✅ Importante para archivos
        timeout: 0, // ✅ Sin timeout para archivos grandes
        onDownloadProgress: (progressEvent) => {
          if (progressEvent.total) {
            const progress = Math.round((progressEvent.loaded * 100) / progressEvent.total);
            console.log(`📥 Progreso descarga: ${progress}%`);
          }
        }
      });
      
      // ✅ Crear descarga automática
      const blob = new Blob([response.data], { type: 'text/csv' });
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      
      // Obtener nombre del archivo de los headers
      const contentDisposition = response.headers['content-disposition'];
      const filename = contentDisposition 
        ? contentDisposition.split('filename=')[1].replace(/"/g, '')
        : `cruce_${new Date().toISOString().slice(0,19).replace(/:/g,'-')}.csv`;
      
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(url);
      
      console.log('Descarga completada:', filename);
      
    } catch (error: any) {
      console.error('❌ Error en descarga:', error);
      throw new Error(`Error en descarga: ${error.response?.data?.detail || error.message}`);
    }
  }

  static async getFileColumnsForCross(fileId: string, sheetName?: string): Promise<any> {
    const params = sheetName ? `?sheet_name=${sheetName}` : '';
    const response = await api.get(`/cross/columns/${fileId}${params}`);
    return response.data;
  }
}
