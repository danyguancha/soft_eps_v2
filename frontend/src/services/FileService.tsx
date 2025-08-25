import api from '../Api';
import type { FileUploadResponse, FileInfo } from '../types/api.types';

export class FileService {
    
    static async uploadFile(file: File): Promise<FileUploadResponse> {
        const formData = new FormData();
        formData.append('file', file);
        
        const response = await api.post('/upload', formData, {
            headers: {
                'Content-Type': 'multipart/form-data',
            },
        });
        
        return response.data;
    }

    static async getFileInfo(fileId: string): Promise<FileInfo> {
        const response = await api.get(`/file/${fileId}`);
        return response.data;
    }

    static async deleteFile(fileId: string): Promise<{ message: string }> {
        const response = await api.delete(`/file/${fileId}`);
        return response.data;
    }

    static async listFiles(): Promise<{ files: FileInfo[]; total: number }> {
        const response = await api.get('/files');
        return response.data;
    }

    static async getColumns(fileId: string, sheetName?: string): Promise<{ columns: string[]; sheet_name: string }> {
        const response = await api.get(`/columns/${fileId}`, {
            params: { sheet_name: sheetName },
        });
        return response.data;
    }
}
