import api from '../Api';
import type { ExportRequest, ExportResponse } from '../types/api.types';

export class ExportService {
    
    static async exportData(request: ExportRequest): Promise<ExportResponse> {
        const response = await api.post('/export', request);
        return response.data;
    }

    static async downloadFile(filename: string): Promise<Blob> {
        const response = await api.get(`/download/${filename}`, {
            responseType: 'blob',
        });
        return response.data;
    }

    static async getExportFormats(): Promise<any> {
        const response = await api.get('/export/formats');
        return response.data;
    }

    static async getExportHistory(): Promise<any> {
        const response = await api.get('/exports');
        return response.data;
    }

    static async cleanupOldExports(daysOld: number = 7): Promise<any> {
        const response = await api.post('/cleanup-exports', null, {
            params: { days_old: daysOld },
        });
        return response.data;
    }
}
