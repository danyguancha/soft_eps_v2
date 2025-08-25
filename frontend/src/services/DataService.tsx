import api from '../Api';
import type { DataRequest, PaginatedResponse } from '../types/api.types';

export class DataService {
    
    static async getData(request: DataRequest): Promise<PaginatedResponse<Record<string, any>>> {
        const response = await api.post('/data', request);
        return response.data;
    }

    static async getDataSample(fileId: string, sheetName?: string, sampleSize: number = 10): Promise<any> {
        const response = await api.get(`/data/sample/${fileId}`, {
            params: { sheet_name: sheetName, sample_size: sampleSize },
        });
        return response.data;
    }

    static async getColumnStatistics(fileId: string, columnName: string, sheetName?: string): Promise<any> {
        const response = await api.get(`/data/statistics/${fileId}/${columnName}`, {
            params: { sheet_name: sheetName },
        });
        return response.data;
    }
}
