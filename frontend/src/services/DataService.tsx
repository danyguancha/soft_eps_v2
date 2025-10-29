import api from '../Api';
import type { DataRequest, PaginatedResponse } from '../types/api.types';

export class DataService {
    
    static async getData(request: DataRequest): Promise<PaginatedResponse<Record<string, any>>> {
        const response = await api.post('/data', request);
        return response.data;
    }
}
