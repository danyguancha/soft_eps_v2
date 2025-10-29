import api from '../Api';
import type { TransformRequest, TransformResponse } from '../types/api.types';

export class TransformService {
    
    static async transformData(request: TransformRequest): Promise<TransformResponse> {
        const response = await api.post('/transform', request);
        return response.data;
    }
}
