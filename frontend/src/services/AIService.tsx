import api from '../Api';
import type { AIRequest, AIResponse } from '../types/api.types';

export class AIService {
    
    static async askAI(request: AIRequest): Promise<AIResponse> {
        const response = await api.post('/ai', request);
        return response.data;
    }
}
