import api from '../Api';

export interface FileCrossRequest {
    file1_key: string;
    file2_key: string;
    file1_sheet?: string;
    file2_sheet?: string;
    key_column_file1: string;
    key_column_file2: string;
    join_type: 'inner' | 'left' | 'right' | 'outer';
}

export class CrossService {
    
    static async crossFiles(request: FileCrossRequest): Promise<any> {
        const response = await api.post('/cross', request);
        return response.data;
    }
}
