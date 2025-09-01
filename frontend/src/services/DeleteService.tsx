import api from '../Api';
import type { DeleteRowsRequest, DeleteRowsByFilterRequest, DeleteResponse } from '../types/api.types';

export class DeleteService {
    
    static async deleteRows(request: DeleteRowsRequest): Promise<DeleteResponse> {
        const response = await api.delete('/rows', { data: request });
        return response.data;
    }

    static async deleteRowsByFilter(request: DeleteRowsByFilterRequest): Promise<DeleteResponse> {
        const response = await api.delete('/rows/filter', { data: request });
        return response.data;
    }

    static async bulkDelete(fileId: string, conditions: any[], confirmDelete: boolean = false, sheetName?: string): Promise<DeleteResponse> {
        const response = await api.delete('/rows/bulk', {
            data: {
                file_id: fileId,
                sheet_name: sheetName,
                conditions,
                confirm_delete: confirmDelete,
            },
        });
        return response.data;
    }

    static async removeDuplicates(fileId: string, columns?: string[], keep: string = 'first', sheetName?: string): Promise<DeleteResponse> {
        const response = await api.delete(`/duplicates/${fileId}`, {
            params: { columns, keep, sheet_name: sheetName },
        });
        return response.data;
    }
}
