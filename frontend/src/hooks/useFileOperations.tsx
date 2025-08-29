// src/hooks/useFileOperations.ts
import { useCallback } from 'react';
import { useFileManager } from './useFileManager';
import { useAlert } from '../components/alerts/AlertProvider';
import { TransformService } from '../services/TransformService';
import { ExportService } from '../services/ExportService';
import { DeleteService } from '../services/DeleteService';
import type { FileInfo, UseFileOperationsReturn, DataRequest } from '../types/api.types';

export const useFileOperations = (): UseFileOperationsReturn => {
  const { showAlert } = useAlert();
  const fileManager = useFileManager();

  const handleUploadSuccess = useCallback(async (res: any) => {
    const newFile = {
      file_id: res.file_id,
      original_name: res.original_name ?? res.filename ?? 'Archivo',
      columns: res.columns,
      sheets: res.sheets,
      total_rows: res.total_rows,
    };
    
    fileManager.setCurrentFile(newFile);
    
    await showAlert({
      title: '✅ ¡Éxito!',
      message: 'Archivo cargado correctamente',
      variant: 'success'
    });
  }, [fileManager.setCurrentFile, showAlert]);

  const handleDeleteRows = useCallback(async (indices: number[]) => {
    if (!fileManager.currentFile) return;
    
    await showAlert({
      title: '⚠️ ¿Eliminar filas?',
      message: `Esta acción eliminará ${indices.length} fila(s) permanentemente.`,
      variant: 'warning',
      actions: [
        {
          label: 'Cancelar',
          type: 'secondary',
          onClick: () => {}
        },
        {
          label: 'Eliminar',
          type: 'primary',
          onClick: async () => {
            try {
              await DeleteService.deleteRows({
                file_id: fileManager.currentFile!.file_id,
                row_indices: indices,
              });
              
              await showAlert({
                title: 'Filas eliminadas',
                message: `${indices.length} filas eliminadas exitosamente`,
                variant: 'success'
              });
              
              // Recargar datos
              await fileManager.loadFileData({
                file_id: fileManager.currentFile!.file_id,
                page: 1,
                page_size: 20,
              });
            } catch (error) {
              await showAlert({
                title: 'Error',
                message: 'No se pudieron eliminar las filas',
                variant: 'error'
              });
            }
          }
        }
      ]
    });
  }, [fileManager.currentFile, fileManager.loadFileData, showAlert]);

  const handleExport = useCallback(async (format: 'csv' | 'excel' | 'json') => {
    if (!fileManager.currentFile) {
      await showAlert({
        title: 'Sin archivo',
        message: 'Selecciona un archivo para exportar',
        variant: 'warning'
      });
      return;
    }

    try {
      const res = await ExportService.exportData({
        file_id: fileManager.currentFile.file_id,
        format,
        include_headers: true,
      });
      
      await showAlert({
        title: '📥 Descarga completada',
        message: `Descargado: ${res.filename}`,
        variant: 'success'
      });
    } catch (error) {
      await showAlert({
        title: 'Error de exportación',
        message: 'Error al exportar archivo',
        variant: 'error'
      });
    }
  }, [fileManager.currentFile, showAlert]);

  const handleFileUploadedFromTransform = useCallback(async (fileInfo: FileInfo) => {
    const newFileData = {
      file_id: fileInfo.file_id,
      original_name: fileInfo.original_name,
      columns: fileInfo.columns,
      sheets: fileInfo.sheets,
      total_rows: fileInfo.total_rows,
    };

    if (!fileManager.currentFile) {
      fileManager.setCurrentFile(newFileData);
      await showAlert({
        title: '✅ Archivo seleccionado',
        message: `Archivo "${fileInfo.original_name}" seleccionado automáticamente`,
        variant: 'success'
      });
    }

    await showAlert({
      title: '📤 Archivo disponible',
      message: `Archivo "${fileInfo.original_name}" disponible para cruce`,
      variant: 'info'
    });
  }, [fileManager.currentFile, fileManager.setCurrentFile, showAlert]);

  return {
    // ✅ Asegurar que devolvemos los tipos correctos
    files: fileManager.files ?? null,
    currentFile: fileManager.currentFile ?? null,
    currentData: fileManager.currentData ?? null,
    loading: fileManager.loading,
    error: fileManager.error,
    setCurrentFile: fileManager.setCurrentFile,
    setError: fileManager.setError,
    loadFiles: fileManager.loadFiles,
    loadFileData: fileManager.loadFileData,
    deleteFile: fileManager.deleteFile,
    handleUploadSuccess,
    handleDeleteRows,
    handleExport,
    handleFileUploadedFromTransform,
  };
};
