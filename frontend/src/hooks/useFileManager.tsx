// hooks/useFileManager.ts
import { useState, useCallback, useEffect } from 'react'; // ← Agregar useEffect
import { FileService } from '../services/FileService';
import { DataService } from '../services/DataService';
import type { FileInfo, DataRequest, PaginatedResponse } from '../types/api.types';

export function useFileManager() {
  const [files, setFiles] = useState<FileInfo[]>([]);
  const [currentFile, setCurrentFile] = useState<FileInfo | null>(null);
  const [currentData, setCurrentData] = useState<PaginatedResponse<Record<string, any>> | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadFiles = useCallback(async () => {
    try {
      console.log('🔄 Cargando lista de archivos...');
      setLoading(true);
      const response = await FileService.listFiles();
      console.log('📁 Archivos obtenidos del API:', response.files?.length || 0);
      setFiles(response.files);
      setError(null);
    } catch (err) {
      console.error('❌ Error cargando archivos:', err);
      setError(err instanceof Error ? err.message : 'Error al cargar archivos');
    } finally {
      setLoading(false);
    }
  }, []);

  // ✅ AGREGAR ESTE useEffect - Esta es la clave del problema
  useEffect(() => {
    console.log('🎯 useFileManager montado - cargando archivos iniciales...');
    loadFiles();
  }, [loadFiles]);

  const uploadFile = useCallback(async (file: File) => {
    try {
      setLoading(true);
      const response = await FileService.uploadFile(file);
      await loadFiles(); // Recargar lista
      setError(null);
      return response;
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error al subir archivo');
      throw err;
    } finally {
      setLoading(false);
    }
  }, [loadFiles]);

  const loadFileData = useCallback(async (request: DataRequest) => {
    try {
      setLoading(true);
      const response = await DataService.getData(request);
      setCurrentData(response);
      setError(null);
      return response;
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error al cargar datos');
      throw err;
    } finally {
      setLoading(false);
    }
  }, []);

  const deleteFile = useCallback(async (fileId: string) => {
    try {
      setLoading(true);
      await FileService.deleteFile(fileId);
      await loadFiles(); // Recargar lista
      if (currentFile?.file_id === fileId) {
        setCurrentFile(null);
        setCurrentData(null);
      }
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error al eliminar archivo');
      throw err;
    } finally {
      setLoading(false);
    }
  }, [loadFiles, currentFile?.file_id]);

  return {
    files,
    currentFile,
    currentData,
    loading,
    error,
    loadFiles, // ✅ Esta función ya estaba exportada correctamente
    uploadFile,
    loadFileData,
    deleteFile,
    setCurrentFile,
    setError
  };
}
