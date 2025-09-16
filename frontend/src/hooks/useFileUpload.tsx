// hooks/useFileUpload.ts - ✅ LÓGICA DE UPLOAD
import { useState } from 'react';
import { message } from 'antd';
import { FileService } from '../services/FileService';
import type { CustomUploadedFile } from '../types/FileTypes';

export const useFileUpload = () => {
  const [uploadedFiles, setUploadedFiles] = useState<CustomUploadedFile[]>([]);
  const [uploading, setUploading] = useState(false);
  const [fileList, setFileList] = useState<any[]>([]);

  const handleCustomUpload = async (options: any) => {
    const { onSuccess, onError, file, onProgress } = options;

    try {
      setUploading(true);
      onProgress({ percent: 10 });

      console.log('📤 Subiendo archivo personalizado:', file.name);

      const uploadResponse = await FileService.uploadFile(file);
      onProgress({ percent: 80 });

      console.log('✅ Archivo subido exitosamente:', uploadResponse);

      const customFile: CustomUploadedFile = {
        uid: file.uid,
        name: file.name,
        filename: uploadResponse.filename || file.name,
        uploadedAt: new Date(),
        size: file.size,
        fileId: uploadResponse.file_id || uploadResponse.id
      };

      setUploadedFiles(prev => [...prev, customFile]);
      onProgress({ percent: 100 });
      onSuccess(uploadResponse, file);

      message.success(`Archivo "${file.name}" subido exitosamente`);
      return customFile;

    } catch (error: any) {
      console.error('❌ Error subiendo archivo:', error);
      onError(error);
      message.error(`Error subiendo "${file.name}": ${error.message || error}`);
      throw error;
    } finally {
      setUploading(false);
    }
  };

  const handleBeforeUpload = (file: File) => {
    const isValidType = file.type === 'text/csv' ||
      file.type === 'application/vnd.ms-excel' ||
      file.type === 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' ||
      file.name.endsWith('.csv') ||
      file.name.endsWith('.xlsx') ||
      file.name.endsWith('.xls');

    if (!isValidType) {
      message.error('Solo se permiten archivos CSV, XLS o XLSX');
      return false;
    }

    const isLt100M = file.size / 1024 / 1024 < 100;
    if (!isLt100M) {
      message.error('El archivo debe ser menor a 100MB');
      return false;
    }

    return true;
  };

  const handleUploadChange = (info: any) => {
    setFileList(info.fileList);

    if (info.file.status === 'done') {
      console.log(`✅ ${info.file.name} subido exitosamente`);
    } else if (info.file.status === 'error') {
      console.error(`❌ ${info.file.name} falló al subir`);
    }
  };

  const handleRemoveUploadedFile = (fileToRemove: CustomUploadedFile) => {
    return new Promise<void>((resolve) => {
      setUploadedFiles(prev => prev.filter(f => f.uid !== fileToRemove.uid));
      message.success('Archivo eliminado');
      resolve();
    });
  };

  return {
    uploadedFiles,
    uploading,
    fileList,
    setUploadedFiles,
    handleCustomUpload,
    handleBeforeUpload,
    handleUploadChange,
    handleRemoveUploadedFile
  };
};
