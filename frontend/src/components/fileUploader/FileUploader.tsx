import React, { useCallback } from 'react';
import { Upload, Button, message } from 'antd';
import { UploadOutlined } from '@ant-design/icons';
import type { UploadProps } from 'antd';
import { FileService } from '../../services/FileService';
import type { FileUploadResponse } from '../../types/api.types';

interface FileUploaderProps {
  onUploadSuccess: (response: FileUploadResponse) => void;
  loading?: boolean;
}

export const FileUploader: React.FC<FileUploaderProps> = ({ onUploadSuccess, loading }) => {
  const handleUpload = useCallback(async (file: File): Promise<boolean> => {
    try {
      const result = await FileService.uploadFile(file);
      onUploadSuccess(result);
      message.success('Archivo cargado exitosamente');
      return true;
    } catch (error) {
      message.error(`Error al cargar archivo: ${error instanceof Error ? error.message : 'Error desconocido'}`);
      return false;
    }
  }, [onUploadSuccess]);

  const uploadProps: UploadProps = {
    name: 'file',
    accept: '.csv,.xlsx,.xls',
    beforeUpload: (file) => {
      handleUpload(file);
      return false; // Prevenir upload automático
    },
    showUploadList: false,
  };

  return (
    <Upload {...uploadProps}>
      <Button icon={<UploadOutlined />} loading={loading} size="large">
        Cargar Archivo Excel/CSV
      </Button>
    </Upload>
  );
};
