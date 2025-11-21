// components/TransformPanel.tsx
import React, { useState, useCallback } from 'react';
import { Card, Col, Row, Space, Modal, Button, Upload, message, Divider } from 'antd';
import { 
  UploadOutlined, 
  InboxOutlined,
  FileExcelOutlined,
  DeleteOutlined
} from '@ant-design/icons';
import { FileService } from '../../services/FileService';
import type { FileInfo } from '../../types/api.types';

const { Dragger } = Upload;

interface Props {
  isMobile: boolean;
  onSelectOp: (op: string) => void;
  availableFiles?: FileInfo[];
  onRefreshFiles?: () => Promise<void>;
  onFileUploaded?: (fileInfo: FileInfo) => void;
}

export const TransformPanel: React.FC<Props> = ({ 
  isMobile, 
  availableFiles = [],
  onRefreshFiles,
  onFileUploaded
}) => {
  const [uploadModalVisible, setUploadModalVisible] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [messageApi, contextHolder] = message.useMessage();

  const safeRefreshFiles = useCallback(async () => {
    if (onRefreshFiles) {
      console.log('🔄 Llamando onRefreshFiles...');
      try {
        await onRefreshFiles();
        console.log('onRefreshFiles completado');
      } catch (error) {
        console.error('❌ Error en onRefreshFiles:', error);
      }
    } else {
      console.warn('⚠️ onRefreshFiles no está definido');
    }
  }, [onRefreshFiles]);

  const handleFileUpload = async (file: File) => {
    console.log('📤 Iniciando subida de archivo:', file.name);
    setUploading(true);
    
    try {
      const result = await FileService.uploadFile(file);
      console.log('Archivo subido exitosamente:', result);
      
      messageApi.success(`Archivo "${file.name}" subido exitosamente`);
      
      if (onFileUploaded) {
        const fileInfo: FileInfo = {
          file_id: result.file_id,
          original_name: file.name,
          columns: result.columns,
          sheets: result.sheets || undefined,
          total_rows: result.total_rows
        };
        onFileUploaded(fileInfo);
      }
      
      await safeRefreshFiles();
      
      // Cerrar el modal automáticamente después de la subida exitosa
      setTimeout(() => {
        setUploadModalVisible(false);
      }, 1000); // Dar tiempo para que se vea el mensaje de éxito
      
    } catch (error: any) {
      console.error('❌ Error al subir archivo:', error);
      
      let errorMessage = 'Error desconocido';
      if (error.response?.data?.detail) {
        errorMessage = error.response.data.detail;
      } else if (error.message) {
        errorMessage = error.message;
      }
      
      messageApi.error({
        content: `Error al subir "${file.name}": ${errorMessage}`,
        duration: 6
      });
    } finally {
      setUploading(false);
    }
    
    return false;
  };

  const handleDeleteFile = async (fileId: string, fileName: string) => {
    try {
      console.log('🗑️ Eliminando archivo:', fileName, fileId);
      
      await FileService.deleteFile(fileId);
      console.log('Archivo eliminado exitosamente');
      
      messageApi.success(`Archivo "${fileName}" eliminado exitosamente`);
      await safeRefreshFiles();
      
    } catch (error: any) {
      console.error('❌ Error al eliminar archivo:', error);
      
      let errorMessage = 'Error desconocido';
      if (error.response?.data?.detail) {
        errorMessage = error.response.data.detail;
      } else if (error.message) {
        errorMessage = error.message;
      }
      
      messageApi.error(`Error al eliminar "${fileName}": ${errorMessage}`);
    }
  };

  const uploadProps = {
    name: 'file',
    multiple: true,
    accept: '.xlsx,.xls,.csv',
    beforeUpload: handleFileUpload,
    showUploadList: false,
    customRequest: ({ onSuccess }: any) => {
      setTimeout(() => {
        onSuccess("ok");
      }, 0);
    },
  };

  return (
    <div className="content-container">
      {contextHolder}
      
      <Card title="Transformaciones de Datos" className="transform-card">
        <Row gutter={[16, 16]}>
          
          {/* Cruce de Archivos */}
          <Col xs={24} sm={24} md={8}>
            <Card size="small" title="Cruce de Datos" className="transform-category">
              <Space direction="vertical" className="transform-buttons" style={{ width: '100%' }}>
                
                <Divider style={{ margin: '8px 0' }} />

                <Button
                  type="dashed"
                  size={isMobile ? 'large' : 'middle'}
                  icon={<UploadOutlined />}
                  onClick={() => setUploadModalVisible(true)}
                  style={{ width: '100%' }}
                  loading={uploading}
                >
                  {availableFiles.length === 0 
                    ? 'Subir Archivos' 
                    : `Subir Más (${availableFiles.length})`
                  }
                </Button>

                {/* Información de archivos */}
                <div style={{ 
                  fontSize: '11px', 
                  color: '#666', 
                  textAlign: 'center',
                  marginTop: '8px',
                  padding: '6px',
                  background: availableFiles.length >= 2 ? '#f6ffed' : '#fff7e6',
                  border: `1px dashed ${availableFiles.length >= 2 ? '#52c41a' : '#faad14'}`,
                  borderRadius: '4px'
                }}>
                  <div style={{ fontWeight: 'bold' }}>
                    {availableFiles.length} archivo{availableFiles.length !== 1 ? 's' : ''} disponible{availableFiles.length !== 1 ? 's' : ''}
                  </div>
                  <div style={{ fontSize: '10px', marginTop: '2px' }}>
                    {availableFiles.length >= 2 
                      ? 'Listo para cruzar' 
                      : `⚠️ Necesitas ${2 - availableFiles.length} más`
                    }
                  </div>
                </div>

                {/* Lista compacta de archivos */}
                {availableFiles.length > 0 && (
                  <div style={{ 
                    maxHeight: '120px', 
                    overflow: 'auto',
                    background: '#fafafa',
                    padding: '8px',
                    borderRadius: '4px',
                    border: '1px solid #d9d9d9'
                  }}>
                    {availableFiles.map((file, index) => (
                      <div key={file.file_id} style={{ 
                        display: 'flex', 
                        alignItems: 'center', 
                        justifyContent: 'space-between',
                        marginBottom: index < availableFiles.length - 1 ? '4px' : 0,
                        fontSize: '11px'
                      }}>
                        <div style={{ display: 'flex', alignItems: 'center', flex: 1, minWidth: 0 }}>
                          <FileExcelOutlined style={{ color: '#52c41a', marginRight: '4px' }} />
                          <span style={{ 
                            textOverflow: 'ellipsis', 
                            overflow: 'hidden', 
                            whiteSpace: 'nowrap'
                          }}>
                            {file.original_name}
                          </span>
                        </div>
                        <Button
                          type="text"
                          size="small"
                          icon={<DeleteOutlined />}
                          onClick={() => handleDeleteFile(file.file_id, file.original_name)}
                          style={{ 
                            padding: '2px 4px',
                            height: 'auto',
                            color: '#ff4d4f'
                          }}
                        />
                      </div>
                    ))}
                  </div>
                )}
              </Space>
            </Card>
          </Col>
        </Row>
      </Card>

      {/* Modal simplificado para subir archivos */}
      <Modal
        title="📤 Subir Archivos"
        open={uploadModalVisible}
        onCancel={() => setUploadModalVisible(false)}
        footer={[
          <Button key="close" onClick={() => setUploadModalVisible(false)}>
            Cerrar
          </Button>
        ]}
        width={600}
        destroyOnHidden
      >
        <div style={{ padding: '20px 0' }}>
          <Dragger {...uploadProps} style={{ marginBottom: '20px' }}>
            <p className="ant-upload-drag-icon">
              <InboxOutlined style={{ fontSize: '48px', color: '#1890ff' }} />
            </p>
            <p className="ant-upload-text">
              Arrastra archivos aquí o haz clic para seleccionar
            </p>
            <p className="ant-upload-hint">
              Soporta archivos .xlsx, .xls y .csv. El modal se cerrará automáticamente después de subir.
            </p>
          </Dragger>

          {/* Archivos actuales */}
          {availableFiles.length > 0 && (
            <>
              <h4>📁 Archivos Actuales ({availableFiles.length}):</h4>
              <div style={{ 
                background: '#f6ffed', 
                padding: '15px', 
                borderRadius: '6px',
                border: '1px solid #b7eb8f',
                maxHeight: '200px',
                overflow: 'auto'
              }}>
                {availableFiles.map((file, index) => (
                  <div key={file.file_id} style={{ 
                    display: 'flex', 
                    alignItems: 'center', 
                    justifyContent: 'space-between',
                    marginBottom: index < availableFiles.length - 1 ? '8px' : 0
                  }}>
                    <div>
                      <FileExcelOutlined style={{ color: '#52c41a', marginRight: '8px' }} />
                      <strong>{file.original_name}</strong>
                      <span style={{ color: '#666', marginLeft: '8px' }}>
                        ({file.total_rows} filas)
                      </span>
                    </div>
                    <Button
                      type="text"
                      size="small"
                      icon={<DeleteOutlined />}
                      onClick={() => handleDeleteFile(file.file_id, file.original_name)}
                      style={{ color: '#ff4d4f' }}
                    />
                  </div>
                ))}
              </div>
            </>
          )}
        </div>
      </Modal>

     
    </div>
  );
};
