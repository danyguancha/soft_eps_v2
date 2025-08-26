// components/TransformPanel.tsx
import React, { useState, useCallback } from 'react';
import { Card, Col, Row, Space, Modal, Button, Upload, message, Divider } from 'antd';
import { 
  LinkOutlined, 
  FileTextOutlined, 
  UploadOutlined, 
  InboxOutlined,
  FileExcelOutlined,
  DeleteOutlined
} from '@ant-design/icons';
import { TransformButton } from './TransformButton';
import FileCrossManager from '../cross/FileCrossManager';
import { FileService } from '../../services/FileService';
import type { FileInfo } from '../../types/api.types';

const { Dragger } = Upload;

interface Props {
  isMobile: boolean;
  onSelectOp: (op: string) => void;
  availableFiles?: FileInfo[];
  onRefreshFiles?: () => Promise<void>; // Cambiar a Promise<void> para async
  onFileUploaded?: (fileInfo: FileInfo) => void;
}

export const TransformPanel: React.FC<Props> = ({ 
  isMobile, 
  onSelectOp,
  availableFiles = [],
  onRefreshFiles,
  onFileUploaded
}) => {
  const [crossModalVisible, setCrossModalVisible] = useState(false);
  const [uploadModalVisible, setUploadModalVisible] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [messageApi, contextHolder] = message.useMessage();

  // Validar que onRefreshFiles existe
  const safeRefreshFiles = useCallback(async () => {
    if (onRefreshFiles) {
      console.log('🔄 Llamando onRefreshFiles...');
      try {
        await onRefreshFiles();
        console.log('✅ onRefreshFiles completado');
      } catch (error) {
        console.error('❌ Error en onRefreshFiles:', error);
      }
    } else {
      console.warn('⚠️ onRefreshFiles no está definido');
    }
  }, [onRefreshFiles]);

  const handleOpenCross = () => {
    console.log('🚀 handleOpenCross ejecutado');
    console.log('📊 Archivos disponibles:', availableFiles.length);
    console.log('📁 Lista actual de archivos:', availableFiles.map(f => ({ id: f.file_id, name: f.original_name })));
    
    if (availableFiles.length < 2) {
      messageApi.warning({
        content: `Necesitas al menos 2 archivos para realizar un cruce. Actualmente tienes ${availableFiles.length} archivo(s).`,
        duration: 4,
        style: { marginTop: '10vh' }
      });
      
      setTimeout(() => {
        setUploadModalVisible(true);
      }, 1000);
      
      return;
    }
    
    setCrossModalVisible(true);
  };

  const handleFileUpload = async (file: File) => {
    console.log('📤 Iniciando subida de archivo:', file.name);
    setUploading(true);
    
    try {
      const result = await FileService.uploadFile(file);
      console.log('✅ Archivo subido exitosamente:', result);
      
      messageApi.success(`Archivo "${file.name}" subido exitosamente`);
      
      // Callback opcional para notificar al padre inmediatamente
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
      
      // Refrescar la lista de archivos
      await safeRefreshFiles();
      
      // Verificar si ahora tenemos suficientes archivos
      // Nota: usamos setTimeout para permitir que el estado se actualice
      setTimeout(() => {
        console.log('📊 Archivos después del refresh:', availableFiles.length);
        if (availableFiles.length >= 1) { // Se compara con >= 1 porque acabamos de subir uno
          messageApi.info({
            content: `¡Genial! Archivo subido. ${availableFiles.length >= 2 ? '¿Quieres hacer el cruce ahora?' : 'Sube un archivo más para poder hacer el cruce.'}`,
            duration: 6
          });
        }
      }, 1500);
      
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
      console.log('✅ Archivo eliminado exitosamente');
      
      messageApi.success(`Archivo "${fileName}" eliminado exitosamente`);
      
      // Refrescar la lista
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

  // Debug: Mostrar estado actual
  console.log('🎭 Rendering TransformPanel');
  console.log('📁 availableFiles.length:', availableFiles.length);
  console.log('📋 onRefreshFiles defined:', !!onRefreshFiles);

  return (
    <div className="content-container">
      {contextHolder}
      
      <Card title="Transformaciones de Datos" className="transform-card">
        <Row gutter={[16, 16]}>
          {/* Operaciones básicas */}
          <Col xs={24} sm={24} md={8}>
            <Card size="small" title="Operaciones Básicas" className="transform-category">
              <Space direction="vertical" className="transform-buttons" style={{ width: '100%' }}>
                <TransformButton
                  opKey="concatenate"
                  label="Concatenar Columnas"
                  size={isMobile ? 'large' : 'middle'}
                  onSelect={onSelectOp}
                />
                <TransformButton
                  opKey="split_column"
                  label="Dividir Columna"
                  size={isMobile ? 'large' : 'middle'}
                  onSelect={onSelectOp}
                />
                <TransformButton
                  opKey="replace_values"
                  label="Reemplazar Valores"
                  size={isMobile ? 'large' : 'middle'}
                  onSelect={onSelectOp}
                />
              </Space>
            </Card>
          </Col>

          {/* Operaciones avanzadas */}
          <Col xs={24} sm={24} md={8}>
            <Card size="small" title="Operaciones Avanzadas" className="transform-category">
              <Space direction="vertical" className="transform-buttons" style={{ width: '100%' }}>
                <TransformButton
                  opKey="to_uppercase"
                  label="Convertir a Mayúsculas"
                  size={isMobile ? 'large' : 'middle'}
                  onSelect={onSelectOp}
                />
                <TransformButton
                  opKey="fill_null"
                  label="Llenar Valores Nulos"
                  size={isMobile ? 'large' : 'middle'}
                  onSelect={onSelectOp}
                />
                <TransformButton
                  opKey="delete_column"
                  label="Eliminar Columna"
                  size={isMobile ? 'large' : 'middle'}
                  onSelect={onSelectOp}
                />
              </Space>
            </Card>
          </Col>

          {/* Cruce de Archivos */}
          <Col xs={24} sm={24} md={8}>
            <Card size="small" title="Cruce de Datos" className="transform-category">
              <Space direction="vertical" className="transform-buttons" style={{ width: '100%' }}>
                <Button
                  type="primary"
                  size={isMobile ? 'large' : 'middle'}
                  icon={<LinkOutlined />}
                  onClick={handleOpenCross}
                  style={{ width: '100%' }}
                  disabled={availableFiles.length < 2}
                >
                  Cruzar Archivos ({availableFiles.length}/2)
                </Button>
                
                <Button
                  type="default"
                  size={isMobile ? 'large' : 'middle'}
                  icon={<FileTextOutlined />}
                  onClick={handleOpenCross}
                  style={{ width: '100%' }}
                  disabled={availableFiles.length < 2}
                >
                  VLOOKUP Avanzado
                </Button>

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

                {/* Debug button */}
                <Button
                  type="dashed"
                  size="small"
                  onClick={async () => {
                    console.log('🔄 Refresh manual triggered');
                    await safeRefreshFiles();
                  }}
                  style={{ width: '100%', fontSize: '10px' }}
                >
                  🔄 Refresh Manual
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
                      ? '✅ Listo para cruzar' 
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

      {/* Modal para subir archivos */}
      <Modal
        title="📤 Subir Archivos para Cruce"
        open={uploadModalVisible}
        onCancel={() => setUploadModalVisible(false)}
        footer={[
          <Button key="refresh" onClick={safeRefreshFiles}>
            🔄 Actualizar Lista
          </Button>,
          <Button key="close" onClick={() => setUploadModalVisible(false)}>
            Cerrar
          </Button>
        ]}
        width={600}
        destroyOnClose
      >
        <div style={{ padding: '20px 0' }}>
          <div style={{ marginBottom: '20px', textAlign: 'center' }}>
            <h4 style={{ color: '#1890ff' }}>
              {availableFiles.length === 0 
                ? 'Sube al menos 2 archivos para poder hacer el cruce'
                : `Tienes ${availableFiles.length} archivo(s). ${availableFiles.length < 2 ? `Sube ${2 - availableFiles.length} más para hacer el cruce.` : '¡Ya puedes hacer el cruce!'}`
              }
            </h4>
          </div>

          <Dragger {...uploadProps} style={{ marginBottom: '20px' }}>
            <p className="ant-upload-drag-icon">
              <InboxOutlined style={{ fontSize: '48px', color: '#1890ff' }} />
            </p>
            <p className="ant-upload-text">
              Arrastra archivos aquí o haz clic para seleccionar
            </p>
            <p className="ant-upload-hint">
              Soporta archivos .xlsx, .xls y .csv. Puedes subir múltiples archivos.
            </p>
          </Dragger>

          {/* Archivos actuales */}
          {availableFiles.length > 0 && (
            <>
              <h4>📁 Archivos Actuales:</h4>
              <div style={{ 
                background: '#f6ffed', 
                padding: '15px', 
                borderRadius: '6px',
                border: '1px solid #b7eb8f'
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
                      {file.sheets && file.sheets.length > 0 && (
                        <span style={{ color: '#666', marginLeft: '8px' }}>
                          | {file.sheets.length} hoja{file.sheets.length !== 1 ? 's' : ''}
                        </span>
                      )}
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

          {availableFiles.length >= 2 && (
            <div style={{ 
              marginTop: '20px', 
              textAlign: 'center',
              background: '#e6f7ff',
              padding: '15px',
              borderRadius: '6px',
              border: '1px solid #91d5ff'
            }}>
              <p style={{ color: '#1890ff', fontWeight: 'bold', margin: 0 }}>
                ✅ ¡Perfecto! Ya tienes {availableFiles.length} archivos.
              </p>
              <Button 
                type="primary" 
                size="large"
                icon={<LinkOutlined />}
                onClick={() => {
                  setUploadModalVisible(false);
                  setCrossModalVisible(true);
                }}
                style={{ marginTop: '10px' }}
              >
                Proceder al Cruce de Archivos
              </Button>
            </div>
          )}
        </div>
      </Modal>

      {/* Modal para el cruce de archivos */}
      <Modal
        title="🔄 Cruzar Archivos"
        open={crossModalVisible}
        onCancel={() => setCrossModalVisible(false)}
        footer={null}
        width="95%"
        style={{ maxWidth: '1400px' }}
        destroyOnClose
        centered
      >
        <FileCrossManager
          availableFiles={availableFiles}
          onRefreshFiles={safeRefreshFiles}
          onComplete={() => setCrossModalVisible(false)}
        />
      </Modal>
    </div>
  );
};
