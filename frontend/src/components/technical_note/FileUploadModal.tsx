// components/technical-note/FileUploadModal.tsx
import React from 'react';
import { Modal, Space, Alert, Upload, Typography, Card, Row, Col, Button } from 'antd';
import { CloudUploadOutlined, DeleteOutlined } from '@ant-design/icons';
import type { CustomUploadedFile, AgeGroupIcon } from '../../types/FileTypes';
import { createCustomFileIcon } from '../../config/ageGroups.config';

const { Title, Text } = Typography;
const { Dragger } = Upload;

interface FileUploadModalProps {
  visible: boolean;
  uploading: boolean;
  uploadedFiles: CustomUploadedFile[];
  fileList: any[];
  fileSelectionLoading: boolean;
  allFileGroups: AgeGroupIcon[];
  onCancel: () => void;
  onCustomUpload: (options: any) => void;
  onBeforeUpload: (file: File) => boolean;
  onUploadChange: (info: any) => void;
  onRemoveUploadedFile: (file: CustomUploadedFile) => void;
  onFileGroupClick: (group: AgeGroupIcon) => void;
}

export const FileUploadModal: React.FC<FileUploadModalProps> = ({
  visible,
  uploadedFiles,
  fileList,
  fileSelectionLoading,
  allFileGroups,
  onCancel,
  onCustomUpload,
  onBeforeUpload,
  onUploadChange,
  onRemoveUploadedFile,
  onFileGroupClick
}) => {
  return (
    <Modal
      title={
        <Space>
          <CloudUploadOutlined />
          <span>Cargar Archivo Personalizado</span>
        </Space>
      }
      open={visible}
      onCancel={onCancel}
      footer={null}
      width={600}
    >
      <Space direction="vertical" style={{ width: '100%' }} size="large">
        <Alert
          message="Tipos de archivo soportados"
          description="Puedes cargar archivos CSV, XLS o XLSX."
          type="info"
          showIcon
        />

        <Dragger
          name="file"
          multiple={false}
          customRequest={onCustomUpload}
          beforeUpload={onBeforeUpload}
          onChange={onUploadChange}
          fileList={fileList}
          accept=".csv,.xls,.xlsx,application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,text/csv"
        >
          <p className="ant-upload-drag-icon">
            <CloudUploadOutlined />
          </p>
          <p className="ant-upload-text">
            Haz clic o arrastra tu archivo aquí para subirlo
          </p>
          <p className="ant-upload-hint">
            Soporta archivos CSV, XLS y XLSX únicamente.
          </p>
        </Dragger>

        {uploadedFiles.length > 0 && (
          <div>
            <Title level={5}>Archivos Subidos ({uploadedFiles.length})</Title>
            <Space direction="vertical" style={{ width: '100%' }}>
              {uploadedFiles.map((file) => (
                <Card key={file.uid} size="small" style={{ marginBottom: 8 }}>
                  <Row justify="space-between" align="middle">
                    <Col flex="auto">
                      <Space>
                        {createCustomFileIcon()}
                        <div>
                          <Text strong>{file.name}</Text>
                          <br />
                          <Text type="secondary" style={{ fontSize: '12px' }}>
                            {(file.size / 1024 / 1024).toFixed(2)} MB • {file.uploadedAt.toLocaleString()}
                          </Text>
                        </div>
                      </Space>
                    </Col>
                    <Col>
                      <Space>
                        <Button
                          type="primary"
                          size="small"
                          onClick={() => {
                            const group = allFileGroups.find(g => g.filename === file.filename);
                            if (group) onFileGroupClick(group);
                          }}
                          loading={fileSelectionLoading}
                        >
                          Usar Archivo
                        </Button>
                        <Button
                          type="text"
                          size="small"
                          danger
                          icon={<DeleteOutlined />}
                          onClick={() => onRemoveUploadedFile(file)}
                        />
                      </Space>
                    </Col>
                  </Row>
                </Card>
              ))}
            </Space>
          </div>
        )}
      </Space>
    </Modal>
  );
};
