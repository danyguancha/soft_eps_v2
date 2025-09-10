// components/technical-note/FileGridSection.tsx - ✅ FILE GRID SECTION
import React from 'react';
import { Card, Row, Col, Typography, Space, Tooltip, Spin, Button } from 'antd';
import { CloudUploadOutlined, DeleteOutlined } from '@ant-design/icons';
import type { AgeGroupIcon, CustomUploadedFile } from '../../types/FileTypes';

const { Text } = Typography;

interface FileGridSectionProps {
  allFileGroups: AgeGroupIcon[];
  selectedFile: string | null;
  availableFiles: any[];
  uploadedFiles: CustomUploadedFile[];
  fileSelectionLoading: boolean;
  hasGeographicFilters: boolean;
  onFileGroupClick: (group: AgeGroupIcon) => void;
  onShowUploadModal: () => void;
  onRemoveUploadedFile: (file: CustomUploadedFile) => void;
  getFileByDisplayName: (displayName: string) => any;
}

export const FileGridSection: React.FC<FileGridSectionProps> = ({
  allFileGroups,
  selectedFile,
  availableFiles,
  uploadedFiles,
  fileSelectionLoading,
  hasGeographicFilters,
  onFileGroupClick,
  onShowUploadModal,
  onRemoveUploadedFile,
  getFileByDisplayName
}) => {
  return (
    <Card
      title={
        <Space>
          <span>Archivos Disponibles</span>
          {uploadedFiles.length > 0 && (
            <Text type="secondary" style={{ fontSize: '12px' }}>
              ({allFileGroups.length - uploadedFiles.length} precargados, {uploadedFiles.length} personalizados)
            </Text>
          )}
          {hasGeographicFilters && (
            <Text type="secondary" style={{ fontSize: '12px' }}>
              (Con filtros geográficos aplicados)
            </Text>
          )}
        </Space>
      }
      size="small"
      style={{ marginBottom: 24 }}
      bodyStyle={{ padding: '20px 24px' }}
    >
      <Row gutter={[16, 16]} justify="start">
        {allFileGroups.map((group) => {
          let isAvailable = true;
          let isSelected = false;

          if (group.isCustomFile) {
            isSelected = selectedFile === group.filename;
            isAvailable = true;
          } else {
            const fileInfo = getFileByDisplayName(group.displayName);
            isSelected = !!(selectedFile && fileInfo?.filename && selectedFile === fileInfo.filename);
            isAvailable = availableFiles.some(f => f.display_name === group.displayName);
          }

          const isLoading = fileSelectionLoading && isSelected;

          return (
            <Col key={group.key} xs={12} sm={8} md={4}>
              <Tooltip title={group.description}>
                <div
                  style={{
                    textAlign: 'center',
                    cursor: isAvailable ? 'pointer' : 'not-allowed',
                    padding: '16px 8px',
                    borderRadius: '12px',
                    border: `2px solid ${isSelected ? group.color : '#f0f0f0'}`,
                    backgroundColor: isSelected
                      ? `${group.color}15`
                      : isAvailable ? '#fafafa' : '#f5f5f5',
                    transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
                    opacity: isAvailable ? 1 : 0.6,
                    boxShadow: isSelected
                      ? `0 4px 12px ${group.color}40`
                      : '0 2px 8px rgba(0,0,0,0.06)',
                    transform: isSelected ? 'translateY(-1px)' : 'translateY(0)',
                    position: 'relative'
                  }}
                  onClick={() => isAvailable && !isLoading && onFileGroupClick(group)}
                  onMouseEnter={(e) => {
                    if (isAvailable && !isSelected && !isLoading) {
                      e.currentTarget.style.backgroundColor = `${group.color}10`;
                      e.currentTarget.style.transform = 'translateY(-3px)';
                      e.currentTarget.style.boxShadow = `0 6px 16px ${group.color}30`;
                    }
                  }}
                  onMouseLeave={(e) => {
                    if (isAvailable && !isSelected && !isLoading) {
                      e.currentTarget.style.backgroundColor = '#fafafa';
                      e.currentTarget.style.transform = 'translateY(0)';
                      e.currentTarget.style.boxShadow = '0 2px 8px rgba(0,0,0,0.06)';
                    }
                  }}
                >
                  {isLoading && (
                    <div style={{
                      position: 'absolute',
                      top: 0,
                      left: 0,
                      right: 0,
                      bottom: 0,
                      backgroundColor: 'rgba(255,255,255,0.8)',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      borderRadius: '10px'
                    }}>
                      <Spin size="small" />
                    </div>
                  )}

                  {group.isCustomFile && !isLoading && (
                    <div style={{
                      position: 'absolute',
                      top: '4px',
                      right: '4px'
                    }}>
                      <Button
                        type="text"
                        size="small"
                        icon={<DeleteOutlined />}
                        onClick={(e) => {
                          e.stopPropagation();
                          const fileToRemove = uploadedFiles.find(f => f.filename === group.filename);
                          if (fileToRemove) {
                            onRemoveUploadedFile(fileToRemove);
                          }
                        }}
                        style={{
                          color: '#ff4d4f',
                          fontSize: '12px',
                          padding: '2px',
                          minWidth: 'auto',
                          height: 'auto'
                        }}
                      />
                    </div>
                  )}

                  <div style={{
                    marginBottom: 12,
                    display: 'flex',
                    justifyContent: 'center',
                    alignItems: 'center',
                    height: '40px',
                    opacity: isAvailable && !isLoading ? 1 : 0.5,
                    filter: !isAvailable ? 'grayscale(100%)' : 'none'
                  }}>
                    {group.icon}
                  </div>

                  <div style={{
                    fontSize: '13px',
                    fontWeight: isSelected ? 600 : 500,
                    color: isAvailable ? '#262626' : '#bfbfbf',
                    lineHeight: 1.3,
                    marginBottom: group.isCustomFile ? 6 : 0
                  }}>
                    {group.displayName}
                  </div>

                  {group.isCustomFile && group.fileSize && (
                    <div style={{
                      fontSize: '10px',
                      color: '#8c8c8c',
                      marginBottom: 4
                    }}>
                      {(group.fileSize / 1024 / 1024).toFixed(1)} MB
                    </div>
                  )}

                  {!isAvailable && !group.isCustomFile && (
                    <div style={{
                      fontSize: '10px',
                      color: '#ff4d4f',
                      marginTop: 6,
                      fontWeight: 500
                    }}>
                      No disponible
                    </div>
                  )}

                  {isSelected && !isLoading && (
                    <div style={{
                      fontSize: '10px',
                      color: group.color,
                      marginTop: 4,
                      fontWeight: 600,
                      textTransform: 'uppercase',
                      letterSpacing: '0.5px'
                    }}>
                      ● Activo
                    </div>
                  )}

                  {group.isCustomFile && !isSelected && !isLoading && (
                    <div style={{
                      fontSize: '10px',
                      color: group.color,
                      marginTop: 4,
                      fontWeight: 500
                    }}>
                      Personalizado
                    </div>
                  )}
                </div>
              </Tooltip>
            </Col>
          );
        })}

        {/* Botón para cargar más archivos */}
        <Col xs={12} sm={8} md={4}>
          <Tooltip title="Cargar más archivos CSV, XLS o XLSX">
            <div
              style={{
                textAlign: 'center',
                cursor: 'pointer',
                padding: '16px 8px',
                borderRadius: '12px',
                border: '2px dashed #1890ff',
                backgroundColor: '#f6ffff',
                transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
                boxShadow: '0 2px 8px rgba(24,144,255,0.1)',
                position: 'relative'
              }}
              onClick={onShowUploadModal}
              onMouseEnter={(e) => {
                e.currentTarget.style.backgroundColor = '#e6f7ff';
                e.currentTarget.style.transform = 'translateY(-3px)';
                e.currentTarget.style.boxShadow = '0 6px 16px rgba(24,144,255,0.2)';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.backgroundColor = '#f6ffff';
                e.currentTarget.style.transform = 'translateY(0)';
                e.currentTarget.style.boxShadow = '0 2px 8px rgba(24,144,255,0.1)';
              }}
            >
              <div style={{
                marginBottom: 12,
                display: 'flex',
                justifyContent: 'center',
                alignItems: 'center',
                height: '40px'
              }}>
                <CloudUploadOutlined style={{ fontSize: '32px', color: '#1890ff' }} />
              </div>

              <div style={{
                fontSize: '13px',
                fontWeight: 500,
                color: '#1890ff',
                lineHeight: 1.3
              }}>
                Cargar Más
              </div>

              <div style={{
                fontSize: '10px',
                color: '#1890ff',
                marginTop: 4,
                fontWeight: 500
              }}>
                Archivos
              </div>
            </div>
          </Tooltip>
        </Col>
      </Row>
    </Card>
  );
};
