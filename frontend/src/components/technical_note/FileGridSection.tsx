// components/technical-note/FileGridSection.tsx
import React from 'react';
import { Card, Row, Col, Typography, Space, Tooltip, Spin, Button, Alert } from 'antd';
import { CloudUploadOutlined, DeleteOutlined, LockOutlined } from '@ant-design/icons';
import type { AgeGroupIcon, CustomUploadedFile } from '../../types/FileTypes';

const { Text } = Typography;

interface FileGridSectionProps {
  allFileGroups: AgeGroupIcon[];
  selectedFile: string | null;
  availableFiles: any[];
  uploadedFiles: CustomUploadedFile[];
  fileSelectionLoading: boolean;
  hasGeographicFilters: boolean;
  cutoffDateSelected: boolean; // NUEVO
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
  cutoffDateSelected, // NUEVO
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
          {!cutoffDateSelected && (
            <LockOutlined style={{ color: '#ff4d4f', fontSize: 16 }} />
          )}
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
      style={{ 
        marginBottom: 24,
        opacity: cutoffDateSelected ? 1 : 0.6 // NUEVO: Opacidad reducida si no hay fecha
      }}
      bodyStyle={{ padding: '20px 24px' }}
    >
      {!cutoffDateSelected && (
        <Alert
          message="Selección bloqueada"
          description="Debe seleccionar una fecha de corte antes de poder cargar o seleccionar archivos."
          type="warning"
          showIcon
          style={{ marginBottom: 16 }}
        />
      )}

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
          const isDisabled = !cutoffDateSelected || !isAvailable; // NUEVO

          return (
            <Col key={group.key} xs={12} sm={8} md={4}>
              <Tooltip title={!cutoffDateSelected ? "Seleccione una fecha de corte primero" : group.description}>
                <div
                  style={{
                    textAlign: 'center',
                    cursor: isDisabled ? 'not-allowed' : 'pointer',
                    padding: '16px 8px',
                    borderRadius: '12px',
                    border: `2px solid ${isSelected ? group.color : '#f0f0f0'}`,
                    backgroundColor: isSelected
                      ? `${group.color}15`
                      : isDisabled ? '#f5f5f5' : '#fafafa',
                    transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
                    opacity: isDisabled ? 0.5 : 1,
                    boxShadow: isSelected
                      ? `0 4px 12px ${group.color}40`
                      : '0 2px 8px rgba(0,0,0,0.06)',
                    transform: isSelected ? 'translateY(-1px)' : 'translateY(0)',
                    position: 'relative',
                    filter: isDisabled ? 'grayscale(50%)' : 'none' // NUEVO
                  }}
                  onClick={() => !isDisabled && !isLoading && onFileGroupClick(group)}
                  onMouseEnter={(e) => {
                    if (!isDisabled && !isSelected && !isLoading) {
                      e.currentTarget.style.backgroundColor = `${group.color}10`;
                      e.currentTarget.style.transform = 'translateY(-3px)';
                      e.currentTarget.style.boxShadow = `0 6px 16px ${group.color}30`;
                    }
                  }}
                  onMouseLeave={(e) => {
                    if (!isDisabled && !isSelected && !isLoading) {
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
                      borderRadius: '10px',
                      zIndex: 10
                    }}>
                      <Spin size="small" />
                    </div>
                  )}

                  {!cutoffDateSelected && (
                    <div style={{
                      position: 'absolute',
                      top: '50%',
                      left: '50%',
                      transform: 'translate(-50%, -50%)',
                      zIndex: 5
                    }}>
                      <LockOutlined style={{ fontSize: 24, color: '#ff4d4f' }} />
                    </div>
                  )}

                  {group.isCustomFile && !isLoading && cutoffDateSelected && (
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
                    opacity: cutoffDateSelected && !isLoading ? 1 : 0.3,
                    filter: !isAvailable ? 'grayscale(100%)' : 'none'
                  }}>
                    {group.icon}
                  </div>

                  <div style={{
                    fontSize: '13px',
                    fontWeight: isSelected ? 600 : 500,
                    color: isDisabled ? '#bfbfbf' : '#262626',
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

                  {!isAvailable && !group.isCustomFile && cutoffDateSelected && (
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

                  {group.isCustomFile && !isSelected && !isLoading && cutoffDateSelected && (
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
          <Tooltip title={!cutoffDateSelected ? "Seleccione una fecha de corte primero" : "Cargar más archivos CSV, XLS o XLSX"}>
            <div
              style={{
                textAlign: 'center',
                cursor: cutoffDateSelected ? 'pointer' : 'not-allowed',
                padding: '16px 8px',
                borderRadius: '12px',
                border: '2px dashed #1890ff',
                backgroundColor: cutoffDateSelected ? '#f6ffff' : '#f5f5f5',
                transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
                boxShadow: '0 2px 8px rgba(24,144,255,0.1)',
                position: 'relative',
                opacity: cutoffDateSelected ? 1 : 0.5
              }}
              onClick={() => cutoffDateSelected && onShowUploadModal()}
              onMouseEnter={(e) => {
                if (cutoffDateSelected) {
                  e.currentTarget.style.backgroundColor = '#e6f7ff';
                  e.currentTarget.style.transform = 'translateY(-3px)';
                  e.currentTarget.style.boxShadow = '0 6px 16px rgba(24,144,255,0.2)';
                }
              }}
              onMouseLeave={(e) => {
                if (cutoffDateSelected) {
                  e.currentTarget.style.backgroundColor = '#f6ffff';
                  e.currentTarget.style.transform = 'translateY(0)';
                  e.currentTarget.style.boxShadow = '0 2px 8px rgba(24,144,255,0.1)';
                }
              }}
            >
              {!cutoffDateSelected && (
                <div style={{
                  position: 'absolute',
                  top: '50%',
                  left: '50%',
                  transform: 'translate(-50%, -50%)',
                  zIndex: 5
                }}>
                  <LockOutlined style={{ fontSize: 24, color: '#ff4d4f' }} />
                </div>
              )}

              <div style={{
                marginBottom: 12,
                display: 'flex',
                justifyContent: 'center',
                alignItems: 'center',
                height: '40px',
                opacity: cutoffDateSelected ? 1 : 0.3
              }}>
                <CloudUploadOutlined style={{ fontSize: '32px', color: '#1890ff' }} />
              </div>

              <div style={{
                fontSize: '13px',
                fontWeight: 500,
                color: cutoffDateSelected ? '#1890ff' : '#bfbfbf',
                lineHeight: 1.3
              }}>
                Cargar Más
              </div>

              <div style={{
                fontSize: '10px',
                color: cutoffDateSelected ? '#1890ff' : '#bfbfbf',
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
