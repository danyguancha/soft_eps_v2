// src/components/tabs/tabs/ExportTab/ExportTab.tsx
import React from 'react';
import { Card, Row, Col, Typography, Space } from 'antd';
import { FileOutlined } from '@ant-design/icons';
import type { TabProps } from '../../../types/api.types';
import { useFileOperations } from '../../../hooks/useFileOperations';

const { Title, Text } = Typography;

export const ExportTab: React.FC<TabProps> = ({ fileData, isMobile }) => {
  const { handleExport } = useFileOperations();

  const exportOptions = [
    { key: 'csv', label: 'CSV', color: 'csv-icon', description: isMobile ? 'Separado por comas' : 'Formato separado por comas' },
    { key: 'excel', label: 'Excel', color: 'excel-icon', description: isMobile ? 'Microsoft Excel' : 'Formato Microsoft Excel' },
    { key: 'json', label: 'JSON', color: 'json-icon', description: isMobile ? 'JavaScript Object' : 'Formato JavaScript Object' },
  ];

  return (
    <div className="content-container">
      <Card title="Exportar Datos" className="export-card">
        <Row gutter={[16, 16]}>
          {exportOptions.map((option) => (
            <Col xs={24} sm={8} key={option.key}>
              <Card
                size="small"
                hoverable={!!fileData}
                onClick={() => handleExport(option.key as any)}
                className={`export-option ${option.key}-option ${!fileData ? 'disabled' : ''}`}
                style={{
                  opacity: !fileData ? 0.6 : 1,
                  cursor: !fileData ? 'not-allowed' : 'pointer'
                }}
              >
                <div className="export-content">
                  <FileOutlined className={`export-icon ${option.color}`} />
                  <Title level={isMobile ? 5 : 4}>{option.label}</Title>
                  <Text type="secondary">{option.description}</Text>
                </div>
              </Card>
            </Col>
          ))}
        </Row>

        {fileData && (
          <Card size="small" style={{ marginTop: 16 }} title="Archivo actual">
            <Space direction="vertical" style={{ width: '100%' }}>
              <Text><strong>Nombre:</strong> {fileData.original_name}</Text>
              <Text><strong>Filas:</strong> {fileData.total_rows?.toLocaleString()}</Text>
              <Text><strong>Columnas:</strong> {fileData.columns?.length}</Text>
              {fileData.sheets && fileData.sheets.length > 0 && (
                <Text><strong>Hojas:</strong> {fileData.sheets.join(', ')}</Text>
              )}
            </Space>
          </Card>
        )}
      </Card>
    </div>
  );
};
