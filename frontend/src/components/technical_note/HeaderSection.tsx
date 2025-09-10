// components/technical-note/HeaderSection.tsx
import React from 'react';
import { Row, Col, Typography, Space, Button } from 'antd';
import { BarChartOutlined, CloudUploadOutlined, ReloadOutlined } from '@ant-design/icons';

const { Title, Text } = Typography;

interface HeaderSectionProps {
  hasGeographicFilters: boolean;
  geographicSummary: string;
  loadingFiles: boolean;
  onShowUploadModal: () => void;
  onLoadAvailableFiles: () => void;
  onResetGeographicFilters: () => void;
}

export const HeaderSection: React.FC<HeaderSectionProps> = ({
  hasGeographicFilters,
  geographicSummary,
  loadingFiles,
  onShowUploadModal,
  onLoadAvailableFiles,
  onResetGeographicFilters
}) => {
  return (
    <Row justify="space-between" align="middle" style={{ marginBottom: 24 }}>
      <Col>
        <Title level={3} style={{ margin: 0 }}>
          <BarChartOutlined /> Nota Técnica
        </Title>
        <Space direction="vertical" size="small">
          <Text type="secondary">
            Selecciona un grupo etario o archivo personalizado para ver datos y reportes
          </Text>
          {hasGeographicFilters && (
            <Text type="success" style={{ fontSize: '12px' }}>
              📍 Filtros activos: {geographicSummary}
            </Text>
          )}
        </Space>
      </Col>
      <Col>
        <Space>
          {hasGeographicFilters && (
            <Button
              size="small"
              onClick={onResetGeographicFilters}
              type="default"
            >
              Limpiar filtros
            </Button>
          )}
          <Button
            icon={<CloudUploadOutlined />}
            onClick={onShowUploadModal}
            type="primary"
            ghost
          >
            Cargar Archivo
          </Button>
          <Button
            icon={<ReloadOutlined />}
            onClick={onLoadAvailableFiles}
            loading={loadingFiles}
          >
            Actualizar
          </Button>
        </Space>
      </Col>
    </Row>
  );
};
