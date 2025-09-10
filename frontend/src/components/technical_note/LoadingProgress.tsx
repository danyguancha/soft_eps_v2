// components/technical-note/LoadingProgress.tsx
import React from 'react';
import { Card, Row, Col, Space, Typography, Progress } from 'antd';
import { ClockCircleOutlined } from '@ant-design/icons';

const { Text } = Typography;

interface LoadingProgressProps {
  isVisible: boolean;
  currentPage?: number;
  totalPages?: number;
  hasGeographicFilters?: boolean;
  geographicSummary?: string;
  isLoadingFiles?: boolean;
}

export const LoadingProgress: React.FC<LoadingProgressProps> = ({
  isVisible,
  currentPage,
  totalPages,
  hasGeographicFilters,
  geographicSummary,
  isLoadingFiles = false
}) => {
  if (!isVisible) return null;

  return (
    <Card style={{ marginBottom: 16 }}>
      <Row align="middle">
        <Col span={24}>
          <Space direction="vertical" style={{ width: '100%' }}>
            <Text>
              <ClockCircleOutlined spin /> 
              {isLoadingFiles 
                ? 'Cargando lista de archivos técnicos...' 
                : 'Cargando datos con filtros geográficos...'
              }
            </Text>
            {!isLoadingFiles && totalPages && totalPages > 0 && (
              <Progress
                percent={Math.round(((currentPage || 0) / totalPages) * 100)}
                status="active"
                strokeColor="#1890ff"
              />
            )}
            {!isLoadingFiles && (
              <Progress percent={100} status="active" showInfo={false} />
            )}
            {hasGeographicFilters && (
              <Text type="secondary" style={{ fontSize: '12px' }}>
                📍 Aplicando filtros: {geographicSummary}
              </Text>
            )}
          </Space>
        </Col>
      </Row>
    </Card>
  );
};
