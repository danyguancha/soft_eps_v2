// components/technical-note/HeaderSection.tsx
import React, { useState } from 'react';
import { Row, Col, Typography, Space, Button } from 'antd';
import { 
  BarChartOutlined, 
  CloudUploadOutlined, 
  ReloadOutlined,
  QuestionCircleOutlined 
} from '@ant-design/icons';
import { HelpModal } from './HelpModal';

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
  const [helpModalVisible, setHelpModalVisible] = useState(false);

  return (
    <>
      <Row justify="space-between" align="middle" style={{ marginBottom: 24 }}>
        <Col>
          <Title level={3} style={{ margin: 0 }}>
            <BarChartOutlined /> Nota Técnica
          </Title>
          <Space direction="vertical" size="small">
            <Space align="center" size={8}>
              <Text type="secondary">
                Selecciona la fecha de corte, luego un curso de vida o archivo 
                personalizado para ver datos y reportes
              </Text>
              <QuestionCircleOutlined 
                style={{ 
                  fontSize: 16, 
                  color: '#1890ff', 
                  cursor: 'pointer',
                  transition: 'all 0.3s'
                }}
                onClick={() => setHelpModalVisible(true)}
                onMouseEnter={(e) => {
                  e.currentTarget.style.transform = 'scale(1.2)';
                  e.currentTarget.style.color = '#40a9ff';
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.transform = 'scale(1)';
                  e.currentTarget.style.color = '#1890ff';
                }}
              />
            </Space>
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

      <HelpModal 
        visible={helpModalVisible}
        onClose={() => setHelpModalVisible(false)}
      />
    </>
  );
};
