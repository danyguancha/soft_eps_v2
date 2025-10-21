// components/report/KeywordControls.tsx
import { memo, useCallback } from 'react';
import { Card, Space, Typography, Tag, Row, Col, Button } from 'antd';
import { FilterOutlined, SyncOutlined } from '@ant-design/icons';
import { OptimizedKeywordSelect } from './OptimizedKeywordSelect';

const { Text } = Typography;

interface KeywordControlsProps {
  reportKeywords: string[];
  hasReport: boolean;
  loadingReport: boolean;
  onSetReportKeywords: (keywords: string[]) => void;
  onRegenerateReport: () => void;
}

export const KeywordControls = memo<KeywordControlsProps>(({ 
  reportKeywords, 
  hasReport, 
  loadingReport, 
  onSetReportKeywords, 
  onRegenerateReport 
}) => {
  const handleKeywordsChange = useCallback((keywords: string[]) => {
    onSetReportKeywords(keywords);
  }, [onSetReportKeywords]);

  return (
    <Card 
      size="small" 
      className="keyword-controls-card"
      title={
        <Space>
          <FilterOutlined style={{ color: '#52c41a' }} />
          <Text strong>Palabras Clave</Text>
          {!hasReport && (
            <Tag color="orange" style={{ fontSize: '11px' }}>
              Sin resultados
            </Tag>
          )}
        </Space>
      }
      style={{ marginBottom: 16 }}
    >
      <Row gutter={[16, 12]} align="middle">
        <Col xs={24} sm={16} md={18}>
          <Space direction="vertical" style={{ width: '100%' }} size={4}>
            <Text strong style={{ fontSize: '13px' }}>Seleccionar palabras clave:</Text>
            <OptimizedKeywordSelect
              value={reportKeywords}
              onChange={handleKeywordsChange}
              placeholder="Seleccionar palabras clave"
              disabled={loadingReport}
            />
            <Text type="secondary" style={{ fontSize: '11px' }}>
              {!hasReport ? (
                <span style={{ color: '#fa8c16' }}>
                  ⚠️ Sin resultados. Intenta con diferentes palabras clave.
                </span>
              ) : (
                'Selecciona una o más palabras clave para el análisis.'
              )}
            </Text>
          </Space>
        </Col>
        <Col xs={24} sm={8} md={6}>
          <Button
            icon={<SyncOutlined />}
            onClick={onRegenerateReport}
            type="primary"
            block
            size="middle"
            disabled={reportKeywords.length === 0 || loadingReport}
            loading={loadingReport}
          >
            {loadingReport ? 'Generando...' : 'Actualizar Reporte'}
          </Button>
        </Col>
      </Row>
    </Card>
  );
});

KeywordControls.displayName = 'KeywordControls';
