// components/report/KeywordControls.tsx
import { memo, useCallback } from 'react';
import { Card, Space, Typography, Tag, Row, Col, Button } from 'antd';
import { FilterOutlined, SyncOutlined, CheckSquareOutlined, CloseSquareOutlined } from '@ant-design/icons';
import { OptimizedKeywordSelect } from './OptimizedKeywordSelect';
import './keyword_controls.css';


const { Text } = Typography;


const ALL_KEYWORDS = [
  'medicina',
  'enfermeria',
  'odontológica',
  'fluor',
  'placa',
  'detartraje',
  'sellantes',
  'micronutrientes',
  'vitamina_a',
  'sulfato_ferroso',
  'diu',
  'subdermico',
  'asesoria_pre_test_vih',
  'asesoria_post_test_vih',
  'tamizaje_vih',
  'lactancia_materna',
  'hepatitis_b',
  'hepatitis_c',
  'hematocrito',
  'hemoglobina',
  'educacion_individual',
  'desparasitacion',
  'citologia',
  'adn-vph',
  'suministro_preservativos',
  'anticonceptivo_oral',
  'anticonceptivo_inyectable_mensual',
  'anticonceptivo_inyectable_trimestral',
  'sifilis',
  'cancer_mamografia',
  'valoracion_clinica_mama',
  'antigeno_prostatico',
  'cancer_colon_sangre_oculta',
  'prueba_embarazo',
];


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

  const handleSelectAll = useCallback(() => {
    onSetReportKeywords([...ALL_KEYWORDS]);
  }, [onSetReportKeywords]);

  const handleClearAll = useCallback(() => {
    onSetReportKeywords([]);
  }, [onSetReportKeywords]);

  const allSelected = ALL_KEYWORDS.length === reportKeywords.length &&
    ALL_KEYWORDS.every(k => reportKeywords.includes(k));


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
            <Space style={{ width: '100%', justifyContent: 'space-between' }} wrap={false}>
              <Text strong style={{ fontSize: '13px' }}>
                Seleccionar palabras clave:
              </Text>
              <Space size={4}>
                <Button
                  size="small"
                  icon={<CheckSquareOutlined />}
                  onClick={handleSelectAll}
                  disabled={loadingReport || allSelected}
                  type="link"
                  style={{ fontSize: '14px', padding: '0 4px' }}
                >
                  Seleccionar todos
                </Button>
                <Text type="secondary" style={{ fontSize: '14px' }}>|</Text>
                <Button
                  size="small"
                  icon={<CloseSquareOutlined />}
                  onClick={handleClearAll}
                  disabled={loadingReport || reportKeywords.length === 0}
                  type="link"
                  danger
                  style={{ fontSize: '12px', padding: '0 4px' }}
                >
                  Limpiar
                </Button>
              </Space>
            </Space>
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
                `${reportKeywords.length} de ${ALL_KEYWORDS.length} palabras clave seleccionadas.`
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
