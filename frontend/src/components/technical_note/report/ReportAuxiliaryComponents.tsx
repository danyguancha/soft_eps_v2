// components/report/ReportAuxiliaryComponents.tsx
import { memo } from 'react';
import { Spin, Alert, Button, Empty, Typography } from 'antd';
import { BarChartOutlined } from '@ant-design/icons';

const { Text } = Typography;

export const ReportLoading = memo(() => (
  <div className="temporal-loading-container">
    <Spin size="large" />
    <div className="temporal-loading-text">
      <Text>Generando reporte con filtros geográficos...</Text>
    </div>
  </div>
));

export const NoResultsAlert = memo<{ onRetry: () => void; loading: boolean }>(({ onRetry, loading }) => (
  <Alert
    message="No se encontraron resultados"
    description="No se encontraron columnas que coincidan con las palabras clave y filtros geográficos seleccionados. Intenta cambiar los filtros."
    type="warning"
    showIcon
    style={{ marginBottom: 16 }}
    action={
      <Button size="small" onClick={onRetry} loading={loading}>
        Intentar de nuevo
      </Button>
    }
  />
));

export const NoReportState = memo<{ 
  onGenerateReport: () => void; 
  reportKeywords: string[]; 
  loadingReport: boolean 
}>(({ onGenerateReport, reportKeywords, loadingReport }) => (
  <div className="temporal-no-report">
    <Empty
      description="No se encontraron datos con los filtros seleccionados"
      image={Empty.PRESENTED_IMAGE_SIMPLE}
    >
      <Text type="secondary" style={{ display: 'block', marginBottom: 16 }}>
        Cambia los filtros geográficos o palabras clave e intenta nuevamente.
      </Text>
      <Button
        type="primary"
        icon={<BarChartOutlined />}
        onClick={onGenerateReport}
        className="temporal-generate-button"
        disabled={reportKeywords.length === 0}
        loading={loadingReport}
      >
        {loadingReport ? 'Generando...' : 'Generar Reporte'}
      </Button>
    </Empty>
  </div>
));

ReportLoading.displayName = 'ReportLoading';
NoResultsAlert.displayName = 'NoResultsAlert';
NoReportState.displayName = 'NoReportState';
