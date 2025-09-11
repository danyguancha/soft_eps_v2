// components/report/ReportHeader.tsx
import { memo } from 'react';
import { Space, Tag } from 'antd';
import { BarChartOutlined, EnvironmentOutlined, ExclamationCircleOutlined } from '@ant-design/icons';

interface ReportHeaderProps {
  reportTitle: string;
  hasGeoFilters: boolean;
  geographicFilters: {
    departamento?: string | null;
    municipio?: string | null;
    ips?: string | null;
  };
  hasReport: boolean;
  loadingReport: boolean;
}

export const ReportHeader = memo<ReportHeaderProps>(({ 
  reportTitle, 
  hasGeoFilters, 
  geographicFilters, 
  hasReport, 
  loadingReport 
}) => (
  <Space className="temporal-report-title" wrap>
    <BarChartOutlined />
    <span>{reportTitle}</span>
    
    {hasGeoFilters && (
      <Tag color="blue" icon={<EnvironmentOutlined />}>
        {[geographicFilters.departamento, geographicFilters.municipio, geographicFilters.ips]
          .filter(Boolean).join(' → ')}
      </Tag>
    )}
    
    {!hasReport && !loadingReport && (
      <Tag color="orange" icon={<ExclamationCircleOutlined />}>
        Sin resultados
      </Tag>
    )}
  </Space>
));

ReportHeader.displayName = 'ReportHeader';
