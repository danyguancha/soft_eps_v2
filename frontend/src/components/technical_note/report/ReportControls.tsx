// components/report/ReportControls.tsx
import { memo } from 'react';
import { Space, Button, Statistic, Switch, Typography } from 'antd';
import { EyeOutlined, EyeInvisibleOutlined, BarChartOutlined, CalendarOutlined } from '@ant-design/icons';

const { Text } = Typography;

interface ReportControlsProps {
  hasReport: boolean;
  reportTotalRecords: number;
  showTemporalData: boolean;
  showReport: boolean;
  onSetShowTemporalData: (show: boolean) => void;
  onToggleReportVisibility: () => void;
}

export const ReportControls = memo<ReportControlsProps>(({
  hasReport,
  reportTotalRecords,
  showTemporalData,
  showReport,
  onSetShowTemporalData,
  onToggleReportVisibility
}) => (
  <Space className="temporal-report-controls">
    {hasReport && (
      <>
        <Statistic
          title="Total Atenciones"
          value={reportTotalRecords}
          valueStyle={{ fontSize: '14px' }}
          prefix={<BarChartOutlined />}
          className="temporal-total-statistic"
        />
        <Space direction="vertical" size="small" className="temporal-switch-container">
          <Text className="temporal-switch-label">Desglose</Text>
          <Switch
            checked={showTemporalData}
            onChange={onSetShowTemporalData}
            size="small"
            checkedChildren={<CalendarOutlined />}
            unCheckedChildren={<CalendarOutlined />}
          />
        </Space>
      </>
    )}
    <Button
      icon={showReport ? <EyeInvisibleOutlined /> : <EyeOutlined />}
      onClick={onToggleReportVisibility}
      type={showReport ? 'default' : 'primary'}
      size="small"
      className="temporal-visibility-button"
    >
      {showReport ? 'Ocultar' : 'Mostrar'}
    </Button>
  </Space>
));

ReportControls.displayName = 'ReportControls';
