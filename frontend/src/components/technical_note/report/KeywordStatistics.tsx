// components/report/KeywordStatistics.tsx
import React, { memo } from 'react';
import { Row, Col, Card, Statistic } from 'antd';
import { BarChartOutlined } from '@ant-design/icons';

interface KeywordStat {
  keyword: string;
  total: number;
  itemsCount: number;
  config?: {
    label?: string;
    color?: string;
    icon?: React.ReactNode;
  };
}

interface KeywordStatisticsProps {
  stats: KeywordStat[];
}

export const KeywordStatistics = memo<KeywordStatisticsProps>(({ stats }) => {
  if (!stats.length) return null;

  return (
    <Row gutter={16} className="temporal-stats-row">
      {stats.map((stat) => (
        <Col span={Math.max(6, Math.floor(24 / stats.length))} key={stat.keyword}>
          <Card
            size="small"
            className={`temporal-stat-card temporal-stat-${stat.keyword}`}
            style={{ borderColor: stat.config?.color || '#d9d9d9' }}
          >
            <Statistic
              title={(stat.config?.label || stat.keyword).toUpperCase()}
              value={stat.total}
              suffix="Atenciones"
              valueStyle={{
                color: stat.config?.color || '#595959',
                fontSize: '16px'
              }}
              prefix={stat.config?.icon || <BarChartOutlined />}
              className="temporal-keyword-statistic"
            />
          </Card>
        </Col>
      ))}
    </Row>
  );
});

KeywordStatistics.displayName = 'KeywordStatistics';
