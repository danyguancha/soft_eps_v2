// components/report/ReportTable.tsx
import { memo, useMemo, useCallback } from 'react';
import { Table, Typography, Tag, Space, Button, Tooltip, Empty } from 'antd';
import { CalendarOutlined, ExpandAltOutlined, CompressOutlined } from '@ant-design/icons';
import { getKeywordConfig } from '../../../config/reportKeywords.config';
import type { KeywordAgeItem } from '../../../services/TechnicalNoteService';

const { Text } = Typography;

interface ReportTableProps {
  keywordReport: {
    items: KeywordAgeItem[];
    totals_by_keyword: Record<string, number>;
    temporal_data?: Record<string, any>;
  } | null;
  showTemporalData: boolean;
}

export const ReportTable = memo<ReportTableProps>(({ keywordReport, showTemporalData }) => {
  const expandedRowRender = useCallback((record: KeywordAgeItem) => {
    if (!keywordReport?.temporal_data || !showTemporalData) return null;

    const columnKey = `${record.column}|${record.keyword}|${record.age_range}`;
    const temporalData = keywordReport.temporal_data[columnKey];

    if (!temporalData || !temporalData.years) {
      return (
        <div className="temporal-no-data">
          <Text type="secondary">No hay datos temporales disponibles para esta columna</Text>
        </div>
      );
    }

    const temporalRows: any[] = [];
    const sortedYears = Object.keys(temporalData.years).sort((a, b) => parseInt(b) - parseInt(a));

    sortedYears.forEach(yearKey => {
      const yearData = temporalData.years[yearKey];

      temporalRows.push({
        key: `year-${yearKey}`,
        period_type: 'año',
        period: yearKey,
        count: yearData.total,
        is_year: true
      });

      if (yearData.months) {
        const sortedMonths = Object.entries(yearData.months).sort(([, a], [, b]) => 
          (a as any).month - (b as any).month
        );

        sortedMonths.forEach(([monthName, monthData]) => {
          temporalRows.push({
            key: `month-${yearKey}-${(monthData as any).month}`,
            period_type: 'mes',
            period: `${monthName} ${yearKey}`,
            count: (monthData as any).count,
            is_year: false,
            parent_year: yearKey
          });
        });
      }
    });

    const temporalColumns = [
      {
        title: 'Período',
        dataIndex: 'period',
        key: 'period',
        width: '60%',
        render: (text: string, record: any) => (
          <Space>
            {record.is_year ? (
              <Tag color="blue" icon={<CalendarOutlined />}>AÑO</Tag>
            ) : (
              <Tag color="green">MES</Tag>
            )}
            <Text strong={record.is_year}>{text}</Text>
          </Space>
        )
      },
      {
        title: 'Registros',
        dataIndex: 'count',
        key: 'count',
        width: '40%',
        align: 'right' as const,
        render: (count: number, record: any) => (
          <Text
            strong={record.is_year}
            className={record.is_year ? 'temporal-year-count' : 'temporal-month-count'}
          >
            {count.toLocaleString()}
          </Text>
        )
      }
    ];

    return (
      <div className="temporal-expanded-content">
        <div className="temporal-expanded-header">
          <Space>
            <CalendarOutlined style={{ color: '#1890ff' }} />
            <Text strong>{record.column}</Text>
          </Space>
        </div>

        <Table
          dataSource={temporalRows}
          columns={temporalColumns}
          pagination={false}
          size="small"
          rowClassName={(record) => record.is_year ? 'temporal-year-row' : 'temporal-month-row'}
          className="temporal-detail-table"
        />
      </div>
    );
  }, [keywordReport?.temporal_data, showTemporalData]);

  const reportColumns = useMemo(() => [
    {
      title: 'Columna',
      dataIndex: 'column',
      key: 'column',
      width: '65%',
      ellipsis: true,
      render: (text: string) => (
        <Tooltip title={text}>
          <Text className="temporal-column-name">{text}</Text>
        </Tooltip>
      )
    },
    {
      title: 'Palabra Clave',
      dataIndex: 'keyword',
      key: 'keyword',
      width: '15%',
      render: (keyword: string) => {
        const config = getKeywordConfig(keyword);
        return (
          <Tag
            color={config?.color || 'default'}
            icon={config?.icon}
            className="temporal-keyword-tag"
          >
            {(config?.label || keyword).toUpperCase()}
          </Tag>
        );
      }
    },
    {
      title: 'Total Atenciones',
      dataIndex: 'count',
      key: 'count',
      width: '15%',
      align: 'right' as const,
      render: (count: number) => {
        let color = '#595959';
        if (count > 500) color = '#52c41a';
        else if (count > 100) color = '#1890ff';
        else if (count > 50) color = '#faad14';

        return (
          <Space>
            <Text strong style={{ color }} className="temporal-count-text">
              {count.toLocaleString()}
            </Text>
          </Space>
        );
      },
      sorter: (a: KeywordAgeItem, b: KeywordAgeItem) => a.count - b.count
    },
    {
      title: '%',
      key: 'percentage',
      width: '20%',
      align: 'right' as const,
      render: (_: any, record: KeywordAgeItem) => {
        if (!keywordReport?.totals_by_keyword) return '-';
        
        // Corrección: calcular el total correctamente
        const total = Object.values(keywordReport.totals_by_keyword).reduce(
          (acc: number, curr: number) => acc + curr, 
          0
        );
        
        const percentage = total > 0 ? (record.count / total) * 100 : 0;
        return (
          <Text className="temporal-percentage-text">
            {percentage.toFixed(1)}%
          </Text>
        );
      }
    }
  ], [keywordReport]);

  return (
    <Table
      dataSource={keywordReport?.items || []}
      columns={reportColumns}
      rowKey={(record: KeywordAgeItem) => `${record.column}-${record.keyword}-${record.age_range}`}
      expandable={{
        expandedRowRender: showTemporalData ? expandedRowRender : undefined,
        expandIcon: ({ expanded, onExpand, record }) => {
          const columnKey = `${record.column}|${record.keyword}|${record.age_range}`;
          const hasTemporalData = keywordReport?.temporal_data?.[columnKey];

          if (!showTemporalData || !hasTemporalData) return null;

          return (
            <Button
              type="text"
              size="small"
              icon={expanded ? <CompressOutlined /> : <ExpandAltOutlined />}
              onClick={e => onExpand(record, e)}
              title={expanded ? 'Contraer desglose' : 'Expandir desglose'}
              className="temporal-expand-button"
            />
          );
        },
        rowExpandable: (record) => {
          if (!showTemporalData) return false;
          const columnKey = `${record.column}|${record.keyword}|${record.age_range}`;
          return !!keywordReport?.temporal_data?.[columnKey];
        }
      }}
      pagination={{
        pageSize: 15,
        showSizeChanger: true,
        showQuickJumper: true,
        showTotal: (total, range) =>
          `${range[0]}-${range[1]} de ${total} elementos`,
        size: 'small'
      }}
      size="small"
      scroll={{ x: 800, y: 400 }}
      virtual
      locale={{
        emptyText: (
          <Empty
            description="No se encontraron columnas con los filtros especificados"
            image={Empty.PRESENTED_IMAGE_SIMPLE}
          />
        )
      }}
      className="temporal-main-table"
    />
  );
});

ReportTable.displayName = 'ReportTable';
