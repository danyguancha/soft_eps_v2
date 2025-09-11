// components/report/ReportTable.tsx - VERSION FINAL COMPLETA

import React, { memo, useMemo, useCallback } from 'react';
import { Table, Typography, Tag, Space, Button, Tooltip, Empty } from 'antd';
import { CalendarOutlined, ExpandAltOutlined, CompressOutlined } from '@ant-design/icons';
import { getKeywordConfig } from '../../../config/reportKeywords.config';
import type { KeywordAgeItem } from '../../../services/TechnicalNoteService';

const { Text } = Typography;

// ✅ FUNCIÓN DE NORMALIZACIÓN ROBUSTA (mantiene acentos, limpia espacios)
const normalizeSpaces = (str: string): string => {
  return str.trim().replace(/\s+/g, ' ');
};

const buildCleanColumnKey = (column: string, keyword: string, ageRange: string): string => {
  return `${normalizeSpaces(column)}|${normalizeSpaces(keyword)}|${normalizeSpaces(ageRange)}`;
};

interface ReportTableProps {
  keywordReport: {
    items: KeywordAgeItem[];
    totals_by_keyword: Record<string, number>;
    temporal_data?: Record<string, any>;
  } | null;
  showTemporalData: boolean;
}

export const ReportTable = memo<ReportTableProps>(({ keywordReport, showTemporalData }) => {
  
  // ✅ CREAR MAPA NORMALIZADO DE CLAVES
  const normalizedTemporalData = useMemo(() => {
    if (!keywordReport?.temporal_data) return {};
    
    const normalized: Record<string, any> = {};
    
    Object.entries(keywordReport.temporal_data).forEach(([originalKey, data]) => {
      // Normalizar clave del backend también
      const parts = originalKey.split('|');
      if (parts.length === 3) {
        const normalizedKey = buildCleanColumnKey(parts[0], parts[1], parts[2]);
        normalized[normalizedKey] = data;
        console.log('🔄 Clave normalizada:', { original: originalKey, normalized: normalizedKey });
      }
    });
    
    console.log('📚 Mapa normalizado creado:', Object.keys(normalized));
    return normalized;
  }, [keywordReport?.temporal_data]);

  // ✅ DEBUG: Mostrar claves y estados
  React.useEffect(() => {
    if (keywordReport?.temporal_data) {
      console.log('🔍 TODAS las claves recibidas:', Object.keys(keywordReport.temporal_data));
      console.log('🧹 TODAS las claves normalizadas:', Object.keys(normalizedTemporalData));
      
      Object.entries(normalizedTemporalData).forEach(([key, data]) => {
        console.log(`📋 ${key}:`, {
          type: (data as any).type,
          hasYears: !!(data as any).years,
          hasStates: !!(data as any).states,
          statesKeys: (data as any).states ? Object.keys((data as any).states) : 'N/A'
        });
      });
    }
  }, [keywordReport?.temporal_data, normalizedTemporalData]);

  // ✅ RENDERIZADO DE ESTADOS DE VACUNACIÓN
  const renderVaccinationStatesExpansion = useCallback((record: KeywordAgeItem, statesData: any) => {
    console.log('🏥 Renderizando estados de vacunación:', record.column, statesData);
    
    if (!statesData.states) {
      return (
        <div className="temporal-no-data">
          <Text type="secondary">No hay datos de estados disponibles</Text>
        </div>
      );
    }

    const statesRows: any[] = [];

    // Crear filas para cada estado
    Object.entries(statesData.states).forEach(([stateName, stateInfo]: [string, any]) => {
      statesRows.push({
        key: `state-${stateName}`,
        state_name: stateName,
        count: stateInfo.count,
        percentage: 0 // Se calculará después
      });
    });

    // Calcular porcentajes
    const totalCount = statesRows.reduce((sum, row) => sum + row.count, 0);
    statesRows.forEach(row => {
      row.percentage = totalCount > 0 ? (row.count / totalCount * 100) : 0;
    });

    const statesColumns = [
      {
        title: 'Estado',
        dataIndex: 'state_name',
        key: 'state_name',
        width: '40%',
        render: (text: string) => (
          <Space>
            <Tag color={text === 'Completo' ? 'green' : 'orange'}>
              {text === 'Completo' ? '✅' : '⏳'} {text.toUpperCase()}
            </Tag>
          </Space>
        )
      },
      {
        title: 'Cantidad',
        dataIndex: 'count',
        key: 'count',
        width: '30%',
        align: 'right' as const,
        render: (count: number) => (
          <Text strong className="vaccination-state-count">
            {count.toLocaleString()}
          </Text>
        )
      },
      {
        title: 'Porcentaje',
        dataIndex: 'percentage',
        key: 'percentage',
        width: '30%',
        align: 'right' as const,
        render: (percentage: number) => (
          <Text className="vaccination-state-percentage">
            {percentage.toFixed(1)}%
          </Text>
        )
      }
    ];

    return (
      <div className="vaccination-states-expanded-content">
        <div className="vaccination-states-expanded-header">
          <Space>
            <Tag color="purple">💉 VACUNACIÓN</Tag>
            <Text strong>{record.column}</Text>
          </Space>
        </div>

        <Table
          dataSource={statesRows}
          columns={statesColumns}
          pagination={false}
          size="small"
          className="vaccination-states-detail-table"
          rowClassName="vaccination-state-row"
        />
      </div>
    );
  }, []);

  // ✅ RENDERIZADO EXPANDIDO (fechas + estados)
  const expandedRowRender = useCallback((record: KeywordAgeItem) => {
    if (!showTemporalData) return null;

    // ✅ USAR CLAVE NORMALIZADA
    const cleanKey = buildCleanColumnKey(record.column, record.keyword, record.age_range);
    const temporalData = normalizedTemporalData[cleanKey];

    console.log('🔍 expandedRowRender:', { 
      record: record.column, 
      cleanKey, 
      found: !!temporalData,
      type: temporalData?.type 
    });

    if (!temporalData) {
      return (
        <div className="temporal-no-data">
          <Text type="secondary">No hay datos temporales disponibles para esta columna</Text>
        </div>
      );
    }

    // ✅ VERIFICAR SI SON ESTADOS DE VACUNACIÓN
    if (temporalData.type === 'states') {
      console.log('✅ Renderizando expansión de estados para:', record.column);
      return renderVaccinationStatesExpansion(record, temporalData);
    }

    // ✅ LÓGICA EXISTENTE PARA DATOS TEMPORALES CON FECHAS
    if (!temporalData.years) {
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
  }, [normalizedTemporalData, showTemporalData, renderVaccinationStatesExpansion]);

  // ✅ COLUMNAS DE LA TABLA PRINCIPAL
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

  // ✅ TABLA PRINCIPAL CON EXPANDIBLE
  return (
    <Table
      dataSource={keywordReport?.items || []}
      columns={reportColumns}
      rowKey={(record: KeywordAgeItem) => `${record.column}-${record.keyword}-${record.age_range}`}
      expandable={{
        expandedRowRender: showTemporalData ? expandedRowRender : undefined,
        expandIcon: ({ expanded, onExpand, record }) => {
          if (!showTemporalData) return null;
          
          // ✅ USAR CLAVE NORMALIZADA
          const cleanKey = buildCleanColumnKey(record.column, record.keyword, record.age_range);
          const temporalData = normalizedTemporalData[cleanKey];

          console.log('🔍 expandIcon:', { 
            column: record.column, 
            cleanKey, 
            found: !!temporalData,
            type: temporalData?.type 
          });

          if (!temporalData) {
            console.log('❌ No temporal data para:', record.column);
            return null;
          }

          let hasData = false;
          let isStates = false;

          if (temporalData.type === 'states') {
            isStates = true;
            hasData = !!(temporalData.states && Object.keys(temporalData.states).length > 0);
            console.log('📊 Estados detectados para expand:', record.column, 'hasData:', hasData);
          } else {
            hasData = !!(temporalData.years && Object.keys(temporalData.years).length > 0);
            console.log('📅 Fechas detectadas para expand:', record.column, 'hasData:', hasData);
          }

          if (!hasData) {
            console.log('❌ No hay datos expandibles para:', record.column);
            return null;
          }

          // ✅ ESTILOS DIFERENCIADOS
          const iconStyle = isStates ? 
            { color: '#722ed1' } : // Púrpura para estados
            { color: '#1890ff' };   // Azul para fechas

          const tooltip = expanded ? 
            'Contraer desglose' : 
            isStates ? 
              'Expandir estados de vacunación' : 
              'Expandir desglose temporal';

          console.log('✅ Mostrando botón expand para:', record.column, 'tipo:', isStates ? 'estados' : 'fechas');

          return (
            <Button
              type="text"
              size="small"
              icon={expanded ? <CompressOutlined /> : <ExpandAltOutlined />}
              onClick={e => onExpand(record, e)}
              title={tooltip}
              className="temporal-expand-button"
              style={iconStyle}
            />
          );
        },
        rowExpandable: (record) => {
          if (!showTemporalData) return false;
          
          // ✅ USAR CLAVE NORMALIZADA
          const cleanKey = buildCleanColumnKey(record.column, record.keyword, record.age_range);
          const temporalData = normalizedTemporalData[cleanKey];
          
          if (!temporalData) return false;
          
          const expandable = temporalData.type === 'states' ?
            !!(temporalData.states && Object.keys(temporalData.states).length > 0) :
            !!(temporalData.years && Object.keys(temporalData.years).length > 0);
            
          console.log('🔄 rowExpandable para:', record.column, 'expandable:', expandable);
          return expandable;
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
