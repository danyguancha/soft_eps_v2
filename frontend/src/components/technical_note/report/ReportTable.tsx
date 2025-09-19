// components/technical-note/report/ReportTable.tsx - ✅ CON MANEJO DE "SIN ESPECIFICAR"
import React, { memo, useMemo, useCallback } from 'react';
import { Table, Typography, Tag, Space, Button, Tooltip, Empty } from 'antd';
import { CalendarOutlined, ExpandAltOutlined, CompressOutlined } from '@ant-design/icons';
import type { KeywordAgeReportItem } from '../../../services/TechnicalNoteService';
import type { ColumnsType } from 'antd/es/table';

const { Text } = Typography;

// ✅ FUNCIÓN DE NORMALIZACIÓN ROBUSTA
const normalizeSpaces = (str: string): string => {
  return str.trim().replace(/\s+/g, ' ');
};

// ✅ ACTUALIZADA: Manejo de "Sin especificar"
const normalizeAgeRange = (ageRange: string): string => {
  if (ageRange === 'Sin especificar') {
    return 'Sin especificar';
  }
  
  return ageRange
    .replace(/months?/gi, 'meses')
    .replace(/years?/gi, 'años');
};

const buildCleanColumnKey = (column: string, keyword: string, ageRange: string): string => {
  const normalizedAgeRange = normalizeAgeRange(ageRange);
  return `${normalizeSpaces(column)}|${normalizeSpaces(keyword)}|${normalizedAgeRange}`;
};

// ✅ NUEVA: Función para encontrar datos temporales con fallback
const findTemporalData = (temporalData: Record<string, any>, column: string, keyword: string, ageRange: string) => {
  // Intentar clave con rango normalizado
  const normalizedKey = buildCleanColumnKey(column, keyword, ageRange);
  if (temporalData[normalizedKey]) {
    console.log(`✅ Encontrado con clave normalizada: "${normalizedKey}"`);
    return { data: temporalData[normalizedKey], key: normalizedKey };
  }
  
  // ✅ Intentar con "Sin especificar" si no encontramos con el rango específico
  const fallbackKey = buildCleanColumnKey(column, keyword, 'Sin especificar');
  if (temporalData[fallbackKey]) {
    console.log(`✅ Encontrado con clave fallback "Sin especificar": "${fallbackKey}"`);
    return { data: temporalData[fallbackKey], key: fallbackKey };
  }
  
  // ✅ Buscar por coincidencia parcial (para casos edge)
  const partialMatches = Object.keys(temporalData).filter(key => 
    key.includes(column) && key.includes(keyword)
  );
  
  if (partialMatches.length > 0) {
    console.log(`🔍 Usando coincidencia parcial para "${column}":`, partialMatches[0]);
    return { data: temporalData[partialMatches[0]], key: partialMatches[0] };
  }
  
  console.log(`❌ No encontrado para "${column}" con ninguna clave`);
  return null;
};

// ✅ FUNCIÓN AUXILIAR PARA OBTENER COLOR DE KEYWORD
const getKeywordColor = (keyword: string): string => {
  const colors: Record<string, string> = {
    'medicina': 'blue',
    'enfermeria': 'green',
    'odontologia': 'purple',
    'psicologia': 'orange',
    'nutricion': 'cyan',
    'fisioterapia': 'magenta',
    'vacunacion': 'geekblue',
    'crecimiento': 'gold',
    'desarrollo': 'lime'
  };
  return colors[keyword.toLowerCase()] || 'default';
};

interface ReportTableProps {
  keywordReport: {
    items: KeywordAgeReportItem[];
    totals_by_keyword: Record<string, any>;
    temporal_data?: Record<string, any>;
    global_statistics?: any;
  } | null;
  showTemporalData: boolean;
}

export const ReportTable = memo<ReportTableProps>(({ keywordReport, showTemporalData }) => {
  
  // ✅ VERIFICACIONES TEMPRANAS DE SEGURIDAD
  if (!keywordReport) {
    return (
      <div style={{ padding: 24, textAlign: 'center' }}>
        <Empty 
          description="No hay datos de reporte disponibles"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      </div>
    );
  }

  if (!keywordReport.items || keywordReport.items.length === 0) {
    return (
      <div style={{ padding: 24, textAlign: 'center' }}>
        <Empty 
          description="No se encontraron elementos en el reporte"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      </div>
    );
  }

  // ✅ DEBUGGING: Verificar datos temporales
  React.useEffect(() => {
    console.log('🔍 === DEBUGGING TEMPORAL DATA CON FALLBACK ===');
    console.log('showTemporalData:', showTemporalData);
    console.log('temporal_data keys:', keywordReport?.temporal_data ? Object.keys(keywordReport.temporal_data) : 'NO DATA');
    
    if (keywordReport?.items && keywordReport.items.length > 0) {
      console.log('Total items:', keywordReport.items.length);
      
      // ✅ DEBUGGING: Probar findTemporalData para cada item
      keywordReport.items.forEach(item => {
        const result = findTemporalData(
          keywordReport.temporal_data || {}, 
          item.column || '', 
          item.keyword || '', 
          item.age_range || ''
        );
        console.log(`🔧 BÚSQUEDA TEMPORAL para "${item.column}":`, {
          age_range_original: item.age_range,
          result: !!result,
          key_used: result?.key || 'NINGUNA'
        });
      });
    }
  }, [keywordReport, showTemporalData]);

  // ✅ USAR DIRECTAMENTE LOS DATOS TEMPORALES
  const temporalData = keywordReport?.temporal_data || {};

  // ✅ VERIFICAR SI HAY DATOS NUMERADOR/DENOMINADOR
  const hasNumeradorDenominador = useMemo(() => {
    if (!keywordReport?.items || keywordReport.items.length === 0) return false;
    
    return keywordReport.items.some((item: KeywordAgeReportItem) => 
      item.numerador !== undefined && item.denominador !== undefined
    );
  }, [keywordReport?.items]);

  // ✅ RENDERIZADO EXPANDIDO TEMPORAL ACTUALIZADO
  const expandedRowRender = useCallback((record: KeywordAgeReportItem) => {
    if (!showTemporalData) return null;

    // ✅ USAR FUNCIÓN DE BÚSQUEDA CON FALLBACK
    const result = findTemporalData(
      temporalData, 
      record.column || '', 
      record.keyword || '', 
      record.age_range || ''
    );

    console.log('🔍 expandedRowRender:', { 
      record: record.column, 
      found: !!result,
      key_used: result?.key,
      hasYears: !!result?.data?.years 
    });

    if (!result || !result.data || !result.data.years) {
      return (
        <div style={{ padding: 16, textAlign: 'center', color: '#999' }}>
          <CalendarOutlined style={{ fontSize: 20, marginBottom: 8 }} />
          <div>No hay datos temporales disponibles para esta columna</div>
          <div style={{ fontSize: '11px', marginTop: 4 }}>
            Columna: {record.column}
          </div>
        </div>
      );
    }

    const recordTemporalData = result.data;
    const temporalRows: any[] = [];
    const sortedYears = Object.keys(recordTemporalData.years).sort((a, b) => parseInt(b) - parseInt(a));

    sortedYears.forEach(yearKey => {
      const yearData = recordTemporalData.years[yearKey];

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
            style={{ color: record.is_year ? '#1890ff' : '#52c41a' }}
          >
            {count.toLocaleString()}
          </Text>
        )
      }
    ];

    return (
      <div style={{ padding: 16, background: '#fafafa' }}>
        <div style={{ marginBottom: 12 }}>
          <Space>
            <CalendarOutlined style={{ color: '#1890ff' }} />
            <Text strong>📅 Análisis Temporal: {record.column}</Text>
            {result.key.includes('Sin especificar') && (
              <Tag color="orange" >Rango no específico</Tag>
            )}
          </Space>
        </div>

        <Table
          dataSource={temporalRows}
          columns={temporalColumns}
          pagination={false}
          size="small"
          rowClassName={(record) => record.is_year ? 'temporal-year-row' : 'temporal-month-row'}
        />
      </div>
    );
  }, [temporalData, showTemporalData]);

  // ✅ RESTO DE COLUMNAS (sin cambios)
  const reportColumns: ColumnsType<KeywordAgeReportItem> = useMemo(() => {
    const baseColumns: ColumnsType<KeywordAgeReportItem> = [
      {
        title: 'Columna',
        dataIndex: 'column',
        key: 'column',
        width: 300,
        ellipsis: true,
        render: (text: string) => (
          <Tooltip title={text}>
            <div style={{ 
              fontWeight: 500,
              color: '#1f1f1f',
              lineHeight: 1.3
            }}>
              {text && text.length > 50 ? `${text.substring(0, 50)}...` : text || ''}
            </div>
          </Tooltip>
        ),
        sorter: (a, b) => (a.column || '').localeCompare(b.column || ''),
      },
      {
        title: 'Palabra Clave',
        dataIndex: 'keyword',
        key: 'keyword',
        width: 120,
        render: (keyword: string) => (
          <Tag color={getKeywordColor(keyword || '')} style={{ fontWeight: 500 }}>
            {(keyword || '').toUpperCase()}
          </Tag>
        ),
      },
      
    ];

    // ✅ AGREGAR COLUMNAS NUMERADOR/DENOMINADOR SI EXISTEN
    if (hasNumeradorDenominador) {
      baseColumns.push(
        {
          title: (
            <Tooltip title="Población total en el rango de edad específico">
              <span>Denominador</span>
            </Tooltip>
          ),
          dataIndex: 'denominador',
          key: 'denominador',
          width: 120,
          align: 'right',
          render: (value: number) => (
            <Text strong style={{ color: '#1890ff' }}>
              {value ? value.toLocaleString() : '-'}
            </Text>
          ),
          sorter: (a, b) => (a.denominador || 0) - (b.denominador || 0),
        },
        {
          title: (
            <Tooltip title="Población con datos registrados">
              <span>Numerador</span>
            </Tooltip>
          ),
          dataIndex: 'numerador',
          key: 'numerador',
          width: 120,
          align: 'right',
          render: (value: number) => (
            <Text strong style={{ color: '#52c41a' }}>
              {value ? value.toLocaleString() : '-'}
            </Text>
          ),
          sorter: (a, b) => (a.numerador || 0) - (b.numerador || 0),
        }
      );
    }

    // ✅ COLUMNA DE PORCENTAJE
    baseColumns.push({
      title: '% de cumplimiento',
      key: 'percentage',
      width: 80,
      align: 'right',
      render: (_, record: KeywordAgeReportItem) => {
        if (hasNumeradorDenominador && record.cobertura_porcentaje !== undefined) {
          const color = record.cobertura_porcentaje >= 70 ? '#52c41a' : 
                       record.cobertura_porcentaje >= 50 ? '#fa8c16' : '#ff4d4f';
          return (
            <Text strong style={{ color }}>
              {record.cobertura_porcentaje.toFixed(1)}%
            </Text>
          );
        }
        
        if (!keywordReport?.totals_by_keyword) return '0.0%';

        const totalRecords = Object.values(keywordReport.totals_by_keyword).reduce(
          (acc: number, curr: any) => acc + (curr.count || curr || 0),
          0
        );

        const recordCount = record.count || 0;
        const percentage = totalRecords > 0 ? (recordCount / totalRecords) * 100 : 0;
        return (
          <Text style={{ color: '#999' }}>
            {percentage.toFixed(1)}%
          </Text>
        );
      }
    });

    return baseColumns;
  }, [keywordReport?.totals_by_keyword, hasNumeradorDenominador]);

  const getRowKey = (record: KeywordAgeReportItem) => 
    `${record.column || 'no-column'}-${record.keyword || 'no-keyword'}-${record.age_range || 'no-age'}`;

  return (
    <div style={{ marginTop: 24 }}>
      

      {/* ✅ TABLA PRINCIPAL CON EXPANDIBLE ACTUALIZADO */}
      <Table
        dataSource={keywordReport.items}
        columns={reportColumns}
        rowKey={getRowKey}
        size="small"
        scroll={{ x: hasNumeradorDenominador ? 1400 : 800, y: 400 }}
        
        expandable={showTemporalData ? {
          expandedRowRender: expandedRowRender,
          expandIcon: ({ expanded, onExpand, record }) => {
            // ✅ USAR FUNCIÓN DE BÚSQUEDA CON FALLBACK
            const result = findTemporalData(
              temporalData, 
              record.column || '', 
              record.keyword || '', 
              record.age_range || ''
            );

            console.log('🔍 expandIcon:', { 
              column: record.column, 
              found: !!result,
              key_used: result?.key,
              hasYears: !!result?.data?.years 
            });

            if (!result || !result.data || !result.data.years) {
              return <span style={{ width: 16, display: 'inline-block' }}></span>;
            }

            const hasData = Object.keys(result.data.years).length > 0;

            if (!hasData) {
              return <span style={{ width: 16, display: 'inline-block' }}></span>;
            }

            console.log('✅ Mostrando botón expand para:', record.column);

            return (
              <Button
                type="text"
                size="small"
                icon={expanded ? <CompressOutlined /> : <ExpandAltOutlined />}
                onClick={(e) => {
                  console.log('🔍 Expand clicked for:', record.column);
                  onExpand(record, e);
                }}
                title={expanded ? 'Contraer desglose' : 'Expandir desglose temporal'}
                style={{ border: 'none', padding: 0, minWidth: 16, color: '#1890ff' }}
              />
            );
          },
          expandIconColumnIndex: 0,
          rowExpandable: (record) => {
            const result = findTemporalData(
              temporalData, 
              record.column || '', 
              record.keyword || '', 
              record.age_range || ''
            );
            
            const expandable = !!(result && result.data && result.data.years && Object.keys(result.data.years).length > 0);
            
            console.log('🔄 rowExpandable para:', record.column, 'expandable:', expandable);
            return expandable;
          }
        } : undefined}
        
        pagination={{
          pageSize: 15,
          showSizeChanger: true,
          showQuickJumper: true,
          showTotal: (total, range) =>
            `${range[0]}-${range[1]} de ${total} actividades`,
          size: 'small'
        }}
        locale={{
          emptyText: (
            <Empty
              description="No se encontraron columnas con los filtros especificados"
              image={Empty.PRESENTED_IMAGE_SIMPLE}
            />
          )
        }}
      />
    </div>
  );
});

ReportTable.displayName = 'ReportTable';
