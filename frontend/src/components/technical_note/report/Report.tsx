// components/technical-note/report/Report.tsx - ✅ CÓDIGO COMPLETO CORREGIDO
import React, { useMemo, useCallback, memo } from 'react';
import { Card, Typography, Row, Col, Spin, Space, Button, Table, Tag, Statistic, Select, Empty, Switch, Tooltip, Alert } from 'antd';
import { BarChartOutlined, EyeOutlined, EyeInvisibleOutlined, SyncOutlined, FilterOutlined, CalendarOutlined, ExpandAltOutlined, CompressOutlined, ExclamationCircleOutlined, EnvironmentOutlined, ClearOutlined } from '@ant-design/icons';
import type { KeywordAgeReport, KeywordAgeItem, GeographicFilters } from '../../../services/TechnicalNoteService';
import {
    SELECT_OPTIONS,
    getKeywordConfig,
    getKeywordLabel,
    DEFAULT_KEYWORDS
} from '../../../config/reportKeywords.config';
import './Report.css';

const { Text } = Typography;
const { Option } = Select;

// ✅ INTERFAZ COMPLETA CON FILTROS GEOGRÁFICOS
interface TemporalReportProps {
    keywordReport: KeywordAgeReport | null;
    loadingReport: boolean;
    showReport: boolean;
    hasReport: boolean;
    reportItemsCount: number;
    reportTotalRecords: number;
    selectedFile: string | null;
    reportKeywords: string[];
    reportMinCount: number;
    showTemporalData: boolean;
    // Filtros geográficos
    geographicFilters: GeographicFilters;
    departamentosOptions: string[];
    municipiosOptions: string[];
    ipsOptions: string[];
    loadingGeoFilters: {
        departamentos: boolean;
        municipios: boolean;
        ips: boolean;
    };
    // Handlers existentes
    onToggleReportVisibility: () => void;
    onRegenerateReport: () => void;
    onSetReportKeywords: (keywords: string[]) => void;
    onSetReportMinCount: (count: number) => void;
    onSetShowTemporalData: (show: boolean) => void;
    onLoadKeywordAgeReport: (
        filename: string, 
        keywords?: string[], 
        minCount?: number, 
        includeTemporal?: boolean,
        geographicFilters?: GeographicFilters
    ) => void;
    onAddKeyword: (keyword: string) => void;
    onRemoveKeyword: (keyword: string) => void;
    // Handlers geográficos
    onDepartamentoChange: (value: string | null) => void;
    onMunicipioChange: (value: string | null) => void;
    onIpsChange: (value: string | null) => void;
    resetGeographicFilters: () => void;
}

// ✅ FUNCIÓN DEBOUNCE OPTIMIZADA
function debounce<T extends (...args: any[]) => any>(
  func: T,
  wait: number
): (...args: Parameters<T>) => void {
  let timeout: NodeJS.Timeout;
  return (...args: Parameters<T>) => {
    clearTimeout(timeout);
    timeout = setTimeout(() => func(...args), wait);
  };
}

// ✅ SELECT OPTIMIZADO CON SCROLL VERTICAL PARA PALABRAS CLAVE
const OptimizedKeywordSelect = memo<{
  value: string[];
  onChange: (values: string[]) => void;
  placeholder?: string;
  disabled?: boolean;
}>(({ value, onChange, placeholder, disabled }) => {
  
  const debouncedOnChange = useCallback(
    debounce((values: string[]) => {
      onChange(values);
    }, 100),
    [onChange]
  );

  const handleChange = useCallback((values: string[]) => {
    if (JSON.stringify(values.sort()) !== JSON.stringify(value.sort())) {
      debouncedOnChange(values);
    }
  }, [debouncedOnChange, value]);

  return (
    <Select
      mode="multiple"
      value={value}
      onChange={handleChange}
      placeholder={placeholder}
      disabled={disabled}
      style={{ width: '100%' }}
      options={SELECT_OPTIONS}
      showSearch={true}
      allowClear={false}
      virtual={true}
      maxTagCount={3}
      maxTagTextLength={18}
      // ✅ PROPIEDADES ACTUALIZADAS ANT DESIGN 5.x
      optionFilterProp="label"  // Filtra por la prop 'label' automáticamente
      popupMatchSelectWidth={false}
      styles={{
        popup: {
          root: {
            minWidth: '280px',
            maxHeight: '240px',
            overflowY: 'auto'
          }
        }
      }}
      classNames={{
        popup: {
          root: 'keywords-select-dropdown'
        }
      }}
      listHeight={240}
      getPopupContainer={(triggerNode) => triggerNode.parentElement || document.body}
    />
  );
});

OptimizedKeywordSelect.displayName = 'OptimizedKeywordSelect';

// ✅ COMPONENTE DE FILTROS GEOGRÁFICOS CORREGIDO
const GeographicFilters = memo<{
  filters: GeographicFilters;
  options: {
    departamentos: string[];
    municipios: string[];
    ips: string[];
  };
  loading: {
    departamentos: boolean;
    municipios: boolean;
    ips: boolean;
  };
  onDepartamentoChange: (value: string | null) => void;
  onMunicipioChange: (value: string | null) => void;
  onIpsChange: (value: string | null) => void;
  onReset: () => void;
  disabled?: boolean;
}>(({ filters, options, loading, onDepartamentoChange, onMunicipioChange, onIpsChange, onReset, disabled = false }) => {

  const hasAnyFilter = filters.departamento || filters.municipio || filters.ips;

  return (
    <Card 
      size="small" 
      className="geographic-filters-card"
      title={
        <Space>
          <EnvironmentOutlined style={{ color: '#1890ff' }} />
          <Text strong>Filtros Geográficos</Text>
          {hasAnyFilter && (
            <Tag color="blue" style={{ fontSize: '11px' }}>
              Filtros activos
            </Tag>
          )}
        </Space>
      }
      extra={
        hasAnyFilter ? (
          <Button
            type="link"
            size="small"
            icon={<ClearOutlined />}
            onClick={onReset}
            disabled={disabled}
            style={{ fontSize: '11px', padding: '0 4px' }}
          >
            Limpiar filtros
          </Button>
        ) : null
      }
      style={{ marginBottom: 16 }}
    >
      <Row gutter={[16, 12]}>
        {/* ✅ SELECT DEPARTAMENTO */}
        <Col xs={24} sm={8} md={8}>
          <Space direction="vertical" style={{ width: '100%' }} size={4}>
            <Text strong style={{ fontSize: '13px' }}>Departamento:</Text>
            <Select
              placeholder="Seleccionar departamento"
              style={{ width: '100%' }}
              size="middle"
              value={filters.departamento}
              onChange={onDepartamentoChange}
              loading={loading.departamentos}
              disabled={disabled || loading.departamentos}
              allowClear
              showSearch
              filterOption={(input, option) => {
                if (option?.children) {
                  return String(option.children).toLowerCase().includes(input.toLowerCase());
                }
                if (option?.label) {
                  return String(option.label).toLowerCase().includes(input.toLowerCase());
                }
                return false;
              }}
              styles={{
                popup: {
                  root: {
                    maxHeight: '200px',
                    overflowY: 'auto'
                  }
                }
              }}
            >
              {options.departamentos.map(dept => (
                <Option key={dept} value={dept}>
                  {dept}
                </Option>
              ))}
            </Select>
          </Space>
        </Col>

        {/* ✅ SELECT MUNICIPIO */}
        <Col xs={24} sm={8} md={8}>
          <Space direction="vertical" style={{ width: '100%' }} size={4}>
            <Text strong style={{ fontSize: '13px' }}>Municipio:</Text>
            <Select
              placeholder={
                !filters.departamento 
                  ? "Selecciona departamento" 
                  : "Seleccionar municipio"
              }
              style={{ width: '100%' }}
              size="middle"
              value={filters.municipio}
              onChange={onMunicipioChange}
              loading={loading.municipios}
              disabled={disabled || !filters.departamento || loading.municipios}
              allowClear
              showSearch
              filterOption={(input, option) => {
                if (option?.children) {
                  return String(option.children).toLowerCase().includes(input.toLowerCase());
                }
                if (option?.label) {
                  return String(option.label).toLowerCase().includes(input.toLowerCase());
                }
                return false;
              }}
              styles={{
                popup: {
                  root: {
                    maxHeight: '200px',
                    overflowY: 'auto'
                  }
                }
              }}
            >
              {options.municipios.map(mun => (
                <Option key={mun} value={mun}>
                  {mun}
                </Option>
              ))}
            </Select>
            {!filters.departamento && (
              <Text type="secondary" style={{ fontSize: '11px' }}>
                Selecciona un departamento primero
              </Text>
            )}
          </Space>
        </Col>

        {/* ✅ SELECT IPS */}
        <Col xs={24} sm={8} md={8}>
          <Space direction="vertical" style={{ width: '100%' }} size={4}>
            <Text strong style={{ fontSize: '13px' }}>IPS:</Text>
            <Select
              placeholder={
                !filters.municipio 
                  ? "Selecciona municipio" 
                  : "Seleccionar IPS"
              }
              style={{ width: '100%' }}
              size="middle"
              value={filters.ips}
              onChange={onIpsChange}
              loading={loading.ips}
              disabled={disabled || !filters.municipio || loading.ips}
              allowClear
              showSearch
              filterOption={(input, option) => {
                if (option?.children) {
                  return String(option.children).toLowerCase().includes(input.toLowerCase());
                }
                if (option?.label) {
                  return String(option.label).toLowerCase().includes(input.toLowerCase());
                }
                return false;
              }}
              styles={{
                popup: {
                  root: {
                    maxHeight: '200px',
                    overflowY: 'auto'
                  }
                }
              }}
            >
              {options.ips.map(ips => (
                <Option key={ips} value={ips}>
                  {ips}
                </Option>
              ))}
            </Select>
            {!filters.municipio && (
              <Text type="secondary" style={{ fontSize: '11px' }}>
                Selecciona un municipio primero
              </Text>
            )}
          </Space>
        </Col>
      </Row>

      {/* ✅ RESUMEN DE FILTROS ACTIVOS */}
      {hasAnyFilter && (
        <Row style={{ marginTop: 12 }}>
          <Col span={24}>
            <Text type="secondary" style={{ fontSize: '12px' }}>
              <strong>Filtros aplicados:</strong> {[
                filters.departamento && `📍 ${filters.departamento}`,
                filters.municipio && `🏘️ ${filters.municipio}`,
                filters.ips && `🏥 ${filters.ips}`
              ].filter(Boolean).join(' → ')}
            </Text>
          </Col>
        </Row>
      )}
    </Card>
  );
});

GeographicFilters.displayName = 'GeographicFilters';

// ✅ COMPONENTE DE PALABRAS CLAVE
const KeywordControls = memo<{
  reportKeywords: string[];
  hasReport: boolean;
  loadingReport: boolean;
  onSetReportKeywords: (keywords: string[]) => void;
  onRegenerateReport: () => void;
}>(({ reportKeywords, hasReport, loadingReport, onSetReportKeywords, onRegenerateReport }) => {
  
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

// ✅ COMPONENTE PRINCIPAL COMPLETO
export const Report: React.FC<TemporalReportProps> = memo(({
    keywordReport,
    loadingReport,
    showReport,
    hasReport,
    reportTotalRecords,
    selectedFile,
    reportKeywords,
    reportMinCount,
    showTemporalData,
    geographicFilters,
    departamentosOptions,
    municipiosOptions,
    ipsOptions,
    loadingGeoFilters,
    onToggleReportVisibility,
    onSetReportKeywords,
    onSetShowTemporalData,
    onLoadKeywordAgeReport,
    onDepartamentoChange,
    onMunicipioChange,
    onIpsChange,
    resetGeographicFilters,
}) => {

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

            const sortedMonths = Object.entries(yearData.months).sort(([, a], [, b]) => a.month - b.month);

            sortedMonths.forEach(([monthName, monthData]) => {
                temporalRows.push({
                    key: `month-${yearKey}-${monthData.month}`,
                    period_type: 'mes',
                    period: `${monthName} ${yearKey}`,
                    count: monthData.count,
                    is_year: false,
                    parent_year: yearKey
                });
            });
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

    // ✅ COLUMNAS MEMOIZADAS
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
            render: (count: number, record: KeywordAgeItem) => {
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
                if (!keywordReport) return '-';
                const total = Object.values(keywordReport.totals_by_keyword).reduce((a, b) => a + b, 0);
                const percentage = total > 0 ? (record.count / total) * 100 : 0;
                return (
                    <Text className="temporal-percentage-text">
                        {percentage.toFixed(1)}%
                    </Text>
                );
            }
        }
    ], [keywordReport]);

    // ✅ ESTADÍSTICAS MEMOIZADAS
    const keywordStats = useMemo(() => {
        if (!keywordReport || !keywordReport.totals_by_keyword) return [];

        return Object.entries(keywordReport.totals_by_keyword).map(([keyword, total]) => ({
            keyword,
            total,
            itemsCount: keywordReport.items.filter(item => item.keyword === keyword).length,
            config: getKeywordConfig(keyword)
        }));
    }, [keywordReport]);

    // ✅ TÍTULO ACTUALIZADO CON INFORMACIÓN GEOGRÁFICA
    const reportTitle = useMemo(() => {
        let baseTitle = "Reporte";
        
        if (reportKeywords && reportKeywords.length > 0) {
            const keywordNames = reportKeywords.map(k => getKeywordLabel(k));
            
            if (keywordNames.length === 1) {
                baseTitle = `Reporte: ${keywordNames[0]}`;
            } else if (keywordNames.length === 2) {
                baseTitle = `Reporte: ${keywordNames.join(' y ')}`;
            } else {
                baseTitle = `Reporte: ${keywordNames.slice(0, -1).join(', ')} y ${keywordNames[keywordNames.length - 1]}`;
            }
        }

        return baseTitle;
    }, [reportKeywords]);

    // ✅ CALLBACKS MEMOIZADOS
    const handleLoadReport = useCallback(() => {
        if (selectedFile) {
            onLoadKeywordAgeReport(
                selectedFile, 
                reportKeywords.length > 0 ? reportKeywords : DEFAULT_KEYWORDS, 
                reportMinCount, 
                true,
                geographicFilters
            );
        }
    }, [selectedFile, reportKeywords, reportMinCount, onLoadKeywordAgeReport, geographicFilters]);

    const handleRegenerateReport = useCallback(() => {
        if (selectedFile) {
            onLoadKeywordAgeReport(
                selectedFile,
                reportKeywords,
                reportMinCount,
                showTemporalData,
                geographicFilters
            );
        }
    }, [selectedFile, reportKeywords, reportMinCount, showTemporalData, onLoadKeywordAgeReport, geographicFilters]);

    // ✅ RENDERIZADO INICIAL
    if (!hasReport && !loadingReport && !showReport) {
        return (
            <Card className="temporal-report-card temporal-empty-state">
                <div className="temporal-empty-content">
                    <CalendarOutlined className="temporal-empty-icon" />
                    <div className="temporal-empty-text">
                        <Text className="temporal-empty-title">
                            Generar Reporte
                        </Text>
                        <Text type="secondary" className="temporal-empty-description">
                            Analiza las columnas con palabras clave y filtros geográficos
                        </Text>
                    </div>
                    <Button
                        type="primary"
                        icon={<BarChartOutlined />}
                        onClick={handleLoadReport}
                        className="temporal-generate-button"
                        size="large"
                    >
                        Generar Reporte Ahora
                    </Button>
                </div>
            </Card>
        );
    }

    const hasGeoFilters = !!(geographicFilters.departamento || geographicFilters.municipio || geographicFilters.ips);

    return (
        <Card
            className="temporal-report-card"
            title={
                <Space className="temporal-report-title" wrap>
                    <BarChartOutlined />
                    <span>{reportTitle}</span>
                    
                    {/* ✅ MOSTRAR FILTROS GEOGRÁFICOS ACTIVOS */}
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
            }
            extra={
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
            }
        >
            {loadingReport ? (
                <div className="temporal-loading-container">
                    <Spin size="large" />
                    <div className="temporal-loading-text">
                        <Text>Generando reporte con filtros geográficos...</Text>
                    </div>
                </div>
            ) : showReport ? (
                <div className="temporal-report-content">
                    {/* ✅ FILTROS GEOGRÁFICOS SIEMPRE VISIBLES */}
                    <GeographicFilters
                        filters={geographicFilters}
                        options={{
                            departamentos: departamentosOptions,
                            municipios: municipiosOptions,
                            ips: ipsOptions
                        }}
                        loading={loadingGeoFilters}
                        onDepartamentoChange={onDepartamentoChange}
                        onMunicipioChange={onMunicipioChange}
                        onIpsChange={onIpsChange}
                        onReset={resetGeographicFilters}
                        disabled={loadingReport}
                    />

                    {/* ✅ CONTROLES DE PALABRAS CLAVE SIEMPRE VISIBLES */}
                    <KeywordControls
                        reportKeywords={reportKeywords}
                        hasReport={hasReport}
                        loadingReport={loadingReport}
                        onSetReportKeywords={onSetReportKeywords}
                        onRegenerateReport={handleRegenerateReport}
                    />

                    {/* ✅ ALERTA CUANDO NO HAY RESULTADOS */}
                    {!hasReport && (
                        <Alert
                            message="No se encontraron resultados"
                            description="No se encontraron columnas que coincidan con las palabras clave y filtros geográficos seleccionados. Intenta cambiar los filtros."
                            type="warning"
                            showIcon
                            style={{ marginBottom: 16 }}
                            action={
                                <Button size="small" onClick={handleRegenerateReport} loading={loadingReport}>
                                    Intentar de nuevo
                                </Button>
                            }
                        />
                    )}

                    {/* ✅ ESTADÍSTICAS */}
                    {hasReport && keywordStats.length > 0 && (
                        <Row gutter={16} className="temporal-stats-row">
                            {keywordStats.map((stat) => (
                                <Col span={Math.max(6, Math.floor(24 / keywordStats.length))} key={stat.keyword}>
                                    <Card
                                        size="small"
                                        className={`temporal-stat-card temporal-stat-${stat.keyword}`}
                                        style={{
                                            borderColor: stat.config?.color || '#d9d9d9'
                                        }}
                                    >
                                        <Statistic
                                            title={(stat.config?.label || stat.keyword).toUpperCase()}
                                            value={stat.total}
                                            suffix={`Atenciones`}
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
                    )}

                    {/* ✅ TABLA PRINCIPAL */}
                    {hasReport ? (
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
                    ) : (
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
                                    onClick={handleLoadReport}
                                    className="temporal-generate-button"
                                    disabled={reportKeywords.length === 0}
                                    loading={loadingReport}
                                >
                                    {loadingReport ? 'Generando...' : 'Generar Reporte'}
                                </Button>
                            </Empty>
                        </div>
                    )}
                </div>
            ) : null}
        </Card>
    );
});

Report.displayName = 'Report';

export default Report;
