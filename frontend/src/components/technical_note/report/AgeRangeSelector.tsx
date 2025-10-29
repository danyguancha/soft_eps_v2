// components/report/AgeRangeSelector.tsx - ✅ VERSIÓN ULTRA COMPACTADA

import React, { useState, useEffect } from 'react';
import {
  Select, Card, Row, Col, Typography, Statistic, Spin, Alert,
  Button, Space, Divider, Tag
} from 'antd';
import {
  CalendarOutlined, UserOutlined, PlayCircleOutlined,
  StopOutlined, ReloadOutlined
} from '@ant-design/icons';
import { TechnicalNoteService } from '../../../services/TechnicalNoteService';
import type { AgeRangesResponse } from '../../../interfaces/IAge';
import dayjs from 'dayjs';

const { Text } = Typography;
const { Option } = Select;

interface AgeRangeSelectorProps {
  filename: string;
  cutoffDate: string;
  onAgeSelectionChange: (selection: {
    selectedYears: number[];
    selectedMonths: number[];
    corteFecha: string;
  }) => void;
}

export const AgeRangeSelector: React.FC<AgeRangeSelectorProps> = ({
  filename,
  cutoffDate,
  onAgeSelectionChange
}) => {
  const [ageRanges, setAgeRanges] = useState<AgeRangesResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedYears, setSelectedYears] = useState<number[]>([]);
  const [selectedMonths, setSelectedMonths] = useState<number[]>([]);
  const [isGenerating, setIsGenerating] = useState(false);
  const [hasGenerated, setHasGenerated] = useState(false);
  const [lastSuccessfulSelection, setLastSuccessfulSelection] = useState<{
    selectedYears: number[];
    selectedMonths: number[];
    corteFecha: string;
  } | null>(null);

  const loadAgeRanges = async (fecha: string) => {
    if (!fecha) {
      console.warn('⚠️ No se puede cargar rangos sin fecha de corte');
      return;
    }

    setLoading(true);
    setError(null);

    try {
      console.log(`📊 Cargando rangos de edades para ${filename} con fecha ${fecha}`);
      const response = await TechnicalNoteService.getAgeRanges(filename, fecha);
      setAgeRanges(response);
      console.log('✅ Rangos cargados exitosamente:', response);
    } catch (err: any) {
      const errorMsg = err.response?.data?.detail || 'Error cargando rangos de edades';
      setError(errorMsg);
      console.error('❌ Error cargando rangos:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (filename && cutoffDate) {
      console.log(`🔄 Detectado cambio: filename=${filename}, cutoffDate=${cutoffDate}`);
      loadAgeRanges(cutoffDate);
      
      setSelectedYears([]);
      setSelectedMonths([]);
      setHasGenerated(false);
      setLastSuccessfulSelection(null);
      setError(null);
    }
  }, [filename, cutoffDate]);

  const handleGenerateReport = async () => {
    if (selectedYears.length === 0 && selectedMonths.length === 0) {
      setError('Debe seleccionar al menos una edad en años o meses para generar el reporte');
      return;
    }

    if (!cutoffDate) {
      setError('No hay fecha de corte seleccionada');
      return;
    }

    setIsGenerating(true);
    setError(null);

    try {
      const selection = {
        selectedYears,
        selectedMonths,
        corteFecha: cutoffDate
      };

      console.log('🚀 Generando reporte con selección:', selection);
      await onAgeSelectionChange(selection);

      setLastSuccessfulSelection(selection);
      setHasGenerated(true);

    } catch (error: any) {
      console.error('❌ Error generando reporte:', error);
      setError(error.message || 'Error generando reporte de inasistentes');
    } finally {
      setIsGenerating(false);
    }
  };

  const handleClearSelection = () => {
    setSelectedYears([]);
    setSelectedMonths([]);
    setError(null);
    setHasGenerated(false);
    setLastSuccessfulSelection(null);
    console.log('🧹 Selección limpiada');
  };

  const handleReloadReport = async () => {
    if (!lastSuccessfulSelection) return;

    setIsGenerating(true);
    setError(null);

    try {
      console.log('🔄 Recargando reporte con última selección:', lastSuccessfulSelection);
      await onAgeSelectionChange(lastSuccessfulSelection);
    } catch (error: any) {
      console.error('❌ Error recargando reporte:', error);
      setError(error.message || 'Error recargando reporte');
    } finally {
      setIsGenerating(false);
    }
  };

  const hasValidSelection = selectedYears.length > 0 || selectedMonths.length > 0;

  if (loading) {
    return (
      <Card bodyStyle={{ padding: '20px' }}>
        <div style={{ textAlign: 'center' }}>
          <Spin size="default" />
          <div style={{ marginTop: 8 }}>
            <Text style={{ fontSize: '11px' }}>Cargando rangos de edades...</Text>
          </div>
        </div>
      </Card>
    );
  }

  if (error && !ageRanges) {
    return (
      <Card bodyStyle={{ padding: '10px' }}>
        <Alert
          message="Error cargando rangos"
          description={error}
          type="error"
          showIcon
          style={{ fontSize: '11px' }}
          action={
            <Button size="small" onClick={() => loadAgeRanges(cutoffDate)} style={{ fontSize: '11px', height: '24px' }}>
              Reintentar
            </Button>
          }
        />
      </Card>
    );
  }

  if (!ageRanges) {
    return (
      <Card bodyStyle={{ padding: '10px' }}>
        <Alert
          message="Esperando datos"
          description="Seleccione un archivo y fecha de corte para cargar los rangos de edades"
          type="info"
          showIcon
          style={{ fontSize: '11px' }}
        />
      </Card>
    );
  }

  return (
    <>
      <style>{`
        .age-selector-compact .ant-card-head {
          padding: 0 10px !important;
          min-height: 36px !important;
        }
        
        .age-selector-compact .ant-card-head-title {
          padding: 6px 0 !important;
          font-size: 12px !important;
        }
        
        .age-selector-compact .ant-card-body {
          padding: 10px !important;
        }
        
        .age-selector-compact .ant-card-extra {
          padding: 4px 0 !important;
        }
        
        .age-selector-compact .ant-select {
          font-size: 11px !important;
        }
        
        .age-selector-compact .ant-select-selector {
          font-size: 11px !important;
          min-height: 26px !important;
          padding: 0 6px !important;
        }
        
        .age-selector-compact .ant-select-selection-item {
          font-size: 10px !important;
          padding: 0 4px !important;
          line-height: 20px !important;
          height: 20px !important;
        }
        
        .age-selector-compact .ant-tag {
          font-size: 10px !important;
          padding: 0 5px !important;
          line-height: 18px !important;
          margin: 0 4px !important;
        }
        
        .age-selector-compact .ant-btn-sm {
          height: 24px !important;
          padding: 0 8px !important;
          font-size: 11px !important;
        }
        
        .age-selector-compact .ant-statistic {
          margin-bottom: 0 !important;
        }
        
        .age-selector-compact .ant-statistic-title {
          font-size: 10px !important;
          margin-bottom: 2px !important;
        }
        
        .age-selector-compact .ant-statistic-content {
          font-size: 13px !important;
        }
        
        .age-selector-compact .ant-alert {
          padding: 6px 10px !important;
          font-size: 10px !important;
        }
        
        .age-selector-compact .ant-alert-message {
          font-size: 11px !important;
          margin-bottom: 2px !important;
        }
        
        .age-selector-compact .ant-alert-description {
          font-size: 10px !important;
        }
        
        .age-selector-compact .ant-divider {
          margin: 10px 0 !important;
        }
      `}</style>

      <Card
        className="age-selector-compact"
        title={
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <UserOutlined style={{ fontSize: '12px' }} />
            <span style={{ fontSize: '12px' }}>Generar Reporte de Inasistentes</span>
            {filename && <Tag color="blue" style={{ fontSize: '9px', padding: '0 4px', lineHeight: '16px' }}>{filename}</Tag>}
            {cutoffDate && (
              <Tag color="green" icon={<CalendarOutlined style={{ fontSize: '9px' }} />} style={{ fontSize: '9px', padding: '0 4px', lineHeight: '16px' }}>
                Corte: {dayjs(cutoffDate).format('DD/MM/YYYY')}
              </Tag>
            )}
          </div>
        }
        extra={
          hasGenerated && (
            <Space size={4}>
              <Button
                icon={<ReloadOutlined style={{ fontSize: '11px' }} />}
                onClick={handleReloadReport}
                loading={isGenerating}
                disabled={!lastSuccessfulSelection || isGenerating}
                title="Recargar último reporte"
                size="small"
                style={{ fontSize: '10px', height: '24px', padding: '0 6px' }}
              >
                Recargar
              </Button>
              <Button
                icon={<StopOutlined style={{ fontSize: '11px' }} />}
                onClick={handleClearSelection}
                disabled={isGenerating}
                size="small"
                style={{ fontSize: '10px', height: '24px', padding: '0 6px' }}
              >
                Limpiar
              </Button>
            </Space>
          )
        }
        size="small"
        bodyStyle={{ padding: '10px' }}
      >
        <Row gutter={[8, 8]} style={{ marginBottom: 10 }}>
          <Col span={8}>
            <div style={{
              padding: '8px',
              backgroundColor: '#f0f5ff',
              border: '1px solid #1890ff',
              borderRadius: 4,
              textAlign: 'center'
            }}>
              <CalendarOutlined style={{ fontSize: 18, color: '#1890ff', marginBottom: 4 }} />
              <div>
                <Text strong style={{ display: 'block', marginBottom: 2, fontSize: '10px' }}>
                  Fecha de Corte
                </Text>
                <Text style={{ fontSize: 14, color: '#1890ff', fontWeight: 600 }}>
                  {dayjs(cutoffDate).format('DD/MM/YYYY')}
                </Text>
              </div>
              <div style={{ marginTop: 2 }}>
                <Text type="secondary" style={{ fontSize: 9 }}>
                  {dayjs(cutoffDate).format('dddd, D [de] MMMM [de] YYYY')}
                </Text>
              </div>
            </div>
          </Col>
          <Col span={16}>
            <Row gutter={8}>
              <Col span={8}>
                <Statistic
                  title="Total Registros"
                  value={ageRanges.statistics.total_registros}
                  prefix={<UserOutlined style={{ fontSize: '11px' }} />}
                  valueStyle={{ fontSize: '13px' }}
                  style={{ fontSize: '10px' }}
                />
              </Col>
              <Col span={8}>
                <Statistic
                  title="Rango Años"
                  value={`${ageRanges.statistics.rango_años.min}-${ageRanges.statistics.rango_años.max}`}
                  prefix={<CalendarOutlined style={{ fontSize: '11px' }} />}
                  valueStyle={{ fontSize: '13px' }}
                  style={{ fontSize: '10px' }}
                />
              </Col>
              <Col span={8}>
                <Statistic
                  title="Rango Meses"
                  value={`${ageRanges.statistics.rango_meses.min}-${ageRanges.statistics.rango_meses.max}`}
                  prefix={<CalendarOutlined style={{ fontSize: '11px' }} />}
                  valueStyle={{ fontSize: '13px' }}
                  style={{ fontSize: '10px' }}
                />
              </Col>
            </Row>
          </Col>
        </Row>

        <Row gutter={8} style={{ marginBottom: 10 }}>
          <Col span={12}>
            <Text strong style={{ fontSize: '10px' }}>🗓️ Edades en Años:</Text>
            <div style={{ marginTop: 3 }}>
              <Select
                mode="multiple"
                style={{ width: '100%' }}
                placeholder="Seleccionar edades en años"
                value={selectedYears}
                onChange={setSelectedYears}
                disabled={isGenerating}
                showSearch
                size="small"
                filterOption={(input, option) =>
                  option?.children?.toString().toLowerCase().includes(input.toLowerCase()) ?? false
                }
                maxTagCount="responsive"
              >
                {ageRanges.age_ranges.years.map(year => (
                  <Option key={year} value={year}>
                    {year} año{year !== 1 ? 's' : ''}
                  </Option>
                ))}
              </Select>
            </div>
            <div style={{ marginTop: 2 }}>
              <Text type="secondary" style={{ fontSize: 9 }}>
                {ageRanges.age_ranges.years.length} valores únicos disponibles
              </Text>
            </div>
          </Col>

          <Col span={12}>
            <Text strong style={{ fontSize: '10px' }}>📅 Edades en Meses:</Text>
            <div style={{ marginTop: 3 }}>
              <Select
                mode="multiple"
                style={{ width: '100%' }}
                placeholder="Seleccionar edades en meses"
                value={selectedMonths}
                onChange={setSelectedMonths}
                disabled={isGenerating}
                showSearch
                size="small"
                filterOption={(input, option) =>
                  option?.children?.toString().toLowerCase().includes(input.toLowerCase()) ?? false
                }
                maxTagCount="responsive"
              >
                {ageRanges.age_ranges.months.map(month => (
                  <Option key={month} value={month}>
                    {month} mes{month !== 1 ? 'es' : ''}
                  </Option>
                ))}
              </Select>
            </div>
            <div style={{ marginTop: 2 }}>
              <Text type="secondary" style={{ fontSize: 9 }}>
                {ageRanges.age_ranges.months.length} valores únicos disponibles
              </Text>
            </div>
          </Col>
        </Row>

        {hasValidSelection && (
          <div style={{
            marginBottom: 10,
            padding: 8,
            backgroundColor: '#f6ffed',
            border: '1px solid #b7eb8f',
            borderRadius: 4
          }}>
            <Text strong style={{ color: '#52c41a', fontSize: '10px' }}>📋 Filtros Seleccionados:</Text>
            <div style={{ marginTop: 3 }}>
              {selectedYears.length > 0 && (
                <div>
                  <Text style={{ fontSize: 9 }}>
                    🗓️ <strong>Años:</strong> {selectedYears.sort((a, b) => a - b).join(', ')}
                  </Text>
                </div>
              )}
              {selectedMonths.length > 0 && (
                <div>
                  <Text style={{ fontSize: 9 }}>
                    📅 <strong>Meses:</strong> {selectedMonths.sort((a, b) => a - b).slice(0, 15).join(', ')}{selectedMonths.length > 15 ? '...' : ''}
                  </Text>
                </div>
              )}
              <div style={{ marginTop: 2 }}>
                <Text type="secondary" style={{ fontSize: 9 }}>
                  💡 Presione "Generar Reporte" para procesar los inasistentes con la fecha de corte {dayjs(cutoffDate).format('DD/MM/YYYY')}
                </Text>
              </div>
            </div>
          </div>
        )}

        <Divider style={{ margin: '8px 0' }} />

        <div style={{ textAlign: 'center' }}>
          <Space size={8}>
            {!hasValidSelection && (
              <Text type="secondary" style={{ fontSize: '10px' }}>
                ⚠️ Seleccione al menos una edad en años o meses
              </Text>
            )}

            {hasValidSelection && (
              <Button
                type="primary"
                size="small"
                icon={<PlayCircleOutlined style={{ fontSize: '11px' }} />}
                onClick={handleGenerateReport}
                loading={isGenerating}
                disabled={isGenerating}
                style={{ fontSize: '11px', height: '28px', padding: '0 12px' }}
              >
                {isGenerating ? 'Generando Reporte...' : 'Generar Reporte de Inasistentes'}
              </Button>
            )}

            {hasValidSelection && !isGenerating && (
              <Button
                icon={<StopOutlined style={{ fontSize: '11px' }} />}
                onClick={handleClearSelection}
                size="small"
                style={{ fontSize: '11px', height: '28px', padding: '0 10px' }}
              >
                Limpiar Selección
              </Button>
            )}
          </Space>
        </div>

        {error && (
          <Alert
            message="Error"
            description={error}
            type="error"
            showIcon
            closable
            onClose={() => setError(null)}
            style={{ marginTop: 10, fontSize: '10px', padding: '6px 10px' }}
          />
        )}

        {hasGenerated && lastSuccessfulSelection && !isGenerating && !error && (
          <Alert
            message="✅ Reporte Generado Exitosamente"
            description={
              <div style={{ fontSize: '10px' }}>
                <Text style={{ fontSize: '10px' }}>El reporte de inasistentes se ha generado correctamente.</Text>
                <div style={{ marginTop: 3 }}>
                  {lastSuccessfulSelection.selectedYears.length > 0 && (
                    <div style={{ fontSize: '9px' }}>🗓️ Años: {lastSuccessfulSelection.selectedYears.join(', ')}</div>
                  )}
                  {lastSuccessfulSelection.selectedMonths.length > 0 && (
                    <div style={{ fontSize: '9px' }}>📅 Meses: {lastSuccessfulSelection.selectedMonths.slice(0, 10).join(', ')}{lastSuccessfulSelection.selectedMonths.length > 10 ? '...' : ''}</div>
                  )}
                  <div style={{ fontSize: '9px' }}>📅 Fecha corte: {dayjs(lastSuccessfulSelection.corteFecha).format('DD/MM/YYYY')}</div>
                </div>
              </div>
            }
            type="success"
            showIcon
            style={{ marginTop: 10, fontSize: '10px', padding: '6px 10px' }}
          />
        )}

        {!hasValidSelection && !isGenerating && (
          <div style={{
            textAlign: 'center',
            padding: '12px',
            backgroundColor: '#fafafa',
            borderRadius: 4,
            marginTop: 10
          }}>
            <Text type="secondary" style={{ fontSize: '10px' }}>
              🎯 <strong>Instrucciones:</strong><br />
              1. Las edades se calcularán basándose en la fecha de corte: {dayjs(cutoffDate).format('DD/MM/YYYY')}<br />
              2. Seleccione las edades en años y/o meses que desea evaluar<br />
              3. Presione "Generar Reporte" para procesar los inasistentes<br />
              4. Los resultados aparecerán en la tabla inferior
            </Text>
          </div>
        )}
      </Card>
    </>
  );
};
