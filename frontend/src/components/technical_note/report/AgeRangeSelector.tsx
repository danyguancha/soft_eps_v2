// components/report/AgeRangeSelector.tsx - CON FECHA DE CORTE DESDE COMPONENTE PADRE

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
  cutoffDate: string; // NUEVA PROP: Fecha de corte desde el padre (formato YYYY-MM-DD)
  onAgeSelectionChange: (selection: {
    selectedYears: number[];
    selectedMonths: number[];
    corteFecha: string;
  }) => void;
}

export const AgeRangeSelector: React.FC<AgeRangeSelectorProps> = ({
  filename,
  cutoffDate, // RECIBIR fecha de corte desde el padre
  onAgeSelectionChange
}) => {
  const [ageRanges, setAgeRanges] = useState<AgeRangesResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedYears, setSelectedYears] = useState<number[]>([]);
  const [selectedMonths, setSelectedMonths] = useState<number[]>([]);

  // ✅ ESTADO PARA CONTROL MANUAL
  const [isGenerating, setIsGenerating] = useState(false);
  const [hasGenerated, setHasGenerated] = useState(false);
  const [lastSuccessfulSelection, setLastSuccessfulSelection] = useState<{
    selectedYears: number[];
    selectedMonths: number[];
    corteFecha: string;
  } | null>(null);

  // ✅ Cargar rangos de edades cuando cambia el archivo o la fecha de corte
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

  // ✅ Efecto para cargar rangos cuando cambia el archivo o la fecha
  useEffect(() => {
    if (filename && cutoffDate) {
      console.log(`🔄 Detectado cambio: filename=${filename}, cutoffDate=${cutoffDate}`);
      loadAgeRanges(cutoffDate);
      
      // Limpiar selecciones al cambiar fecha o archivo
      setSelectedYears([]);
      setSelectedMonths([]);
      setHasGenerated(false);
      setLastSuccessfulSelection(null);
      setError(null);
    }
  }, [filename, cutoffDate]);

  // ✅ FUNCIÓN PARA GENERAR REPORTE MANUALMENTE
  const handleGenerateReport = async () => {
    // Validar que se haya seleccionado al menos un mes o año
    if (selectedYears.length === 0 && selectedMonths.length === 0) {
      setError('Debe seleccionar al menos una edad en años o meses para generar el reporte');
      return;
    }

    // Validar que haya fecha de corte
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
        corteFecha: cutoffDate // Usar la fecha del padre
      };

      console.log('🚀 Generando reporte con selección:', selection);

      // Llamar al callback del componente padre
      await onAgeSelectionChange(selection);

      // Marcar como exitoso
      setLastSuccessfulSelection(selection);
      setHasGenerated(true);

    } catch (error: any) {
      console.error('❌ Error generando reporte:', error);
      setError(error.message || 'Error generando reporte de inasistentes');
    } finally {
      setIsGenerating(false);
    }
  };

  // ✅ FUNCIÓN PARA LIMPIAR SELECCIONES
  const handleClearSelection = () => {
    setSelectedYears([]);
    setSelectedMonths([]);
    setError(null);
    setHasGenerated(false);
    setLastSuccessfulSelection(null);
    console.log('🧹 Selección limpiada');
  };

  // ✅ FUNCIÓN PARA RECARGAR CON LA MISMA SELECCIÓN
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

  // ✅ VERIFICAR SI HAY SELECCIÓN VÁLIDA
  const hasValidSelection = selectedYears.length > 0 || selectedMonths.length > 0;

  // Renderizado de estados de carga y error
  if (loading) {
    return (
      <Card>
        <div style={{ textAlign: 'center', padding: '40px' }}>
          <Spin size="large" />
          <div style={{ marginTop: 16 }}>
            <Text>Cargando rangos de edades...</Text>
          </div>
        </div>
      </Card>
    );
  }

  if (error && !ageRanges) {
    return (
      <Card>
        <Alert
          message="Error cargando rangos"
          description={error}
          type="error"
          showIcon
          action={
            <Button size="small" onClick={() => loadAgeRanges(cutoffDate)}>
              Reintentar
            </Button>
          }
        />
      </Card>
    );
  }

  if (!ageRanges) {
    return (
      <Card>
        <Alert
          message="Esperando datos"
          description="Seleccione un archivo y fecha de corte para cargar los rangos de edades"
          type="info"
          showIcon
        />
      </Card>
    );
  }

  return (
    <Card
      title={
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <UserOutlined />
          <span>Generar Reporte de Inasistentes</span>
          {filename && <Tag color="blue">{filename}</Tag>}
          {cutoffDate && (
            <Tag color="green" icon={<CalendarOutlined />}>
              Corte: {dayjs(cutoffDate).format('DD/MM/YYYY')}
            </Tag>
          )}
        </div>
      }
      extra={
        hasGenerated && (
          <Space>
            <Button
              icon={<ReloadOutlined />}
              onClick={handleReloadReport}
              loading={isGenerating}
              disabled={!lastSuccessfulSelection || isGenerating}
              title="Recargar último reporte"
            >
              Recargar
            </Button>
            <Button
              icon={<StopOutlined />}
              onClick={handleClearSelection}
              disabled={isGenerating}
            >
              Limpiar
            </Button>
          </Space>
        )
      }
      size="small"
    >
      {/* ✅ INFORMACIÓN DE FECHA DE CORTE Y ESTADÍSTICAS */}
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={8}>
          <div style={{
            padding: '12px',
            backgroundColor: '#f0f5ff',
            border: '2px solid #1890ff',
            borderRadius: 8,
            textAlign: 'center'
          }}>
            <CalendarOutlined style={{ fontSize: 24, color: '#1890ff', marginBottom: 8 }} />
            <div>
              <Text strong style={{ display: 'block', marginBottom: 4 }}>
                Fecha de Corte
              </Text>
              <Text style={{ fontSize: 18, color: '#1890ff', fontWeight: 600 }}>
                {dayjs(cutoffDate).format('DD/MM/YYYY')}
              </Text>
            </div>
            <div style={{ marginTop: 4 }}>
              <Text type="secondary" style={{ fontSize: 11 }}>
                {dayjs(cutoffDate).format('dddd, D [de] MMMM [de] YYYY')}
              </Text>
            </div>
          </div>
        </Col>
        <Col span={16}>
          <Row gutter={16}>
            <Col span={8}>
              <Statistic
                title="Total Registros"
                value={ageRanges.statistics.total_registros}
                prefix={<UserOutlined />}
                valueStyle={{ fontSize: '16px' }}
              />
            </Col>
            <Col span={8}>
              <Statistic
                title="Rango Años"
                value={`${ageRanges.statistics.rango_años.min}-${ageRanges.statistics.rango_años.max}`}
                prefix={<CalendarOutlined />}
                valueStyle={{ fontSize: '16px' }}
              />
            </Col>
            <Col span={8}>
              <Statistic
                title="Rango Meses"
                value={`${ageRanges.statistics.rango_meses.min}-${ageRanges.statistics.rango_meses.max}`}
                prefix={<CalendarOutlined />}
                valueStyle={{ fontSize: '16px' }}
              />
            </Col>
          </Row>
        </Col>
      </Row>

      {/* ✅ SELECTORES DE EDAD */}
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={12}>
          <Text strong>🗓️ Edades en Años:</Text>
          <div style={{ marginTop: 4 }}>
            <Select
              mode="multiple"
              style={{ width: '100%' }}
              placeholder="Seleccionar edades en años"
              value={selectedYears}
              onChange={setSelectedYears}
              disabled={isGenerating}
              showSearch
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
          <div style={{ marginTop: 4 }}>
            <Text type="secondary" style={{ fontSize: 12 }}>
              {ageRanges.age_ranges.years.length} valores únicos disponibles
            </Text>
          </div>
        </Col>

        <Col span={12}>
          <Text strong>📅 Edades en Meses:</Text>
          <div style={{ marginTop: 4 }}>
            <Select
              mode="multiple"
              style={{ width: '100%' }}
              placeholder="Seleccionar edades en meses"
              value={selectedMonths}
              onChange={setSelectedMonths}
              disabled={isGenerating}
              showSearch
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
          <div style={{ marginTop: 4 }}>
            <Text type="secondary" style={{ fontSize: 12 }}>
              {ageRanges.age_ranges.months.length} valores únicos disponibles
            </Text>
          </div>
        </Col>
      </Row>

      {/* ✅ INFORMACIÓN DE SELECCIÓN ACTUAL */}
      {hasValidSelection && (
        <div style={{
          marginBottom: 16,
          padding: 12,
          backgroundColor: '#f6ffed',
          border: '1px solid #b7eb8f',
          borderRadius: 6
        }}>
          <Text strong style={{ color: '#52c41a' }}>📋 Filtros Seleccionados:</Text>
          <div style={{ marginTop: 4 }}>
            {selectedYears.length > 0 && (
              <div>
                <Text style={{ fontSize: 12 }}>
                  🗓️ <strong>Años:</strong> {selectedYears.sort((a, b) => a - b).join(', ')}
                </Text>
              </div>
            )}
            {selectedMonths.length > 0 && (
              <div>
                <Text style={{ fontSize: 12 }}>
                  📅 <strong>Meses:</strong> {selectedMonths.sort((a, b) => a - b).slice(0, 15).join(', ')}{selectedMonths.length > 15 ? '...' : ''}
                </Text>
              </div>
            )}
            <div style={{ marginTop: 4 }}>
              <Text type="secondary" style={{ fontSize: 11 }}>
                💡 Presione "Generar Reporte" para procesar los inasistentes con la fecha de corte {dayjs(cutoffDate).format('DD/MM/YYYY')}
              </Text>
            </div>
          </div>
        </div>
      )}

      <Divider style={{ margin: '16px 0' }} />

      {/* ✅ BOTONES DE ACCIÓN */}
      <div style={{ textAlign: 'center' }}>
        <Space size="middle">
          {!hasValidSelection && (
            <Text type="secondary">
              ⚠️ Seleccione al menos una edad en años o meses
            </Text>
          )}

          {hasValidSelection && (
            <Button
              type="primary"
              size="large"
              icon={<PlayCircleOutlined />}
              onClick={handleGenerateReport}
              loading={isGenerating}
              disabled={isGenerating}
              className="generate-report-button"
            >
              {isGenerating ? 'Generando Reporte...' : 'Generar Reporte de Inasistentes'}
            </Button>
          )}

          {hasValidSelection && !isGenerating && (
            <Button
              icon={<StopOutlined />}
              onClick={handleClearSelection}
            >
              Limpiar Selección
            </Button>
          )}
        </Space>
      </div>

      {/* ✅ ALERTAS DE ESTADO */}
      {error && (
        <Alert
          message="Error"
          description={error}
          type="error"
          showIcon
          closable
          onClose={() => setError(null)}
          style={{ marginTop: 16 }}
        />
      )}

      {hasGenerated && lastSuccessfulSelection && !isGenerating && !error && (
        <Alert
          message="✅ Reporte Generado Exitosamente"
          description={
            <div>
              <Text>El reporte de inasistentes se ha generado correctamente.</Text>
              <div style={{ marginTop: 4, fontSize: 12 }}>
                {lastSuccessfulSelection.selectedYears.length > 0 && (
                  <div>🗓️ Años: {lastSuccessfulSelection.selectedYears.join(', ')}</div>
                )}
                {lastSuccessfulSelection.selectedMonths.length > 0 && (
                  <div>📅 Meses: {lastSuccessfulSelection.selectedMonths.slice(0, 10).join(', ')}{lastSuccessfulSelection.selectedMonths.length > 10 ? '...' : ''}</div>
                )}
                <div>📅 Fecha corte: {dayjs(lastSuccessfulSelection.corteFecha).format('DD/MM/YYYY')}</div>
              </div>
            </div>
          }
          type="success"
          showIcon
          style={{ marginTop: 16 }}
        />
      )}

      {/* ✅ MENSAJE DE AYUDA */}
      {!hasValidSelection && !isGenerating && (
        <div style={{
          textAlign: 'center',
          padding: '20px',
          backgroundColor: '#fafafa',
          borderRadius: 6,
          marginTop: 16
        }}>
          <Text type="secondary">
            🎯 <strong>Instrucciones:</strong><br />
            1. Las edades se calcularán basándose en la fecha de corte: {dayjs(cutoffDate).format('DD/MM/YYYY')}<br />
            2. Seleccione las edades en años y/o meses que desea evaluar<br />
            3. Presione "Generar Reporte" para procesar los inasistentes<br />
            4. Los resultados aparecerán en la tabla inferior
          </Text>
        </div>
      )}
    </Card>
  );
};
