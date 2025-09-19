// components/report/AgeRangeSelector.tsx - CON BOTÓN MANUAL PARA GENERAR REPORTE

import React, { useState, useEffect } from 'react';
import {
  Select, Card, Row, Col, Typography, Statistic, Spin, Alert,
  DatePicker, Button, Space, Divider, Tag
} from 'antd';
import {
  CalendarOutlined, UserOutlined, PlayCircleOutlined,
  StopOutlined, ReloadOutlined, CheckCircleOutlined
} from '@ant-design/icons';
import { TechnicalNoteService } from '../../../services/TechnicalNoteService';
import type { AgeRangesResponse } from '../../../interfaces/IAge';
import dayjs, { Dayjs } from 'dayjs';

const { Text, Title } = Typography;
const { Option } = Select;

interface AgeRangeSelectorProps {
  filename: string;
  onAgeSelectionChange: (selection: {
    selectedYears: number[];
    selectedMonths: number[];
    corteFecha: string;
  }) => void;
  initialCorteFecha?: string;
}

export const AgeRangeSelector: React.FC<AgeRangeSelectorProps> = ({
  filename,
  onAgeSelectionChange,
  initialCorteFecha = "2025-07-31"
}) => {
  const [ageRanges, setAgeRanges] = useState<AgeRangesResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedYears, setSelectedYears] = useState<number[]>([]);
  const [selectedMonths, setSelectedMonths] = useState<number[]>([]);
  const [corteFecha, setCorteFecha] = useState<string>("2025-07-31");

  // ✅ NUEVO ESTADO PARA CONTROL MANUAL
  const [isGenerating, setIsGenerating] = useState(false);
  const [hasGenerated, setHasGenerated] = useState(false);
  const [lastSuccessfulSelection, setLastSuccessfulSelection] = useState<{
    selectedYears: number[];
    selectedMonths: number[];
    corteFecha: string;
  } | null>(null);

  // Cargar rangos de edades
  const loadAgeRanges = async (fecha: string) => {
    setLoading(true);
    setError(null);

    try {
      const response = await TechnicalNoteService.getAgeRanges(filename, fecha);
      setAgeRanges(response);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Error cargando rangos de edades');
      console.error('Error cargando rangos:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (filename) {
      loadAgeRanges(corteFecha);
    }
  }, [filename, corteFecha]);

  // ✅ FUNCIÓN PARA GENERAR REPORTE MANUALMENTE
  const handleGenerateReport = async () => {
    // Validar que se haya seleccionado al menos un mes o año
    if (selectedYears.length === 0 && selectedMonths.length === 0) {
      setError('Debe seleccionar al menos una edad en años o meses para generar el reporte');
      return;
    }

    setIsGenerating(true);
    setError(null);

    try {
      const selection = {
        selectedYears,
        selectedMonths,
        corteFecha
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

  // ✅ HANDLER CORREGIDO PARA FECHA
  const handleDateChange = (date: Dayjs | null, dateString: string | string[]) => {
    const dateStr = Array.isArray(dateString) ? dateString[0] : dateString;

    if (dateStr && date) {
      setCorteFecha(dateStr);
      // Limpiar selecciones y estado al cambiar fecha
      setSelectedYears([]);
      setSelectedMonths([]);
      setHasGenerated(false);
      setLastSuccessfulSelection(null);
      setError(null);
    }
  };

  // ✅ VERIFICAR SI HAY SELECCIÓN VÁLIDA
  const hasValidSelection = selectedYears.length > 0 || selectedMonths.length > 0;

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
        />
      </Card>
    );
  }

  if (!ageRanges) {
    return null;
  }

  return (
    <Card
      title={
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <UserOutlined />
          <span>Generar Reporte de Inasistentes</span>
          {filename && <Tag color="blue">{filename}</Tag>}
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
      {/* ✅ FECHA DE CORTE Y ESTADÍSTICAS */}
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={8}>
          <Text strong>📅 Fecha de Corte:</Text>
          <div style={{ marginTop: 4 }}>
            <DatePicker
              value={dayjs(corteFecha)}
              onChange={handleDateChange}
              format="YYYY-MM-DD"
              placeholder="Seleccionar fecha de corte"
              allowClear={false}
              size="small"
              disabled={isGenerating}
            />
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
                💡 Presione "Generar Reporte" para procesar los inasistentes
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
                <div>📅 Fecha corte: {lastSuccessfulSelection.corteFecha}</div>
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
            1. Seleccione las edades en años y/o meses que desea evaluar<br />
            2. Presione "Generar Reporte" para procesar los inasistentes<br />
            3. Los resultados aparecerán en la tabla inferior
          </Text>
        </div>
      )}
    </Card>
  );
};
