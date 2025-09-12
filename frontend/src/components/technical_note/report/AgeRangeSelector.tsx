// components/report/AgeRangeSelector.tsx - CORREGIR onChange handler

import React, { useState, useEffect } from 'react';
import { Select, Card, Row, Col, Typography, Statistic, Spin, Alert, DatePicker } from 'antd';
import { CalendarOutlined, UserOutlined } from '@ant-design/icons';
import { TechnicalNoteService } from '../../../services/TechnicalNoteService';
import type { AgeRangesResponse } from '../../../interfaces/IAge';
import dayjs, { Dayjs } from 'dayjs';

const { Text } = Typography;
const { Option } = Select;

interface AgeRangeSelectorProps {
  filename: string;
  onAgeSelectionChange: (selection: {
    selectedYears: number[];
    selectedMonths: number[];
    corteFecha: string;
  }) => void;
}

export const AgeRangeSelector: React.FC<AgeRangeSelectorProps> = ({
  filename,
  onAgeSelectionChange
}) => {
  const [ageRanges, setAgeRanges] = useState<AgeRangesResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedYears, setSelectedYears] = useState<number[]>([]);
  const [selectedMonths, setSelectedMonths] = useState<number[]>([]);
  const [corteFecha, setCorteFecha] = useState<string>("2025-07-31");

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

  // Notificar cambios al componente padre
  useEffect(() => {
    onAgeSelectionChange({
      selectedYears,
      selectedMonths,
      corteFecha
    });
  }, [selectedYears, selectedMonths, corteFecha, onAgeSelectionChange]);

  // ✅ CORREGIR: Handler con tipos correctos
  const handleDateChange = (date: Dayjs | null, dateString: string | string[]) => {
    // Como no usamos múltiple, dateString siempre será string, pero manejamos ambos tipos
    const dateStr = Array.isArray(dateString) ? dateString[0] : dateString;
    
    if (dateStr && date) {
      setCorteFecha(dateStr);
      // Limpiar selecciones al cambiar fecha
      setSelectedYears([]);
      setSelectedMonths([]);
    }
  };

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

  if (error) {
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
          <span>Filtros por Edad para Inasistentes</span>
        </div>
      }
      size="small"
    >
      {/* Fecha de Corte */}
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={8}>
          <Text strong>Fecha de Corte:</Text>
          <div style={{ marginTop: 4 }}>
            <DatePicker
              value={dayjs(corteFecha)}
              onChange={handleDateChange} // ✅ Ahora con tipos correctos
              format="YYYY-MM-DD"
              placeholder="Seleccionar fecha de corte"
              allowClear={false}
              size="small"
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
              />
            </Col>
            <Col span={8}>
              <Statistic
                title="Rango Años"
                value={`${ageRanges.statistics.rango_años.min}-${ageRanges.statistics.rango_años.max}`}
                prefix={<CalendarOutlined />}
              />
            </Col>
            <Col span={8}>
              <Statistic
                title="Rango Meses"
                value={`${ageRanges.statistics.rango_meses.min}-${ageRanges.statistics.rango_meses.max}`}
                prefix={<CalendarOutlined />}
              />
            </Col>
          </Row>
        </Col>
      </Row>

      {/* Selectores de Edad */}
      <Row gutter={16}>
        <Col span={12}>
          <Text strong>Edades en Años:</Text>
          <div style={{ marginTop: 4 }}>
            <Select
              mode="multiple"
              style={{ width: '100%' }}
              placeholder="Seleccionar edades en años"
              value={selectedYears}
              onChange={setSelectedYears}
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
          <Text strong>Edades en Meses:</Text>
          <div style={{ marginTop: 4 }}>
            <Select
              mode="multiple"
              style={{ width: '100%' }}
              placeholder="Seleccionar edades en meses"
              value={selectedMonths}
              onChange={setSelectedMonths}
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

      {/* Información de Selección */}
      {(selectedYears.length > 0 || selectedMonths.length > 0) && (
        <div style={{ marginTop: 16, padding: 12, backgroundColor: '#f6ffed', border: '1px solid #b7eb8f', borderRadius: 6 }}>
          <Text strong style={{ color: '#52c41a' }}>Filtros Activos:</Text>
          <div style={{ marginTop: 4 }}>
            {selectedYears.length > 0 && (
              <div>
                <Text style={{ fontSize: 12 }}>
                  📅 Años: {selectedYears.join(', ')}
                </Text>
              </div>
            )}
            {selectedMonths.length > 0 && (
              <div>
                <Text style={{ fontSize: 12 }}>
                  🗓️ Meses: {selectedMonths.slice(0, 10).join(', ')}{selectedMonths.length > 10 ? '...' : ''}
                </Text>
              </div>
            )}
          </div>
        </div>
      )}
    </Card>
  );
};
