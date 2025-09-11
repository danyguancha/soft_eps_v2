// components/report/GeographicFilters.tsx
import { memo } from 'react';
import { Card, Row, Col, Space, Typography, Tag, Button, Select } from 'antd';
import { EnvironmentOutlined, ClearOutlined } from '@ant-design/icons';

const { Text } = Typography;
const { Option } = Select;

interface GeographicFiltersProps {
  filters: {
    departamento?: string | null;
    municipio?: string | null;
    ips?: string | null;
  };
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
}

export const GeographicFilters = memo<GeographicFiltersProps>(({
  filters,
  options,
  loading,
  onDepartamentoChange,
  onMunicipioChange,
  onIpsChange,
  onReset,
  disabled = false
}) => {
  const hasAnyFilter = Boolean(filters.departamento || filters.municipio || filters.ips);

  const renderSelect = (
    label: string,
    value: string | null | undefined,
    placeholder: string,
    options: string[],
    onChange: (value: string | null) => void,
    isLoading: boolean,
    isDisabled: boolean = false,
    dependencyMessage?: string
  ) => (
    <Space direction="vertical" style={{ width: '100%' }} size={4}>
      <Text strong style={{ fontSize: '13px' }}>{label}:</Text>
      <Select
        placeholder={placeholder}
        style={{ width: '100%' }}
        size="middle"
        value={value}
        onChange={onChange}
        loading={isLoading}
        disabled={disabled || isDisabled || isLoading}
        allowClear
        showSearch
        filterOption={(input, option) => {
          if (option?.children) {
            return String(option.children).toLowerCase().includes(input.toLowerCase());
          }
          return false;
        }}
      >
        {options.map(option => (
          <Option key={option} value={option}>
            {option}
          </Option>
        ))}
      </Select>
      {dependencyMessage && (
        <Text type="secondary" style={{ fontSize: '11px' }}>
          {dependencyMessage}
        </Text>
      )}
    </Space>
  );

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
        <Col xs={24} sm={8} md={8}>
          {renderSelect(
            'Departamento',
            filters.departamento,
            'Seleccionar departamento',
            options.departamentos,
            onDepartamentoChange,
            loading.departamentos
          )}
        </Col>

        <Col xs={24} sm={8} md={8}>
          {renderSelect(
            'Municipio',
            filters.municipio,
            !filters.departamento ? 'Selecciona departamento' : 'Seleccionar municipio',
            options.municipios,
            onMunicipioChange,
            loading.municipios,
            !filters.departamento,
            !filters.departamento ? 'Selecciona un departamento primero' : undefined
          )}
        </Col>

        <Col xs={24} sm={8} md={8}>
          {renderSelect(
            'IPS',
            filters.ips,
            !filters.municipio ? 'Selecciona municipio' : 'Seleccionar IPS',
            options.ips,
            onIpsChange,
            loading.ips,
            !filters.municipio,
            !filters.municipio ? 'Selecciona un municipio primero' : undefined
          )}
        </Col>
      </Row>

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
