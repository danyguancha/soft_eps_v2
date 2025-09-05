import React, { useState, useEffect } from 'react';
import { Input, Button, Space, Checkbox, Spin, Row, Col, Typography } from 'antd';
import { SearchOutlined } from '@ant-design/icons';
import type { ExcelFilter } from '../../types/DataTable.types';
import type { FilterCondition } from '../../types/api.types';

const { Text } = Typography;

interface ExcelStyleFilterProps {
  columnName: string;
  filename: string | null;
  filter: ExcelFilter | undefined;
  onClose: () => void;
  onLoadUniqueValues: (columnName: string) => void;
  onUpdateFilter: (columnName: string, selectedValues: string[]) => void;
  onApplyFilter: (columnName: string, onFiltersChange: (filters: FilterCondition[]) => void) => void;
  onClearFilter: (columnName: string, onFiltersChange: (filters: FilterCondition[]) => void) => void;
  onFiltersChange: (filters: FilterCondition[]) => void;
}

export const ExcelStyleFilter: React.FC<ExcelStyleFilterProps> = ({
  columnName,
  filename,
  filter,
  onClose,
  onLoadUniqueValues,
  onUpdateFilter,
  onApplyFilter,
  onClearFilter,
  onFiltersChange
}) => {
  const [localSearchTerm, setLocalSearchTerm] = useState('');

  useEffect(() => {
    if (!filter || (!filter.allValues.length && !filter.loading)) {
      onLoadUniqueValues(columnName);
    }
  }, [columnName, filter, onLoadUniqueValues]);

  if (!filter || filter.loading) {
    return (
      <div style={{ padding: '16px', minWidth: '280px', textAlign: 'center' }}>
        <Spin size="small" />
        <div style={{ marginTop: '8px' }}>
          <Text style={{ fontSize: '12px', color: '#666' }}>
            Cargando valores únicos...
          </Text>
        </div>
      </div>
    );
  }

  if (filter.allValues.length === 0) {
    return (
      <div style={{ padding: '16px', minWidth: '280px', textAlign: 'center' }}>
        <Text style={{ fontSize: '12px', color: '#999' }}>
          No hay valores únicos disponibles
        </Text>
        <div style={{ marginTop: '12px' }}>
          <Button size="small" onClick={onClose}>
            Cerrar
          </Button>
        </div>
      </div>
    );
  }

  const filteredValues = filter.allValues.filter(value =>
    localSearchTerm ? value.toLowerCase().includes(localSearchTerm.toLowerCase()) : true
  );

  const allSelected = filter.selectedValues.length === filter.allValues.length;
  const indeterminate = filter.selectedValues.length > 0 && filter.selectedValues.length < filter.allValues.length;

  return (
    <div style={{ padding: '8px', minWidth: '280px', maxHeight: '400px' }}>
      {/* Header */}
      <div style={{ marginBottom: '8px', borderBottom: '1px solid #f0f0f0', paddingBottom: '8px' }}>
        <Text strong style={{ fontSize: '12px' }}>
          Filtrar: {columnName}
        </Text>
        <br />
        <Text type="secondary" style={{ fontSize: '10px' }}>
          {filename ? '🌐 Valores del servidor' : 'Valores locales (datos de cruce)'}
        </Text>
      </div>

      {/* Buscador interno */}
      <div style={{ marginBottom: '8px' }}>
        <Input
          size="small"
          placeholder={`Buscar en ${columnName}...`}
          value={localSearchTerm}
          onChange={(e) => setLocalSearchTerm(e.target.value)}
          prefix={<SearchOutlined />}
          allowClear
        />
      </div>

      {/* Seleccionar todos */}
      <div style={{ marginBottom: '8px', borderBottom: '1px solid #f0f0f0', paddingBottom: '8px' }}>
        <Checkbox
          indeterminate={indeterminate}
          checked={allSelected}
          onChange={(e) => {
            const newSelectedValues = e.target.checked ? [...filter.allValues] : [];
            onUpdateFilter(columnName, newSelectedValues);
          }}
        >
          <Text strong style={{ fontSize: '12px' }}>
            Seleccionar todos ({filter.allValues.length})
          </Text>
        </Checkbox>
      </div>

      {/* Lista de valores con checkboxes */}
      <div style={{ maxHeight: '200px', overflowY: 'auto', marginBottom: '12px' }}>
        <Space direction="vertical" size="small" style={{ width: '100%' }}>
          {filteredValues.slice(0, 100).map(value => (
            <Checkbox
              key={value}
              checked={filter.selectedValues.includes(value)}
              onChange={(e) => {
                const newSelectedValues = e.target.checked
                  ? [...filter.selectedValues, value]
                  : filter.selectedValues.filter(v => v !== value);
                
                onUpdateFilter(columnName, newSelectedValues);
              }}
              style={{ fontSize: '11px', width: '100%' }}
            >
              <Text 
                style={{ fontSize: '11px' }} 
                title={value}
                ellipsis={{ tooltip: true }}
              >
                {value || '(Vacío)'}
              </Text>
            </Checkbox>
          ))}
        </Space>
      </div>

      {filteredValues.length > 100 && (
        <div style={{ textAlign: 'center', padding: '4px', fontSize: '10px', color: '#666', marginBottom: '8px' }}>
          Mostrando 100 de {filteredValues.length} valores. Use el buscador para encontrar más.
        </div>
      )}

      {filteredValues.length === 0 && localSearchTerm && (
        <div style={{ textAlign: 'center', padding: '16px', color: '#999', marginBottom: '8px' }}>
          <Text style={{ fontSize: '11px' }}>No se encontraron valores que coincidan</Text>
        </div>
      )}

      {/* Botones de acción */}
      <div style={{ borderTop: '1px solid #f0f0f0', paddingTop: '8px' }}>
        <Row gutter={8}>
          <Col span={12}>
            <Button
              type="primary"
              size="small"
              block
              onClick={() => {
                onApplyFilter(columnName, onFiltersChange);
                onClose();
              }}
              disabled={filter.selectedValues.length === 0}
            >
              Aplicar ({filter.selectedValues.length})
            </Button>
          </Col>
          <Col span={12}>
            <Button 
              size="small" 
              block 
              onClick={() => {
                onClearFilter(columnName, onFiltersChange);
                onClose();
              }}
            >
              🗑️ Limpiar
            </Button>
          </Col>
        </Row>
      </div>
    </div>
  );
};
