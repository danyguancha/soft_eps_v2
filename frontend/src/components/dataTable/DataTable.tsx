import React, { useState, useCallback, useMemo } from 'react';
import { Table, Input, Select, Button, Space, Popconfirm, message, Dropdown, Card, Row, Col, Typography, Badge, Tooltip, Tag } from 'antd';
import { SearchOutlined, DeleteOutlined, FilterOutlined, MoreOutlined, ClearOutlined, ExportOutlined, SortAscendingOutlined, SortDescendingOutlined } from '@ant-design/icons';
import type { ColumnsType, TableProps } from 'antd/es/table';
import type { SorterResult } from 'antd/es/table/interface';
import type { FilterCondition, SortCondition } from '../../types/api.types';
import './DataTable.css';

const { Text } = Typography;
const { Option } = Select;

interface DataTableProps {
  data: Record<string, any>[];
  columns: string[];
  loading?: boolean;
  pagination: {
    current: number;
    pageSize: number;
    total: number;
    showSizeChanger: boolean;
    showQuickJumper: boolean;
    size?: 'default' | 'small';
  };
  onPaginationChange: (page: number, pageSize: number) => void;
  onFiltersChange: (filters: FilterCondition[]) => void;
  onSortChange: (sort: SortCondition[]) => void;
  onDeleteRows: (indices: number[]) => void;
  onSearch: (searchTerm: string) => void;
}

interface ColumnFilter {
  column: string;
  operator: string;
  value: string;
}

export const DataTable: React.FC<DataTableProps> = ({
  data,
  columns,
  loading,
  pagination,
  onPaginationChange,
  onFiltersChange,
  onSortChange,
  onDeleteRows,
  onSearch
}) => {
  const [selectedRowKeys, setSelectedRowKeys] = useState<React.Key[]>([]);
  const [columnFilters, setColumnFilters] = useState<Record<string, ColumnFilter>>({});
  const [searchTerm, setSearchTerm] = useState('');
  const [sortState, setSortState] = useState<{ column: string; direction: 'asc' | 'desc' } | null>(null);

  const filterOperators = [
    { value: 'contains', label: 'Contiene' },
    { value: 'equals', label: 'Igual a' },
    { value: 'starts_with', label: 'Empieza con' },
    { value: 'ends_with', label: 'Termina con' },
    { value: 'gt', label: 'Mayor que' },
    { value: 'lt', label: 'Menor que' },
    { value: 'gte', label: 'Mayor o igual' },
    { value: 'lte', label: 'Menor o igual' },
    { value: 'is_null', label: 'Es nulo' },
    { value: 'is_not_null', label: 'No es nulo' }
  ];

  const handleTableChange: TableProps<any>['onChange'] = useCallback(
    (_pagination: any, _filters: any, sorter: any[] | SorterResult<any>, _extra: any) => {
      if (sorter && !Array.isArray(sorter)) {
        const singleSorter = sorter as SorterResult<any>;
        if (singleSorter.field && singleSorter.order) {
          const direction = singleSorter.order === 'ascend' ? 'asc' : 'desc';
          setSortState({
            column: singleSorter.field as string,
            direction
          });
          
          onSortChange([{
            column: singleSorter.field as string,
            direction
          }]);
        }
      } else if (Array.isArray(sorter)) {
        const firstSorter = sorter[0];
        if (firstSorter?.field && firstSorter?.order) {
          const direction = firstSorter.order === 'ascend' ? 'asc' : 'desc';
          setSortState({
            column: firstSorter.field as string,
            direction
          });
          
          onSortChange([{
            column: firstSorter.field as string,
            direction
          }]);
        }
      } else {
        setSortState(null);
        onSortChange([]);
      }
    }, 
    [onSortChange]
  );

  // ===== COLUMNAS CON ANCHOS FIJOS Y ICONOS ALINEADOS =====
  const tableColumns: ColumnsType<Record<string, any>> = useMemo(() => {
    return columns.map((columnName, index) => {
      const currentFilter = columnFilters[columnName];
      
      // ===== CALCULAR ANCHOS ESPECÍFICOS PARA CADA COLUMNA =====
      let columnWidth = 120; // Default
      const textLength = columnName.length;
      
      // Calcular ancho basado en el nombre de la columna y contenido típico
      if (textLength <= 5) columnWidth = 80;
      else if (textLength <= 10) columnWidth = 120;
      else if (textLength <= 15) columnWidth = 150;
      else if (textLength <= 20) columnWidth = 180;
      else columnWidth = 200;

      // Ajustes específicos para tipos de columnas comunes
      if (columnName.toLowerCase().includes('id')) columnWidth = 100;
      if (columnName.toLowerCase().includes('nombre')) columnWidth = 160;
      if (columnName.toLowerCase().includes('apellido')) columnWidth = 160;
      if (columnName.toLowerCase().includes('documento')) columnWidth = 140;
      if (columnName.toLowerCase().includes('municipio')) columnWidth = 140;
      if (columnName.toLowerCase().includes('gestor')) columnWidth = 200;
      if (columnName.toLowerCase().includes('medicamento')) columnWidth = 180;
      if (columnName.toLowerCase().includes('diagnóstico')) columnWidth = 180;
      
      return {
        title: (
          <div style={{ 
            display: 'flex', 
            alignItems: 'center', 
            justifyContent: 'space-between',
            width: '100%',
            minHeight: '24px',
            padding: '2px 0'
          }}>
            <span style={{ 
              flex: 1,
              fontSize: '9px',
              fontWeight: '600',
              lineHeight: '1.2',
              textAlign: 'left',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap'
            }}>
              {columnName}
            </span>
            <div style={{ 
              display: 'flex', 
              alignItems: 'center',
              gap: '2px',
              marginLeft: '4px',
              flexShrink: 0
            }}>
              {currentFilter && (
                <Badge dot size="small">
                  <FilterOutlined style={{ 
                    color: '#1890ff', 
                    fontSize: '8px' 
                  }} />
                </Badge>
              )}
              {sortState?.column === columnName && (
                sortState.direction === 'asc' ? 
                  <SortAscendingOutlined style={{ 
                    color: '#1890ff', 
                    fontSize: '8px' 
                  }} /> :
                  <SortDescendingOutlined style={{ 
                    color: '#1890ff', 
                    fontSize: '8px' 
                  }} />
              )}
            </div>
          </div>
        ),
        dataIndex: columnName,
        key: columnName,
        width: columnWidth, // ===== ANCHO FIJO CALCULADO =====
        ellipsis: {
          showTitle: false,
        },
        sorter: true,
        sortOrder: sortState?.column === columnName ? 
          (sortState.direction === 'asc' ? 'ascend' : 'descend') : null,
        
        render: (text: any) => (
          <Tooltip placement="topLeft" title={text}>
            <div className="data-table-cell-content">
              {text !== null && text !== undefined ? String(text) : '-'}
            </div>
          </Tooltip>
        ),

        filterDropdown: () => (
          <div className="data-table-filter-dropdown">
            <Space direction="vertical" style={{ width: '100%' }}>
              <Row gutter={8}>
                <Col span={24}>
                  <Text className="data-table-filter-title">Filtrar: {columnName}</Text>
                </Col>
              </Row>
              
              <Row gutter={8}>
                <Col span={12}>
                  <Select
                    placeholder="Operador"
                    value={currentFilter?.operator || 'contains'}
                    onChange={(operator) => {
                      setColumnFilters(prev => ({
                        ...prev,
                        [columnName]: { 
                          ...prev[columnName], 
                          column: columnName, 
                          operator 
                        }
                      }));
                    }}
                    style={{ width: '100%' }}
                    size="small"
                  >
                    {filterOperators.map(op => (
                      <Option key={op.value} value={op.value}>{op.label}</Option>
                    ))}
                  </Select>
                </Col>
                
                <Col span={12}>
                  <Input
                    placeholder="Valor"
                    value={currentFilter?.value || ''}
                    onChange={(e) => {
                      setColumnFilters(prev => ({
                        ...prev,
                        [columnName]: {
                          ...prev[columnName],
                          column: columnName,
                          operator: prev[columnName]?.operator || 'contains',
                          value: e.target.value
                        }
                      }));
                    }}
                    size="small"
                    disabled={
                      currentFilter?.operator === 'is_null' || 
                      currentFilter?.operator === 'is_not_null'
                    }
                  />
                </Col>
              </Row>

              <Row gutter={8}>
                <Col span={12}>
                  <Button
                    type="primary"
                    onClick={() => applyColumnFilter(columnName)}
                    icon={<FilterOutlined />}
                    size="small"
                    block
                  >
                    Aplicar
                  </Button>
                </Col>
                <Col span={12}>
                  <Button
                    onClick={() => clearColumnFilter(columnName)}
                    size="small"
                    block
                  >
                    Limpiar
                  </Button>
                </Col>
              </Row>
            </Space>
          </div>
        ),
        
        filterIcon: (filtered: boolean) => (
          <FilterOutlined style={{ color: filtered ? '#1890ff' : undefined }} />
        )
      };
    });
  }, [columns, columnFilters, sortState]);

  // Resto de funciones (sin cambios)
  const applyColumnFilter = useCallback((columnName: string) => {
    const filter = columnFilters[columnName];
    if (!filter) return;

    const activeFilters = Object.values(columnFilters)
      .filter(f => f && f.value !== undefined && f.value !== '')
      .map(f => ({
        column: f.column,
        operator: f.operator as any,
        value: f.value
      }));

    onFiltersChange(activeFilters);
    message.success(`Filtro aplicado en ${columnName}`);
  }, [columnFilters, onFiltersChange]);

  const clearColumnFilter = useCallback((columnName: string) => {
    setColumnFilters(prev => {
      const newFilters = { ...prev };
      delete newFilters[columnName];
      return newFilters;
    });

    const remainingFilters = Object.values(columnFilters)
      .filter(f => f && f.column !== columnName && f.value !== undefined && f.value !== '')
      .map(f => ({
        column: f.column,
        operator: f.operator as any,
        value: f.value
      }));

    onFiltersChange(remainingFilters);
    message.success(`Filtro removido de ${columnName}`);
  }, [columnFilters, onFiltersChange]);

  const clearAllFilters = useCallback(() => {
    setColumnFilters({});
    onFiltersChange([]);
    message.success('Todos los filtros removidos');
  }, [onFiltersChange]);

  const rowSelection = {
    selectedRowKeys,
    onChange: (newSelectedRowKeys: React.Key[]) => {
      setSelectedRowKeys(newSelectedRowKeys);
    },
    onSelectAll: (selected: boolean) => {
      if (selected) {
        const allKeys = data.map((_, index) => index);
        setSelectedRowKeys(allKeys);
      } else {
        setSelectedRowKeys([]);
      }
    },
  };

  const handleDeleteSelected = useCallback(() => {
    const indices = selectedRowKeys.map(key => Number(key));
    onDeleteRows(indices);
    setSelectedRowKeys([]);
  }, [selectedRowKeys, onDeleteRows]);

  const handleGlobalSearch = useCallback(() => {
    onSearch(searchTerm);
    if (searchTerm) {
      message.success(`Buscando: "${searchTerm}"`);
    }
  }, [searchTerm, onSearch]);

  const actionsMenu = {
    items: [
      {
        key: 'export',
        label: 'Exportar datos',
        icon: <ExportOutlined />,
      },
      {
        key: 'clear-filters',
        label: 'Limpiar filtros',
        icon: <ClearOutlined />,
        onClick: clearAllFilters,
      },
    ],
  };

  const hasActiveFilters = Object.keys(columnFilters).length > 0;
  const hasSelectedRows = selectedRowKeys.length > 0;

  // ===== CALCULAR ANCHO TOTAL DE LA TABLA =====
  const totalWidth = tableColumns.reduce((sum, col) => sum + (col.width as number || 120), 0);

  return (
    <div className="data-table-container">
      <Card className="data-table-card">
        {/* Barra de herramientas superior */}
        <div className="data-table-toolbar">
          <Row gutter={16} align="middle">
            <Col xs={24} sm={12} md={8}>
              <Input.Search
                placeholder="Buscar en todos los campos..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                onSearch={handleGlobalSearch}
                enterButton={<SearchOutlined />}
                allowClear
              />
            </Col>

            <Col xs={24} sm={12} md={8}>
              <div className="data-table-status-tags">
                {hasActiveFilters && (
                  <Tag color="blue" closable onClose={clearAllFilters}>
                    {Object.keys(columnFilters).length} filtro(s) activo(s)
                  </Tag>
                )}
                {sortState && (
                  <Tag color="green">
                    Ordenado por: {sortState.column} ({sortState.direction})
                  </Tag>
                )}
              </div>
            </Col>

            <Col xs={24} sm={24} md={8}>
              <div className="data-table-actions">
                <Space>
                  <Popconfirm
                    title={`¿Eliminar ${selectedRowKeys.length} fila(s) seleccionada(s)?`}
                    onConfirm={handleDeleteSelected}
                    disabled={!hasSelectedRows}
                    placement="topRight"
                  >
                    <Button
                      danger
                      icon={<DeleteOutlined />}
                      disabled={!hasSelectedRows}
                    >
                      Eliminar ({selectedRowKeys.length})
                    </Button>
                  </Popconfirm>

                  <Dropdown menu={actionsMenu} placement="bottomRight">
                    <Button icon={<MoreOutlined />}>
                      Más acciones
                    </Button>
                  </Dropdown>
                </Space>
              </div>
            </Col>
          </Row>
        </div>

        <div className="data-table-info">
          <Row>
            <Col span={24}>
              <Text type="secondary">
                Mostrando {data.length} de {pagination.total} registros
                {hasSelectedRows && ` • ${selectedRowKeys.length} seleccionada(s)`}
              </Text>
            </Col>
          </Row>
        </div>

        {/* ===== TABLA CON ANCHOS FIJOS Y ALINEACIÓN CORRECTA ===== */}
        <div className="data-table-wrapper">
          <Table
            rowSelection={rowSelection}
            columns={tableColumns}
            dataSource={data.map((row, index) => ({ ...row, key: index }))}
            loading={loading}
            pagination={{
              ...pagination,
              showTotal: (total, range) => 
                `${range[0]}-${range[1]} de ${total} registros`,
              showSizeChanger: true,
              showQuickJumper: true,
              pageSizeOptions: ['50', '100', '200', '500'],
              onChange: onPaginationChange,
              onShowSizeChange: onPaginationChange,
            }}
            onChange={handleTableChange}
            scroll={{ 
              x: totalWidth,
              y: 300 // Altura fija
            }}
            size="small"
            bordered
            tableLayout="fixed" // ===== LAYOUT FIJO PARA ALINEACIÓN CORRECTA =====
            rowClassName={(record, index) => 
              selectedRowKeys.includes(index) ? 'table-row-selected' : ''
            }
          />
        </div>
      </Card>
    </div>
  );
};
