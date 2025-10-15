import React, { useMemo } from 'react';
import { Table, Input, Button, Space, Popconfirm, Card, Row, Col, Badge, Tooltip, Tag, Dropdown, type MenuProps } from 'antd';
import { SearchOutlined, DeleteOutlined, FilterOutlined, MoreOutlined, ClearOutlined, ExportOutlined, SortAscendingOutlined, SortDescendingOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import type { DataTableProps } from '../../types/DataTable.types';
import { useDataTable } from '../../hooks/useDataTable';
import { ExcelStyleFilter } from './ExcelStyleFilter';
import { calculateColumnWidth, isColumnFiltered } from '../../utils/ColumnUtils';
import './DataTable.css';


export const DataTable: React.FC<DataTableProps> = ({
  data,
  columns,
  loading,
  filename: rawFilename, // Renombrar para procesar
  pagination,
  onPaginationChange,
  onFiltersChange,
  onSortChange,
  onDeleteRows,
  onSearch
}) => {
  // Normalizar filename para evitar undefined
  const filename: string | null = rawFilename ?? null;

  const {
    selectedRowKeys,
    setSelectedRowKeys,
    searchTerm,
    setSearchTerm,
    sortState,
    hasActiveFilters,
    hasSelectedRows,
    displayData,
    paginatedDisplayData,
    finalPagination,
    handleTableChange,
    handleDeleteSelected,
    handleGlobalSearch,
    handleLocalPaginationChange,
    excelFilters,
    setExcelFilters,
    loadUniqueValues,
    applyExcelFilter,
    clearExcelFilter,
    isLocalFiltering,
    clearAllFilters
  } = useDataTable(
    filename, // Ahora es string | null limpio
    data,
    pagination,
    onPaginationChange,
    onFiltersChange,
    onSortChange,
    onDeleteRows,
    onSearch
  );

  // Columnas de la tabla
  const tableColumns: ColumnsType<Record<string, any>> = useMemo(() => {
    return columns.map((columnName) => {
      const isFiltered = isColumnFiltered(columnName, excelFilters);
      const columnWidth = calculateColumnWidth(columnName);
      
      return {
        title: (
          <div className="header-content">
            <span className="header-text">
              {columnName}
            </span>
            <div className="header-icons">
              {isFiltered && (
                <Badge dot size="small">
                  <FilterOutlined className="header-icon filter-icon" />
                </Badge>
              )}
              {sortState?.column === columnName && (
                sortState.direction === 'asc' ? 
                  <SortAscendingOutlined className="header-icon sort-icon" /> :
                  <SortDescendingOutlined className="header-icon sort-icon" />
              )}
            </div>
          </div>
        ),
        dataIndex: columnName,
        key: columnName,
        width: columnWidth,
        ellipsis: false,
        sorter: true,
        sortOrder: sortState?.column === columnName ? 
          (sortState.direction === 'asc' ? 'ascend' : 'descend') : null,
        
        render: (text: any) => (
          <Tooltip placement="topLeft" title={text}>
            <div className="cell-content">
              {text !== null && text !== undefined ? String(text) : '-'}
            </div>
          </Tooltip>
        ),

        filterDropdown: ({ close }) => (
          <ExcelStyleFilter
            columnName={columnName}
            filename={filename}
            filter={excelFilters[columnName]}
            onClose={close}
            onLoadUniqueValues={loadUniqueValues}
            onUpdateFilter={(columnName: string, selectedValues: string[]) => { // Tipado explícito
              setExcelFilters(prev => ({
                ...prev,
                [columnName]: {
                  ...prev[columnName],
                  selectedValues
                }
              }));
            }}
            onApplyFilter={applyExcelFilter}
            onClearFilter={clearExcelFilter}
            onFiltersChange={onFiltersChange}
          />
        ),
        
        filterIcon: (filtered: boolean) => (
          <FilterOutlined style={{ color: isFiltered ? '#1890ff' : undefined }} />
        )
      };
    });
  }, [columns, excelFilters, sortState, filename, loadUniqueValues, setExcelFilters, applyExcelFilter, clearExcelFilter, onFiltersChange]);

  const rowSelection = {
    selectedRowKeys,
    onChange: (newSelectedRowKeys: React.Key[]) => {
      setSelectedRowKeys(newSelectedRowKeys);
    },
    columnWidth: 30,
  };

  // Menu correctamente tipado
  const actionsMenu: MenuProps = {
    items: [
      {
        key: 'export',
        label: 'Exportar datos',
        icon: <ExportOutlined />,
      },
      {
        key: 'clear-filters',
        label: 'Limpiar todos los filtros',
        icon: <ClearOutlined />,
      },
    ],
    onClick: ({ key }: { key: string }) => { // Tipado explícito del destructuring
      switch (key) {
        case 'clear-filters':
          clearAllFilters();
          break;
        case 'export':
          console.log('Exportar datos');
          break;
        default:
          break;
      }
    },
  };

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
                  <Tag 
                    color="blue" 
                    closable 
                    onClose={(e: React.MouseEvent) => { // Tipado explícito
                      e.preventDefault();
                      clearAllFilters();
                    }}
                  >
                    Filtros {filename ? 'del servidor' : 'locales'} activos
                  </Tag>
                )}
                {sortState && (
                  <Tag color="green">
                    Ordenado: {sortState.column} ({sortState.direction})
                  </Tag>
                )}
                <Tag color="purple">
                  {columns.length} columnas
                </Tag>
                {!filename && isLocalFiltering && (
                  <Tag color="orange">
                    🔍 {displayData.length} de {data.length} registros
                  </Tag>
                )}
              </div>
            </Col>

            <Col xs={24} sm={24} md={8}>
              <div className="data-table-actions">
                <Space>
                  <Popconfirm
                    title={`¿Eliminar ${selectedRowKeys.length} fila(s)?`}
                    onConfirm={handleDeleteSelected}
                    disabled={!hasSelectedRows}
                  >
                    <Button
                      danger
                      icon={<DeleteOutlined />}
                      disabled={!hasSelectedRows}
                      size="small"
                    >
                      Eliminar ({selectedRowKeys.length})
                    </Button>
                  </Popconfirm>

                  <Dropdown menu={actionsMenu}>
                    <Button icon={<MoreOutlined />} size="small">
                      Más
                    </Button>
                  </Dropdown>
                </Space>
              </div>
            </Col>
          </Row>
        </div>

        {/* Tabla */}
        <div className="data-table-wrapper">
          <Table
            rowSelection={rowSelection}
            columns={tableColumns}
            dataSource={paginatedDisplayData.map((row, index) => ({ ...row, key: index }))}
            loading={loading}
            pagination={{
              ...finalPagination,
              showTotal: (total, range) => 
                `${range[0]}-${range[1]} de ${total.toLocaleString()} • Filtros ${filename ? 'del servidor' : 'locales'}`,
              onChange: handleLocalPaginationChange,
              onShowSizeChange: handleLocalPaginationChange, 
            }}
            onChange={handleTableChange}
            scroll={{ 
              x: 'max-content',
              y: 260
            }}
            size="small"
            bordered
            tableLayout="fixed"
            className="compact-table"
          />
        </div>
      </Card>
    </div>
  );
};
