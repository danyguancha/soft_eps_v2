// src/components/tabs/tabs/CrossTab/CrossTab.tsx - VERSIÓN SIN ADVERTENCIA FALSA
import React from 'react';
import { Card, Alert, Button, Space } from 'antd';
import { DownloadOutlined } from '@ant-design/icons';
import type { TabProps } from '../../../types/api.types';
import { DataTable } from '../../dataTable/DataTable';


export const CrossTab: React.FC<TabProps> = ({ 
  isMobile, 
  onTabChange,
  crossResult,
  processedCrossData,
  crossDataTotal,
  onExportCrossResult,
  onClearCrossResult,
}) => {

  if (!crossResult) {
    return (
      <div className="content-container">
        <Card title="Resultado del Cruce">
          <Alert
            message="Sin resultado de cruce"
            description="Realiza un cruce de archivos desde la sección 'Transformar' para ver los resultados aquí."
            type="info"
            showIcon
            action={
              <Button
                size="small"
                type="primary"
                onClick={() => onTabChange('transform')}
              >
                Ir a Transformar
              </Button>
            }
          />
        </Card>
      </div>
    );
  }

  // ✅ VERIFICACIÓN MÁS INTELIGENTE
  const hasAllData = processedCrossData?.length === crossDataTotal;
  const hasPartialData = processedCrossData && processedCrossData.length > 0;
  
  console.log('CrossTab - Análisis de datos:', {
    processedCrossDataLength: processedCrossData?.length,
    crossDataTotal: crossDataTotal,
    hasAllData: hasAllData,
    hasPartialData: hasPartialData,
    ratio: processedCrossData?.length && crossDataTotal ? 
      `${((processedCrossData.length / crossDataTotal) * 100).toFixed(1)}%` : '0%'
  });

  return (
    <div className="content-container">
      {/* Información del resultado del cruce */}
      <Alert
        message="Cruce completado exitosamente"
        description={`${crossDataTotal?.toLocaleString()} registros con ${crossResult.columns?.length} columnas`}
        type="success"
        style={{ marginBottom: 16 }}
        action={
          <Space>
            <Button
              type="primary"
              icon={<DownloadOutlined />}
              onClick={() => onExportCrossResult?.('csv')}
            >
              {isMobile ? 'CSV' : 'Exportar CSV'}
            </Button>
            <Button onClick={onClearCrossResult} danger>
              {isMobile ? 'Limpiar' : 'Limpiar Resultado'}
            </Button>
          </Space>
        }
      />

      <Card
        className="data-table-card"
        title={
          <div className="table-header">
            <div className="table-title">
              <span>📋 Resultado del Cruce</span>
            </div>
          </div>
        }
      >
        <DataTable
          data={processedCrossData || []} 
          columns={crossResult?.columns ?? []}
          filename={null} 
          loading={false}
          pagination={{
            current: 1, 
            pageSize: 20,
            total: processedCrossData?.length || 0,
            showSizeChanger: !isMobile,
            showQuickJumper: !isMobile,
            size: isMobile ? 'small' : 'default',
          }}
          onPaginationChange={() => {}} 
          onFiltersChange={() => {}} 
          onSortChange={() => {}} 
          onDeleteRows={() => {}} 
          onSearch={() => {}} 
        />
      </Card>
    </div>
  );
};
