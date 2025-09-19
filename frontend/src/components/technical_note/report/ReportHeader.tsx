// components/technical-note/report/ReportHeader.tsx - ✅ ACTUALIZADO
import React, { memo } from 'react';
import { Typography, Tag, Space } from 'antd';
import { CheckCircleOutlined, BarChartOutlined } from '@ant-design/icons';
import type { GlobalStatistics } from '../../../services/TechnicalNoteService';

const { Text } = Typography;

// ✅ INTERFAZ ACTUALIZADA
export interface ReportHeaderProps {
  reportTitle: string;
  hasGeoFilters: boolean;
  geographicFilters: any;
  hasReport: boolean;
  loadingReport: boolean;
  // ✅ NUEVOS CAMPOS
  globalStats?: GlobalStatistics | null;
  metodo?: string;
}

export const ReportHeader: React.FC<ReportHeaderProps> = memo(({
  reportTitle,
  hasGeoFilters,
  geographicFilters,
  hasReport,
  loadingReport,
  globalStats,  // ✅ NUEVO
  metodo       // ✅ NUEVO
}) => {
  return (
    <Space direction="vertical" size={4}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        <Text strong style={{ fontSize: '16px' }}>{reportTitle}</Text>
        
        {/* ✅ NUEVO: Indicador de método */}
        {metodo?.includes('NUMERADOR_DENOMINADOR') && (
          <Tag color="green" icon={<BarChartOutlined />}>
            N/D
          </Tag>
        )}
        
        {hasReport && !loadingReport && (
          <Tag color="success" icon={<CheckCircleOutlined />}>
            Activo
          </Tag>
        )}
      </div>

      {/* Filtros geográficos */}
      {hasGeoFilters && (
        <Text type="secondary" style={{ fontSize: '12px' }}>
          {[
            geographicFilters.departamento && `Dept: ${geographicFilters.departamento}`,
            geographicFilters.municipio && `Mun: ${geographicFilters.municipio}`,
            geographicFilters.ips && `IPS: ${geographicFilters.ips}`
          ].filter(Boolean).join(' → ')}
        </Text>
      )}

      {/* ✅ NUEVO: Estadísticas globales resumidas */}
      {globalStats && (
        <Text type="secondary" style={{ fontSize: '12px' }}>
          {globalStats.total_actividades} actividades • 
          Población: {globalStats.total_denominador_global.toLocaleString()} • 
          Cobertura: {globalStats.cobertura_global_porcentaje.toFixed(1)}%
        </Text>
      )}
    </Space>
  );
});

ReportHeader.displayName = 'ReportHeader';
