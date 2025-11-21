// components/technical-note/report/ReportControls.tsx - ACTUALIZADO
import { EyeInvisibleOutlined, EyeOutlined } from '@ant-design/icons';
import { Button, Space, Statistic, Switch, Tooltip } from 'antd';
import React, { memo } from 'react';

// INTERFAZ ACTUALIZADA
export interface ReportControlsProps {
  hasReport: boolean;
  reportTotalRecords: number;
  showTemporalData: boolean;
  showReport: boolean;
  onSetShowTemporalData: (show: boolean) => void;
  onToggleReportVisibility: () => void;
  // NUEVOS CAMPOS NUMERADOR/DENOMINADOR
  totalDenominador?: number;
  totalNumerador?: number;
  coberturaGlobal?: number;
}

export const ReportControls: React.FC<ReportControlsProps> = memo(({
  hasReport,
  reportTotalRecords,
  showTemporalData,
  showReport,
  onSetShowTemporalData,
  onToggleReportVisibility,
  // NUEVOS PROPS
  totalDenominador = 0,
  totalNumerador = 0,
  coberturaGlobal = 0
}) => {
  const tieneNumeradorDenominador = totalDenominador > 0 && totalNumerador > 0;

  return (
    <Space size="large">
      {/* ESTADÍSTICAS NUMERADOR/DENOMINADOR O TRADICIONALES */}
      {tieneNumeradorDenominador ? (
        // Mostrar estadísticas N/D
        <Space size="middle">
          <Tooltip title="Población total evaluada">
            <Statistic
              title="Denominador"
              value={totalDenominador}
              formatter={(value) => value?.toLocaleString()}
              valueStyle={{ fontSize: '14px' }}
            />
          </Tooltip>
          
          <Tooltip title="Población con datos registrados">
            <Statistic
              title="Numerador"
              value={totalNumerador}
              formatter={(value) => value?.toLocaleString()}
              valueStyle={{ fontSize: '14px', color: '#52c41a' }}
            />
          </Tooltip>
          
          <Tooltip title="Porcentaje de cobertura global">
            <Statistic
              title="Cobertura"
              value={coberturaGlobal}
              suffix="%"
              precision={1}
              valueStyle={{ 
                fontSize: '14px', 
                color: coberturaGlobal >= 70 ? '#52c41a' : coberturaGlobal >= 50 ? '#fa8c16' : '#ff4d4f'
              }}
            />
          </Tooltip>
        </Space>
      ) : (
        // Mostrar estadísticas tradicionales
        <Statistic
          title="Registros"
          value={reportTotalRecords}
          formatter={(value) => value?.toLocaleString()}
          valueStyle={{ fontSize: '14px' }}
        />
      )}

      {/* Controles existentes */}
      {hasReport && (
        <Space>
          <Tooltip title="Mostrar/ocultar análisis temporal">
            <Switch
              checked={showTemporalData}
              onChange={onSetShowTemporalData}
              checkedChildren="Temporal"
              unCheckedChildren="Temporal"
              size="small"
            />
          </Tooltip>

          <Button
            type="text"
            icon={showReport ? <EyeInvisibleOutlined /> : <EyeOutlined />}
            onClick={onToggleReportVisibility}
            size="small"
          >
            {showReport ? 'Ocultar' : 'Mostrar'}
          </Button>
        </Space>
      )}
    </Space>
  );
});

ReportControls.displayName = 'ReportControls';
