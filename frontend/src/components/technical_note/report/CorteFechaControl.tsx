// components/technical-note/report/CorteFechaControl.tsx - VERSIÓN ACTUALIZADA (SOLO LECTURA)
import React, { memo } from 'react';
import { Alert, Typography, Space, Card } from 'antd';
import { CalendarOutlined, InfoCircleOutlined, LockOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import 'dayjs/locale/es';

dayjs.locale('es');

const { Text } = Typography;

interface CorteFechaControlProps {
  corteFecha: string; // Fecha recibida desde el componente padre (YYYY-MM-DD)
  showInfo?: boolean; // Mostrar información adicional
  variant?: 'compact' | 'card' | 'inline'; // Variantes de visualización
}

export const CorteFechaControl: React.FC<CorteFechaControlProps> = memo(({
  corteFecha,
  showInfo = true,
  variant = 'compact'
}) => {
  
  // Validar que haya fecha
  if (!corteFecha) {
    return (
      <Alert
        message="Fecha de corte no seleccionada"
        description="Debe seleccionar una fecha de corte en la sección principal antes de continuar."
        type="warning"
        showIcon
        icon={<CalendarOutlined />}
      />
    );
  }

  const formattedDate = dayjs(corteFecha).format('DD/MM/YYYY');
  const fullDate = dayjs(corteFecha).format('dddd, D [de] MMMM [de] YYYY');

  // VARIANTE COMPACT (Para formularios)
  if (variant === 'compact') {
    return (
      <div style={{
        padding: '12px',
        backgroundColor: '#f0f5ff',
        border: '1px solid #adc6ff',
        borderRadius: 6,
        marginBottom: 16
      }}>
        <Space direction="vertical" size={4} style={{ width: '100%' }}>
          <Space align="center">
            <CalendarOutlined style={{ color: '#1890ff', fontSize: 16 }} />
            <Text strong style={{ color: '#1890ff' }}>
              Fecha de Corte Establecida
            </Text>
            <LockOutlined style={{ fontSize: 12, color: '#8c8c8c' }} />
          </Space>
          
          <div style={{ paddingLeft: 20 }}>
            <Text style={{ fontSize: 18, fontWeight: 600, color: '#1890ff' }}>
              {formattedDate}
            </Text>
          </div>

          {showInfo && (
            <div style={{ paddingLeft: 20 }}>
              <Text type="secondary" style={{ fontSize: 12 }}>
                <InfoCircleOutlined style={{ marginRight: 4 }} />
                Las edades se calcularán al {formattedDate}
              </Text>
            </div>
          )}
        </Space>
      </div>
    );
  }

  // VARIANTE CARD (Para secciones destacadas)
  if (variant === 'card') {
    return (
      <Card
        size="small"
        style={{
          backgroundColor: '#f0f5ff',
          border: '2px solid #1890ff',
          marginBottom: 16
        }}
      >
        <Space direction="vertical" size={8} style={{ width: '100%', textAlign: 'center' }}>
          <CalendarOutlined style={{ fontSize: 32, color: '#1890ff' }} />
          
          <div>
            <Text type="secondary" style={{ fontSize: 12, display: 'block' }}>
              Fecha de Corte
            </Text>
            <Text style={{ fontSize: 24, fontWeight: 600, color: '#1890ff', display: 'block' }}>
              {formattedDate}
            </Text>
          </div>

          {showInfo && (
            <>
              <Text type="secondary" style={{ fontSize: 11 }}>
                {fullDate}
              </Text>
              
              <Alert
                message={
                  <Text style={{ fontSize: 11 }}>
                    <InfoCircleOutlined style={{ marginRight: 4 }} />
                    Las edades y rangos se calculan basándose en esta fecha
                  </Text>
                }
                type="info"
                style={{ fontSize: 11, padding: '4px 8px' }}
              />
            </>
          )}

          <div style={{ marginTop: 4 }}>
            <Space>
              <LockOutlined style={{ fontSize: 12, color: '#8c8c8c' }} />
              <Text type="secondary" style={{ fontSize: 11 }}>
                Fecha bloqueada (configurada en la sección principal)
              </Text>
            </Space>
          </div>
        </Space>
      </Card>
    );
  }

  // VARIANTE INLINE (Para mostrar en una línea)
  if (variant === 'inline') {
    return (
      <Space
        style={{
          padding: '8px 12px',
          backgroundColor: '#f0f5ff',
          border: '1px solid #adc6ff',
          borderRadius: 6,
          marginBottom: 8
        }}
      >
        <CalendarOutlined style={{ color: '#1890ff' }} />
        <Text strong>Fecha de Corte:</Text>
        <Text style={{ color: '#1890ff', fontWeight: 600 }}>
          {formattedDate}
        </Text>
        <LockOutlined style={{ fontSize: 12, color: '#8c8c8c' }} />
        {showInfo && (
          <Text type="secondary" style={{ fontSize: 12 }}>
            (Las edades se calculan a esta fecha)
          </Text>
        )}
      </Space>
    );
  }

  return null;
});

CorteFechaControl.displayName = 'CorteFechaControl';
