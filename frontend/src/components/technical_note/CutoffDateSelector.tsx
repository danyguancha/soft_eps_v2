// components/technical-note/CutoffDateSelector.tsx
import React from 'react';
import { Card, DatePicker, Alert, Space, Typography } from 'antd';
import { CalendarOutlined } from '@ant-design/icons';
import dayjs, { Dayjs } from 'dayjs';
import 'dayjs/locale/es';
import locale from 'antd/es/date-picker/locale/es_ES';

dayjs.locale('es');

const { Text } = Typography;

interface CutoffDateSelectorProps {
  selectedDate: Dayjs | null;
  onDateChange: (date: Dayjs | null) => void;
}

export const CutoffDateSelector: React.FC<CutoffDateSelectorProps> = ({
  selectedDate,
  onDateChange
}) => {
  return (
    <Card
      style={{ 
        marginBottom: 24, 
        border: selectedDate ? '2px solid #52c41a' : '2px solid #ff4d4f',
        boxShadow: selectedDate 
          ? '0 4px 12px rgba(82, 196, 26, 0.2)' 
          : '0 4px 12px rgba(255, 77, 79, 0.2)',
          fontSize: 9,
        
      }}
    >
      <Space direction="vertical" style={{ width: '100%' }} size="middle">
        <Space align="center">
          <CalendarOutlined 
            style={{ 
              fontSize: 24, 
              color: selectedDate ? '#52c41a' : '#ff4d4f' 
            }} 
          />
          <Text strong style={{ fontSize: 16 }}>
            Fecha de Corte (Obligatoria)
          </Text>
        </Space>

        {!selectedDate && (
          <Alert
            message="⚠️ Debe seleccionar una fecha de corte antes de continuar"
            description="La fecha de corte es obligatoria para cargar y visualizar archivos. Por favor, seleccione una fecha del calendario."
            type="error"
            showIcon
            style={{ marginBottom: 12, width: '100%', height: 'auto', fontSize: 10 }}
          />
        )}

        <Space direction="vertical" style={{ width: '100%' }}>
          <Text type="secondary">
            Seleccione la fecha de corte para el análisis de datos:
          </Text>
          <DatePicker
            value={selectedDate}
            onChange={onDateChange}
            format="DD/MM/YYYY"
            placeholder="Seleccione fecha de corte"
            size="large"
            style={{ width: '100%', maxWidth: 400 }}
            allowClear
            status={!selectedDate ? 'error' : undefined}
            locale={locale} // USAR LOCALE OFICIAL DE ANT DESIGN
          />
          
          {selectedDate && (
            <Alert
              message={`✓ Fecha de corte seleccionada: ${selectedDate.format('DD/MM/YYYY')}`}
              type="success"
              showIcon
              style={{ marginTop: 8 }}
            />
          )}
        </Space>
      </Space>
    </Card>
  );
};
