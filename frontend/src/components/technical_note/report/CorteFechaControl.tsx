// components/technical-note/report/CorteFechaControl.tsx - ✅ NUEVO COMPONENTE
import React, { memo } from 'react';
import { DatePicker, Form, Typography, Tooltip } from 'antd';
import { CalendarOutlined, InfoCircleOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';

const { Text } = Typography;

interface CorteFechaControlProps {
  corteFecha: string;
  onCorteFechaChange: (fecha: string) => void;
  disabled?: boolean;
}

export const CorteFechaControl: React.FC<CorteFechaControlProps> = memo(({
  corteFecha,
  onCorteFechaChange,
  disabled = false
}) => {
  const handleDateChange = (date: dayjs.Dayjs | null) => {
    if (date) {
      onCorteFechaChange(date.format('YYYY-MM-DD'));
    }
  };

  return (
    <Form.Item 
      label={
        <span>
          <CalendarOutlined style={{ marginRight: 4 }} />
          Fecha de Corte
          <Tooltip title="Fecha utilizada para calcular las edades en meses y años. Afecta el cálculo de numerador y denominador por rango específico.">
            <InfoCircleOutlined style={{ marginLeft: 4, color: '#999' }} />
          </Tooltip>
        </span>
      }
      style={{ marginBottom: 0 }}
    >
      <DatePicker
        value={corteFecha ? dayjs(corteFecha) : null}
        onChange={handleDateChange}
        format="YYYY-MM-DD"
        placeholder="Seleccionar fecha"
        disabled={disabled}
        style={{ width: '100%' }}
        showToday
        allowClear={false}
      />
      {corteFecha && (
        <Text type="secondary" style={{ fontSize: '12px', marginTop: 2, display: 'block' }}>
          Las edades se calcularán al {dayjs(corteFecha).format('DD/MM/YYYY')}
        </Text>
      )}
    </Form.Item>
  );
});

CorteFechaControl.displayName = 'CorteFechaControl';
