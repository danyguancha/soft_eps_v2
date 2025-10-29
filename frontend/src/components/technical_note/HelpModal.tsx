// components/technical-note/HelpModal.tsx
import React from 'react';
import { Modal, Typography, Divider, Space, Tag } from 'antd';
import { 
  CalendarOutlined, 
  FileOutlined, 
  FilterOutlined,
  InfoCircleOutlined 
} from '@ant-design/icons';

const { Title, Paragraph, Text } = Typography;

interface HelpModalProps {
  visible: boolean;
  onClose: () => void;
}

export const HelpModal: React.FC<HelpModalProps> = ({ visible, onClose }) => {
  return (
    <Modal
      title={
        <Space>
          <InfoCircleOutlined style={{ color: '#1890ff' }} />
          <Text strong>Guía de Uso - Nota Técnica</Text>
        </Space>
      }
      open={visible}
      onCancel={onClose}
      footer={null}
      width={600}
      centered
    >
      <Space direction="vertical" size="large" style={{ width: '100%' }}>
        
        {/* Paso 1 */}
        <div>
          <Title level={5}>
            <CalendarOutlined style={{ color: '#52c41a', marginRight: 8 }} />
            1. Seleccionar Fecha de Corte
          </Title>
          <Paragraph style={{ marginLeft: 24, marginBottom: 8 }}>
            La fecha de corte es <Tag color="red">obligatoria</Tag> y determina 
            el período de análisis de los datos.
          </Paragraph>
          <Paragraph type="secondary" style={{ marginLeft: 24, fontSize: 12 }}>
            Sin una fecha de corte seleccionada, no podrás cargar archivos ni 
            visualizar reportes.
          </Paragraph>
        </div>

        <Divider style={{ margin: '8px 0' }} />

        {/* Paso 2 */}
        <div>
          <Title level={5}>
            <FileOutlined style={{ color: '#1890ff', marginRight: 8 }} />
            2. Seleccionar Archivo
          </Title>
          <Paragraph style={{ marginLeft: 24, marginBottom: 8 }}>
            Tienes dos opciones para visualizar datos:
          </Paragraph>
          <ul style={{ marginLeft: 40 }}>
            <li>
              <Text strong>Curso de Vida:</Text> Selecciona uno de los cursos 
              de vida disponibles en el sistema
            </li>
            <li style={{ marginTop: 4 }}>
              <Text strong>Archivo Personalizado:</Text> Carga tu propio archivo 
              usando el botón "Cargar Archivo"
            </li>
          </ul>
          <Text strong>Nota:</Text>
          <Text style={{ marginLeft: 4, fontSize: 12, color: '#f41c1cff' }}>
            Los datos de los archivos están basados en la estructura definida en el software de
            <strong style={{color:'#046b17ff'}}> SIGIRES PROCEX</strong>, 
            por favor asegúrate de seguirla al cargar archivos personalizados.
          </Text>
        </div>

        <Divider style={{ margin: '8px 0' }} />

        {/* Paso 3 */}
        <div>
          <Title level={5}>
            <FilterOutlined style={{ color: '#722ed1', marginRight: 8 }} />
            3. Aplicar Filtros (Opcional)
          </Title>
          <Paragraph style={{ marginLeft: 24, marginBottom: 8 }}>
            Puedes filtrar los datos por:
          </Paragraph>
          <ul style={{ marginLeft: 40 }}>
            <li>Departamento</li>
            <li style={{ marginTop: 4 }}>Municipio</li>
            <li style={{ marginTop: 4 }}>Centro Zonal</li>
          </ul>
          <Paragraph type="secondary" style={{ marginLeft: 24, fontSize: 12 }}>
            Los filtros se aplican automáticamente y puedes limpiarlos 
            con el botón "Limpiar filtros".
          </Paragraph>
        </div>

        <Divider style={{ margin: '8px 0' }} />

        {/* Nota adicional */}
        <div style={{ 
          background: '#f0f5ff', 
          padding: 12, 
          borderRadius: 6,
          border: '1px solid #adc6ff'
        }}>
          <Text strong style={{ color: '#1890ff' }}>
            💡 Consejo:
          </Text>
          <Paragraph style={{ marginBottom: 0, marginTop: 4, fontSize: 12 }}>
            Usa el botón "Actualizar" para recargar la lista de archivos 
            disponibles después de realizar cambios en el sistema.
          </Paragraph>
        </div>

      </Space>
    </Modal>
  );
};
