// components/technical-note/HelpModal.tsx
import React from 'react';
import { Modal, Typography, Divider, Space, Tag, Alert } from 'antd';
import { 
  CalendarOutlined, 
  FileOutlined, 
  FilterOutlined,
  InfoCircleOutlined,
  WarningOutlined,
  FileTextOutlined,
  DownloadOutlined,
  UserDeleteOutlined,
  ExportOutlined,
  FilePdfOutlined,
  FileExcelOutlined,
  FolderOpenOutlined,
  CopyOutlined
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
      width={800}
      centered
      style={{ maxHeight: '85vh' }}
      bodyStyle={{ maxHeight: '70vh', overflowY: 'auto' }}
    >
      <Space direction="vertical" size="large" style={{ width: '100%' }}>
        
        {/* Paso 1 - Procesar Archivos NT RPMS (NUEVO - PRIMERO) */}
        <div>
          <Title level={5}>
            <FolderOpenOutlined style={{ color: '#fa8c16', marginRight: 8 }} />
            1. Procesar Archivos NT RPMS (Paso Inicial)
          </Title>
          <Paragraph style={{ marginLeft: 24, marginBottom: 12 }}>
            <Text strong>Este es el primer paso obligatorio</Text> antes de realizar 
            cualquier análisis. Debe extraer la información de los archivos Excel NT RPMS en un único archivo para realizar
            el análisis de acuerdo a los servicios contratados por los prestadores.
          </Paragraph>
          
          <div style={{ marginLeft: 24, marginBottom: 12 }}>
            <Text strong>📖 ¿Cómo obtener la ruta de la carpeta?</Text>
            <ol style={{ marginLeft: 20, marginTop: 8, fontSize: 13 }}>
              <li style={{ marginBottom: 6 }}>
                Abra el <Text strong>Explorador de Archivos</Text> y navegue hasta la 
                carpeta con los archivos NT RPMS
              </li>
              <li style={{ marginBottom: 6 }}>
                Haga clic en la <Text strong>barra de direcciones</Text> (donde aparece la ruta)
              </li>
              <li style={{ marginBottom: 6 }}>
                La ruta completa se seleccionará automáticamente
              </li>
              <li style={{ marginBottom: 6 }}>
                Presione <Tag>Ctrl + C</Tag> para copiar la ruta
              </li>
              <li>
                Pegue la ruta en el campo con <Tag>Ctrl + V</Tag> y haga clic en 
                <Text strong> "Consolidar Archivos"</Text>
              </li>
            </ol>
          </div>

          <Alert
            message="📝 Ejemplos de rutas válidas"
            description={
              <Space direction="vertical" style={{ width: '100%', marginTop: 4 }}>
                <div>
                  <Text strong style={{ fontSize: 12 }}>Windows:</Text>
                  <ul style={{ marginBottom: 0, paddingLeft: 20, marginTop: 4 }}>
                    <li style={{ fontSize: 11, fontFamily: 'Consolas, monospace' }}>
                      C:\Users\USUARIO\archivos_a_evaluar
                    </li>
                    <li style={{ fontSize: 11, fontFamily: 'Consolas, monospace' }}>
                      D:\Datos\NT_RPMS\2025
                    </li>
                  </ul>
                </div>
              </Space>
            }
            type="info"
            showIcon
            icon={<CopyOutlined />}
            style={{ marginLeft: 24, fontSize: 11 }}
          />

          
        </div>

        <Divider style={{ margin: '8px 0' }} />

        {/* Paso 2 - Seleccionar Fecha de Corte */}
        <div>
          <Title level={5}>
            <CalendarOutlined style={{ color: '#52c41a', marginRight: 8 }} />
            2. Seleccionar Fecha de Corte
          </Title>
          <Paragraph style={{ marginLeft: 24, marginBottom: 8 }}>
            Después de consolidar los archivos, la fecha de corte es <Tag color="red">obligatoria</Tag> y 
            determina el período de análisis de los datos.
          </Paragraph>
          <Paragraph type="secondary" style={{ marginLeft: 24, fontSize: 12 }}>
            Sin una fecha de corte seleccionada, no podrás cargar archivos ni visualizar reportes.
          </Paragraph>
        </div>

        <Divider style={{ margin: '8px 0' }} />

        {/* Paso 3 - Seleccionar Archivo */}
        <div>
          <Title level={5}>
            <FileOutlined style={{ color: '#1890ff', marginRight: 8 }} />
            3. Seleccionar Archivo
          </Title>
          <Paragraph style={{ marginLeft: 24, marginBottom: 12 }}>
            <Text strong>Archivo Personalizado:</Text> Carga tu propio archivo 
            usando el botón "Cargar Archivo"
          </Paragraph>
          
          <Alert
            message={
              <Space>
                <FileTextOutlined style={{ fontSize: 18 }} />
                <Text strong style={{ fontSize: 16 }}>
                  IMPORTANTE: Estructura del Archivo
                </Text>
              </Space>
            }
            description={
              <div style={{ marginTop: 8 }}>
                <Paragraph style={{ marginBottom: 8, fontSize: 14 }}>
                  Los datos de los archivos están basados en la estructura definida en el software de{' '}
                  <Text strong style={{ 
                    color: '#046b17ff', 
                    fontSize: 15,
                    backgroundColor: '#d4edda',
                    padding: '2px 6px',
                    borderRadius: 4
                  }}>
                    SIGIRES PROCEX
                  </Text>
                </Paragraph>
                <Paragraph style={{ marginBottom: 0, fontSize: 13 }}>
                  <WarningOutlined style={{ color: '#faad14', marginRight: 6 }} />
                  Por favor, <Text strong underline>asegúrate de seguir esta estructura</Text> al 
                  cargar archivos personalizados para evitar errores de procesamiento.
                </Paragraph>
              </div>
            }
            type="warning"
            showIcon
            icon={<WarningOutlined style={{ fontSize: 22 }} />}
            style={{
              marginLeft: 24,
              marginRight: 0,
              border: '3px solid #faad14',
              boxShadow: '0 4px 12px rgba(250, 173, 20, 0.3)',
              backgroundColor: '#fffbe6'
            }}
          />
        </div>

        <Divider style={{ margin: '8px 0' }} />

        {/* Paso 4 - Aplicar Filtros */}
        <div>
          <Title level={5}>
            <FilterOutlined style={{ color: '#722ed1', marginRight: 8 }} />
            4. Aplicar Filtros (Opcional)
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

        {/* Paso 5 - Descarga de Reportes */}
        <div>
          <Title level={5}>
            <DownloadOutlined style={{ color: '#eb2f96', marginRight: 8 }} />
            5. Descargar Reportes Generales
          </Title>
          <Paragraph style={{ marginLeft: 24, marginBottom: 8 }}>
            Una vez que los datos estén cargados y procesados, puedes descargar 
            los reportes en diferentes formatos:
          </Paragraph>
          
          <div style={{ marginLeft: 24, marginBottom: 12 }}>
            <Space direction="vertical" size="small" style={{ width: '100%' }}>
              <div>
                <FileExcelOutlined style={{ color: '#52c41a', marginRight: 8, fontSize: 16 }} />
                <Text strong>Formato CSV:</Text>
                <Paragraph type="secondary" style={{ marginLeft: 28, marginTop: 4, marginBottom: 4, fontSize: 12 }}>
                  Ideal para análisis de datos en Excel, hojas de cálculo o 
                  importación a otros sistemas. Contiene todos los registros 
                  procesados con las columnas completas.
                </Paragraph>
              </div>
              
              <div style={{ marginTop: 8 }}>
                <FilePdfOutlined style={{ color: '#ff4d4f', marginRight: 8, fontSize: 16 }} />
                <Text strong>Formato PDF:</Text>
                <Paragraph type="secondary" style={{ marginLeft: 28, marginTop: 4, marginBottom: 0, fontSize: 12 }}>
                  Perfecto para presentación de informes, impresión o archivo. 
                  Incluye tablas resumen, gráficos estadísticos y análisis 
                  descriptivo de los indicadores.
                </Paragraph>
              </div>
            </Space>
          </div>

          <Paragraph type="secondary" style={{ marginLeft: 24, fontSize: 12, fontStyle: 'italic' }}>
            Los botones de descarga aparecerán habilitados una vez que se 
            hayan procesado correctamente los datos del archivo seleccionado.
          </Paragraph>
        </div>

        <Divider style={{ margin: '8px 0' }} />

        {/* Paso 6 - Reporte de Inasistentes */}
        <div>
          <Title level={5}>
            <UserDeleteOutlined style={{ color: '#f5222d', marginRight: 8 }} />
            6. Generar Reporte de Inasistentes
          </Title>
          <Paragraph style={{ marginLeft: 24, marginBottom: 8 }}>
            Esta funcionalidad te permite identificar a los beneficiarios que 
            <Text strong> no asistieron</Text> a los controles de crecimiento y desarrollo 
            según los parámetros definidos.
          </Paragraph>
          
          <div style={{ marginLeft: 24, marginBottom: 8 }}>
            <Text strong>Pasos para generar el reporte:</Text>
            <ol style={{ marginLeft: 20, marginTop: 8 }}>
              <li style={{ marginBottom: 6 }}>
                <Text strong>Selecciona el rango de edad:</Text>
                <Paragraph type="secondary" style={{ marginLeft: 0, marginTop: 2, marginBottom: 4, fontSize: 12 }}>
                  Elige entre <Tag color="blue">Meses</Tag> o <Tag color="purple">Años</Tag> 
                  según el rango que necesites analizar.
                </Paragraph>
              </li>
              <li style={{ marginBottom: 0 }}>
                <Text strong>Genera el reporte:</Text>
                <Paragraph type="secondary" style={{ marginLeft: 0, marginTop: 2, marginBottom: 0, fontSize: 12 }}>
                  Haz clic en el botón "Generar Reporte" y el sistema procesará 
                  los datos para identificar a todos los inasistentes dentro del 
                  rango especificado.
                </Paragraph>
              </li>
            </ol>
          </div>

          <div style={{ 
            background: '#fff1f0', 
            padding: 10, 
            borderRadius: 6,
            border: '1px solid #ffccc7',
            marginLeft: 24
          }}>
            <Text strong style={{ color: '#cf1322' }}>
              📊 Datos incluidos en el reporte:
            </Text>
            <Paragraph style={{ marginBottom: 0, marginTop: 4, fontSize: 12 }}>
              Tipo de documento, número de documento, nombres completos, fecha de nacimiento, 
              edad actual, sexo, última fecha de asistencia, días sin asistir, 
              departamento, municipio y centro zonal.
            </Paragraph>
          </div>
        </div>

        <Divider style={{ margin: '8px 0' }} />

        {/* Paso 7 - Exportar Reporte de Inasistentes */}
        <div>
          <Title level={5}>
            <ExportOutlined style={{ color: '#13c2c2', marginRight: 8 }} />
            7. Exportar Reporte de Inasistentes
          </Title>
          <Paragraph style={{ marginLeft: 24, marginBottom: 8 }}>
            Una vez generado el reporte de inasistentes, podrás exportarlo 
            para su análisis externo o archivo.
          </Paragraph>
          
          <div style={{ marginLeft: 24, marginBottom: 8 }}>
            <Space direction="vertical" size="middle" style={{ width: '100%' }}>
              <div>
                <FileExcelOutlined style={{ color: '#52c41a', marginRight: 8, fontSize: 16 }} />
                <Text strong>Exportación en formato CSV:</Text>
                <Paragraph type="secondary" style={{ marginLeft: 28, marginTop: 4, marginBottom: 4, fontSize: 12 }}>
                  El archivo CSV contendrá todos los registros de inasistentes 
                  encontrados con sus datos completos, organizados en columnas 
                  para fácil manipulación en Excel o sistemas externos.
                </Paragraph>
              </div>

              <div>
                <Text strong style={{ color: '#1890ff' }}>Características del archivo exportado:</Text>
                <ul style={{ marginLeft: 20, marginTop: 4, fontSize: 12 }}>
                  <li>Codificación UTF-8 para caracteres especiales</li>
                  <li style={{ marginTop: 4 }}>Separador de columnas: punto y coma (;)</li>
                  <li style={{ marginTop: 4 }}>Primera fila con encabezados de columna</li>
                  <li style={{ marginTop: 4 }}>Fecha y hora de generación incluida en el nombre del archivo</li>
                </ul>
              </div>
            </Space>
          </div>

          <div style={{ 
            background: '#e6f7ff', 
            padding: 10, 
            borderRadius: 6,
            border: '1px solid #91d5ff',
            marginLeft: 24
          }}>
            <Text strong style={{ color: '#096dd9' }}>
              ✅ Usos recomendados:
            </Text>
            <Paragraph style={{ marginBottom: 0, marginTop: 4, fontSize: 12 }}>
              • Análisis estadístico detallado • Identificación de patrones de inasistencia 
              • Generación de reportes personalizados • Planificación de estrategias de 
              seguimiento • Importación a sistemas de gestión externa
            </Paragraph>
          </div>
        </div>

        <Divider style={{ margin: '8px 0' }} />

        {/* Nota adicional final */}
        <div style={{ 
          background: '#f0f5ff', 
          padding: 12, 
          borderRadius: 6,
          border: '1px solid #adc6ff'
        }}>
          <Text strong style={{ color: '#1890ff' }}>
            💡 Consejo Final:
          </Text>
          <Paragraph style={{ marginBottom: 0, marginTop: 4, fontSize: 12 }}>
            Usa el botón "Actualizar" para recargar la lista de archivos 
            disponibles después de realizar cambios en el sistema. Recuerda 
            que todos los reportes se generan en tiempo real basados en 
            los datos actuales cargados.
          </Paragraph>
        </div>

      </Space>
    </Modal>
  );
};
