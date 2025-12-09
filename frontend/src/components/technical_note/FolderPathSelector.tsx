// components/technical-note/FolderPathSelector.tsx - CORREGIDO
import React, { useState, useEffect } from 'react';
import { Card, Alert, Space, Typography, Button, Tooltip, Input, Tag, Radio } from 'antd';
import { 
  FolderOpenOutlined, 
  CheckCircleOutlined, 
  InfoCircleOutlined, 
  DeleteOutlined,
  PlayCircleOutlined,
  LoadingOutlined,
  CloudServerOutlined,
  DesktopOutlined
} from '@ant-design/icons';

const { Text } = Typography;

interface FolderPathSelectorProps {
  selectedPath: string;
  onPathChange: (path: string) => void;
  onProcess?: (mode: 'network' | 'local') => void;
  disabled?: boolean;
  processing?: boolean;
}

type PathType = 'network' | 'local' | 'invalid' | 'empty';

interface PathValidation {
  isValid: boolean;
  pathType: PathType;
  message: string;
  suggestion?: string;
}

export const FolderPathSelector: React.FC<FolderPathSelectorProps> = ({
  selectedPath,
  onPathChange,
  onProcess,
  disabled = false,
  processing = false
}) => {
  const [validation, setValidation] = useState<PathValidation>({
    isValid: false,
    pathType: 'empty',
    message: ''
  });
  const [processMode, setProcessMode] = useState<'auto' | 'network' | 'local'>('auto');

  useEffect(() => {
    if (!selectedPath.trim()) {
      setValidation({
        isValid: false,
        pathType: 'empty',
        message: ''
      });
      return;
    }

    const result = validateFolderPath(selectedPath);
    setValidation(result);
  }, [selectedPath]);

  const validateFolderPath = (path: string): PathValidation => {
    if (!path.trim()) {
      return { 
        isValid: false, 
        pathType: 'empty',
        message: 'La ruta no puede estar vacía' 
      };
    }

    // Validar ruta UNC (red compartida)
    const uncPathRegex = /^\\\\[^\\]+\\[^\\]+/;
    if (uncPathRegex.test(path)) {
      return { 
        isValid: true, 
        pathType: 'network',
        message: 'Ruta de red compartida (UNC)',
        suggestion: 'Asegúrate de que la carpeta esté compartida y tenga permisos de lectura'
      };
    }

    // Validar ruta de Windows con drive mapeado (CORREGIDO: Y-Z en lugar de Z-Y)
    const mappedDriveRegex = /^[Y-Z]:(\\|\/)/i;
    if (mappedDriveRegex.test(path)) {
      return { 
        isValid: true, 
        pathType: 'network',
        message: 'Drive mapeado de red',
        suggestion: 'El drive debe estar mapeado correctamente en el servidor'
      };
    }

    // Validar ruta de Windows local
    const windowsPathRegex = /^[a-zA-Z]:(\\|\/)[^<>:"|?*]+$/;
    if (windowsPathRegex.test(path)) {
      return { 
        isValid: true, 
        pathType: 'local',
        message: 'Ruta local de Windows',
        suggestion: 'Esta carpeta debe existir en el servidor'
      };
    }

    // Validar ruta Unix/Linux
    const unixPathRegex = /^\/[^<>:"|?*]+$/;
    if (unixPathRegex.test(path)) {
      return { 
        isValid: true, 
        pathType: 'local',
        message: 'Ruta local de Unix/Linux',
        suggestion: 'Esta carpeta debe existir en el servidor'
      };
    }

    return { 
      isValid: false, 
      pathType: 'invalid',
      message: 'Formato de ruta inválido',
      suggestion: 'Use formato UNC (\\\\IP\\carpeta) para red o ruta absoluta para local'
    };
  };

  const handleClear = () => {
    onPathChange('');
  };

  const handlePathChange = (value: string) => {
    let normalizedPath = value.trim();
    
    // Normalizar barras en rutas Windows
    if (/^[a-zA-Z]:\//.test(normalizedPath)) {
      normalizedPath = normalizedPath.replace(/\//g, '\\');
    }
    
    // Normalizar rutas UNC con barras incorrectas
    if (normalizedPath.startsWith('//')) {
      normalizedPath = normalizedPath.replace(/^\/\//, '\\\\');
    }
    
    onPathChange(normalizedPath);
  };

  const handleProcess = () => {
    if (onProcess && validation.isValid && !processing) {
      // Determinar el modo de procesamiento
      let mode: 'network' | 'local';
      
      if (processMode === 'auto') {
        mode = validation.pathType === 'network' ? 'network' : 'local';
      } else {
        mode = processMode;
      }
      
      onProcess(mode);
    }
  };

  const canProcess = validation.isValid && !processing;

  const getPathTypeTag = () => {
    if (!validation.isValid || validation.pathType === 'empty') return null;

    if (validation.pathType === 'network') {
      return (
        <Tag 
          icon={<CloudServerOutlined />} 
          color="blue"
          style={{ fontSize: 11, padding: '0 8px' }}
        >
          Red Compartida
        </Tag>
      );
    }

    if (validation.pathType === 'local') {
      return (
        <Tag 
          icon={<DesktopOutlined />} 
          color="green"
          style={{ fontSize: 11, padding: '0 8px' }}
        >
          Local Servidor
        </Tag>
      );
    }

    return null;
  };

  const getExamplePaths = () => {
    return (
      <Space direction="vertical" size={4} style={{ fontSize: 11, color: '#8c8c8c' }}>
        <Text type="secondary" style={{ fontSize: 11 }}>
          <strong>Red:</strong> \\192.168.1.100\NT_RPMS_Share
        </Text>
        <Text type="secondary" style={{ fontSize: 11 }}>
          <strong>Local:</strong> C:\Users\Usuario\archivos_nt
        </Text>
      </Space>
    );
  };

  return (
    <Card
      size="small"
      style={{ 
        marginBottom: 16, 
        border: validation.isValid 
          ? '1px solid #52c41a' 
          : selectedPath.trim()
            ? '1px solid #ff4d4f'
            : '1px solid #d9d9d9',
      }}
    >
      <Space direction="vertical" style={{ width: '100%' }} size="small">
        {/* Header compacto */}
        <Space align="center" style={{ marginBottom: 4 }}>
          <FolderOpenOutlined 
            style={{ 
              fontSize: 18, 
              color: validation.isValid ? '#52c41a' : selectedPath.trim() ? '#ff4d4f' : '#1890ff'
            }} 
          />
          <Text strong style={{ fontSize: 14 }}>
            Paso 1: Procesar Archivos NT RPMS
          </Text>
          {getPathTypeTag()}
          <Tooltip 
            title={getExamplePaths()}
            placement="right"
          >
            <InfoCircleOutlined style={{ color: '#1890ff', fontSize: 14 }} />
          </Tooltip>
        </Space>

        {/* Input y botones en layout compacto */}
        <Space.Compact style={{ width: '100%' }}>
          <Input
            value={selectedPath}
            onChange={(e) => handlePathChange(e.target.value)}
            placeholder="\\192.168.1.100\NT_RPMS_Share  o  C:\archivos_nt"
            disabled={disabled || processing}
            prefix={
              validation.pathType === 'network' 
                ? <CloudServerOutlined style={{ color: '#1890ff' }} />
                : <FolderOpenOutlined style={{ color: '#8c8c8c' }} />
            }
            suffix={
              selectedPath.trim() ? (
                validation.isValid ? (
                  <CheckCircleOutlined style={{ color: '#52c41a' }} />
                ) : (
                  <InfoCircleOutlined style={{ color: '#ff4d4f' }} />
                )
              ) : null
            }
            status={selectedPath.trim() && !validation.isValid ? 'error' : undefined}
            style={{ 
              fontFamily: 'Consolas, Monaco, monospace',
              fontSize: 12
            }}
          />
          {selectedPath.trim() && !processing && (
            <Button 
              danger
              icon={<DeleteOutlined />}
              onClick={handleClear}
              disabled={disabled}
            />
          )}
        </Space.Compact>

        {/* Mensaje de validación */}
        {selectedPath.trim() && (
          <Text 
            type={validation.isValid ? 'success' : 'danger'} 
            style={{ fontSize: 11, display: 'block', marginTop: 4 }}
          >
            {validation.message}
            {validation.suggestion && (
              <>
                <br />
                <Text type="secondary" style={{ fontSize: 11 }}>
                  💡 {validation.suggestion}
                </Text>
              </>
            )}
          </Text>
        )}

        {/* Selector de modo (solo si ruta es válida) */}
        {validation.isValid && !processing && (
          <Space direction="vertical" size={4} style={{ width: '100%', marginTop: 8 }}>
            <Text type="secondary" style={{ fontSize: 11 }}>
              Modo de procesamiento:
            </Text>
            <Radio.Group 
              value={processMode} 
              onChange={(e) => setProcessMode(e.target.value)}
              size="small"
              buttonStyle="solid"
            >
              <Radio.Button value="auto">
                🤖 Automático
              </Radio.Button>
              <Radio.Button value="network">
                <CloudServerOutlined /> Red
              </Radio.Button>
              <Radio.Button value="local">
                <DesktopOutlined /> Local
              </Radio.Button>
            </Radio.Group>
          </Space>
        )}

        {/* Botón de procesar - solo visible cuando la ruta es válida */}
        {validation.isValid && (
          <Button
            type="primary"
            size="middle"
            block
            icon={processing ? <LoadingOutlined /> : <PlayCircleOutlined />}
            onClick={handleProcess}
            disabled={!canProcess}
            loading={processing}
            style={{ 
              backgroundColor: canProcess ? '#52c41a' : undefined,
              borderColor: canProcess ? '#52c41a' : undefined,
              marginTop: 8
            }}
          >
            {processing 
              ? 'Procesando archivos NT RPMS...' 
              : `Procesar desde ${
                  processMode === 'auto' 
                    ? (validation.pathType === 'network' ? 'red' : 'servidor')
                    : (processMode === 'network' ? 'red' : 'servidor')
                }`
            }
          </Button>
        )}

        {/* Alert compacto solo cuando está procesando */}
        {processing && (
          <Alert
            message="Extracción de información en proceso..."
            description={
              <Space direction="vertical" size={2}>
                <Text style={{ fontSize: 11 }}>
                  {validation.pathType === 'network' 
                    ? 'Accediendo a carpeta compartida en red...'
                    : 'Leyendo archivos del servidor...'
                  }
                </Text>
                <Text type="secondary" style={{ fontSize: 11 }}>
                  Por favor espere, esto puede tomar varios minutos.
                </Text>
              </Space>
            }
            type="info"
            showIcon
            style={{ padding: '8px 12px', fontSize: 12 }}
          />
        )}

        {/* Información adicional para rutas de red */}
        {validation.pathType === 'network' && !processing && (
          <Alert
            message="📡 Carpeta Compartida en Red"
            description={
              <Space direction="vertical" size={2}>
                <Text style={{ fontSize: 11 }}>
                  • La carpeta debe estar compartida en el equipo cliente
                </Text>
                <Text style={{ fontSize: 11 }}>
                  • El servidor debe tener permisos de lectura
                </Text>
                <Text style={{ fontSize: 11 }}>
                  • Ambos equipos deben estar en la misma red
                </Text>
              </Space>
            }
            type="info"
            showIcon
            style={{ padding: '8px 12px', fontSize: 11, marginTop: 8 }}
          />
        )}
      </Space>
    </Card>
  );
};
