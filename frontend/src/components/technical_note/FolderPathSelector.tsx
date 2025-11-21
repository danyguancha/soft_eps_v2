// components/technical-note/FolderPathSelector.tsx
import React, { useState, useEffect } from 'react';
import { Card, Alert, Space, Typography, Button, Tooltip, Input } from 'antd';
import { 
  FolderOpenOutlined, 
  CheckCircleOutlined, 
  InfoCircleOutlined, 
  DeleteOutlined,
  PlayCircleOutlined,
  LoadingOutlined
} from '@ant-design/icons';

const { Text } = Typography;

interface FolderPathSelectorProps {
  selectedPath: string;
  onPathChange: (path: string) => void;
  onProcess?: () => void;
  disabled?: boolean;
  processing?: boolean;
}

export const FolderPathSelector: React.FC<FolderPathSelectorProps> = ({
  selectedPath,
  onPathChange,
  onProcess,
  disabled = false,
  processing = false
}) => {
  const [isValidPath, setIsValidPath] = useState<boolean>(false);
  const [validationMessage, setValidationMessage] = useState<string>('');

  useEffect(() => {
    if (!selectedPath.trim()) {
      setIsValidPath(false);
      setValidationMessage('');
      return;
    }

    const validation = validateFolderPath(selectedPath);
    setIsValidPath(validation.isValid);
    setValidationMessage(validation.message);
  }, [selectedPath]);

  const validateFolderPath = (path: string): { isValid: boolean; message: string } => {
    if (!path.trim()) {
      return { isValid: false, message: 'La ruta no puede estar vacía' };
    }

    const windowsPathRegex = /^[a-zA-Z]:(\\|\/)[^<>:"|?*]+$/;
    const unixPathRegex = /^\/[^<>:"|?*]+$/;
    const uncPathRegex = /^\\\\[^\\]+\\[^\\]+/;

    if (windowsPathRegex.test(path)) {
      return { isValid: true, message: 'Ruta de Windows válida' };
    }

    if (unixPathRegex.test(path)) {
      return { isValid: true, message: 'Ruta de Unix/Linux válida' };
    }

    if (uncPathRegex.test(path)) {
      return { isValid: true, message: 'Ruta UNC válida' };
    }

    return { 
      isValid: false, 
      message: 'Formato de ruta inválido. Use formato Windows (C:\\carpeta) o Unix (/carpeta)' 
    };
  };

  const handleClear = () => {
    onPathChange('');
  };

  const handlePathChange = (value: string) => {
    let normalizedPath = value.trim();
    
    if (/^[a-zA-Z]:\//.test(normalizedPath)) {
      normalizedPath = normalizedPath.replace(/\//g, '\\');
    }
    
    onPathChange(normalizedPath);
  };

  const handleProcess = () => {
    if (onProcess && isValidPath && !processing) {
      onProcess();
    }
  };

  const canProcess = isValidPath && !processing;

  return (
    <Card
      size="small"
      style={{ 
        marginBottom: 16, 
        border: isValidPath 
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
              color: isValidPath ? '#52c41a' : selectedPath.trim() ? '#ff4d4f' : '#1890ff'
            }} 
          />
          <Text strong style={{ fontSize: 14 }}>
            Paso 1: Procesar Archivos NT RPMS
          </Text>
          <Tooltip title="Ingrese la ruta completa de la carpeta con archivos NT RPMS">
            <InfoCircleOutlined style={{ color: '#1890ff', fontSize: 14 }} />
          </Tooltip>
        </Space>

        {/* Input y botones en layout compacto */}
        <Space.Compact style={{ width: '100%' }}>
          <Input
            value={selectedPath}
            onChange={(e) => handlePathChange(e.target.value)}
            placeholder="C:\Users\USUARIO\archivos_a_evaluar"
            disabled={disabled || processing}
            prefix={<FolderOpenOutlined style={{ color: '#8c8c8c' }} />}
            suffix={
              selectedPath.trim() ? (
                isValidPath ? (
                  <CheckCircleOutlined style={{ color: '#52c41a' }} />
                ) : (
                  <InfoCircleOutlined style={{ color: '#ff4d4f' }} />
                )
              ) : null
            }
            status={selectedPath.trim() && !isValidPath ? 'error' : undefined}
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

        {/* Botón de procesar - solo visible cuando la ruta es válida */}
        {isValidPath && (
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
              : 'Procesar archivos NT RPMS'
            }
          </Button>
        )}

        {/* Alert compacto solo cuando está procesando */}
        {processing && (
          <Alert
            message="Extracción de información en proceso..."
            description="Por favor espere mientras se procesan los archivos."
            type="info"
            showIcon
            style={{ padding: '8px 12px', fontSize: 12 }}
          />
        )}
      </Space>
    </Card>
  );
};
