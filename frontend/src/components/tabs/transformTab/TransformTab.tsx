// src/components/tabs/tabs/TransformTab/TransformTab.tsx
import React from 'react';
import { Card, Alert, Space, Button, Typography } from 'antd';
import { SwapOutlined } from '@ant-design/icons';
import type { TabProps } from '../../../types/api.types';
import { TransformPanel } from '../../transform/TransformPanel';
import { useFileOperations } from '../../../hooks/useFileOperations';

const { Text } = Typography;

export const TransformTab: React.FC<TabProps> = ({ isMobile, onOpenCrossModal }) => {
  const { files, handleFileUploadedFromTransform, loadFiles } = useFileOperations();

  return (
    <div className="content-container">
      <TransformPanel
        isMobile={isMobile}
        onSelectOp={() => {}}
        availableFiles={files || []}
        onRefreshFiles={loadFiles}
        onFileUploaded={handleFileUploadedFromTransform}
      />

      <Card title="🔄 Cruzar Archivos" style={{ marginTop: 16 }}>
        <Alert
          message="Cruce de Archivos"
          description="Combina datos de dos archivos basándose en columnas clave comunes. Perfecto para enriquecer tus datos principales con información adicional."
          type="info"
          showIcon
          style={{ marginBottom: 16 }}
        />

        <Space>
          <Button
            type="primary"
            icon={<SwapOutlined />}
            size="large"
            onClick={onOpenCrossModal}
            disabled={!files || files.length < 2}
          >
            {isMobile ? 'Cruzar' : 'Iniciar Cruce de Archivos'}
          </Button>

          {(!files || files.length < 2) && (
            <Text type="secondary">
              (Necesitas al menos 2 archivos cargados)
            </Text>
          )}
        </Space>
      </Card>
    </div>
  );
};
