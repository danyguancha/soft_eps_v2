// src/components/tabs/TabRenderer.tsx
import React from 'react';
import { Alert, Button } from 'antd';
import type { TabRendererProps } from '../../types/api.types';
import { getTabConfig } from './TabRegistry';

// ✅ Renderizador extensible de tabs (Open/Closed Principle)
export const TabRenderer: React.FC<TabRendererProps> = ({ 
  activeTab, 
  fileData,
  onTabChange,
  ...otherProps 
}) => {
  const tabConfig = getTabConfig(activeTab);

  if (!tabConfig) {
    return (
      <Alert
        message="Tab no encontrado"
        description={`El tab "${activeTab}" no está registrado.`}
        type="error"
        showIcon
      />
    );
  }

  // Verificar si el tab requiere archivo y no hay uno seleccionado
  if (tabConfig.requiresFile && !fileData) {
    return (
      <Alert
        message="Archivo requerido"
        description={`Esta funcionalidad requiere que selecciones un archivo primero.`}
        type="info"
        showIcon
        action={
          <Button
            size="small"
            type="primary"
            onClick={() => onTabChange('upload')}
          >
            Ir a Cargar
          </Button>
        }
      />
    );
  }

  const TabComponent = tabConfig.component;

  return (
    <TabComponent
      fileData={fileData}
      onTabChange={onTabChange}
      {...otherProps}
    />
  );
};
