// src/components/tabs/tabs/ChatTab/ChatTab.tsx
import React from 'react';
import { Alert, Button } from 'antd';
import type { TabProps } from '../../../types/api.types';
import { ChatBot } from '../../chatbot/ChatBot';

export const ChatTab: React.FC<TabProps> = ({ fileData, onTabChange }) => {
  return (
    <div className="content-container">
      {!fileData && (
        <Alert
          message="Recomendación"
          description="Para obtener respuestas más precisas, selecciona un archivo en la sección 'Cargar' para dar contexto al chat."
          type="info"
          showIcon
          style={{ marginBottom: 16 }}
          action={
            <Button
              size="small"
              type="primary"
              onClick={() => onTabChange('upload')}
            >
              Seleccionar Archivo
            </Button>
          }
        />
      )}
      <ChatBot fileContext={fileData?.file_id} />
    </div>
  );
};
