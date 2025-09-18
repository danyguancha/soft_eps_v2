// src/components/tabs/tabs/ChatTab/ChatTab.tsx
import React from 'react';

import type { TabProps } from '../../../types/api.types';
import { ChatBot } from '../../chatbot/ChatBot';

export const ChatTab: React.FC<TabProps> = ({ fileData }) => {
  return (
    <div className="content-container">
      <ChatBot fileContext={fileData?.file_id} />
    </div>
  );
};
